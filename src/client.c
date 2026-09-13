/* client - sends one file reliably over UDP.
 *
 *   ./client <host> <port> <file> [dgram_bytes] [mbit]
 *   ./client 192.168.20.100 9000 data.bin 1472 90
 *
 * dgram_bytes is the entire UDP payload, including the 16-byte FT header:
 *   MTU 1500 -> 1472
 *   MTU 9000 -> 8972
 *   MTU 9001 -> 8973
 *
 * The sender streams the file once at a paced rate. Every report interval it
 * asks the receiver what is missing (FIN) and the receiver answers with the
 * gaps below the highest sequence it has seen. Those chunks are retransmitted
 * inside the same paced stream, so lost chunks are repaired while the first
 * pass is still running and the link never idles. Once every chunk has been
 * sent, FIN carries FT_FLAG_ALL_SENT and the loop continues until DONE.
 *
 * All timeouts derive from the RTT measured on the META/READY handshake and
 * refined by every FIN/NACK_END exchange. A per-sequence "last sent" clock
 * suppresses retransmission requests that were generated before the previous
 * copy could have arrived.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <math.h>
#include <netdb.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "proto.h"

struct pacer {
    uint64_t interval_ns;
    uint64_t next_ns;
};

#define FT_ROUND_RING 64

/* Everything the send loop needs, gathered so helpers stay short. */
struct sender {
    int sock, fd;
    uint32_t payload;
    int dgram;
    uint64_t file_size, total_pkts;
    char *datagram;

    uint8_t *pending;               /* bitmap: requested, not yet resent */
    uint64_t pending_count;
    uint64_t pending_cursor;        /* byte index where the next scan starts */
    uint32_t *last_sent_ms;         /* ms since start + 1; 0 = never sent */
    uint64_t start_mono_ns;

    uint64_t rtt_min_ns, srtt_ns;
    uint64_t next_new;              /* next never-sent sequence */
    uint64_t round;                 /* feedback round of the latest FIN */
    uint64_t round_sent_ns;         /* when that FIN went out */
    uint32_t round_sent_ms[FT_ROUND_RING]; /* FIN send time per recent round */
    uint8_t round_answered[FT_ROUND_RING / 8];
    int outstanding;                /* waiting for NACK_END/DONE of `round` */
    int trace;

    /* Rate probing (mbit = auto). The receiver reports how many unique
     * chunks it holds in every NACK_END; the sender knows how many datagrams
     * it had sent when the matching FIN went out (FIFO path). The delivery
     * ratio of a window is therefore measured exactly. Below the link
     * capacity that ratio is 1 - p whatever the rate; above it, it falls
     * in proportion. So: raise the rate while the ratio holds, step back
     * when it drops. Injected loss cannot fool this, congestion cannot hide. */
    int probe;
    double rate_bps, rate_max_bps, probe_step;
    uint64_t sent_at_fin[FT_ROUND_RING];
    uint64_t window_round, window_sent, window_recv, window_start_ns;
    double ratio_ref;
    int probe_hold, probe_windows, probe_skip, probe_backoffs;
    struct pacer *pacer;

    double loss_estimate;           /* fraction of chunks lost per pass, from reports */
    int copies;                     /* copies per retransmitted chunk this round */
    uint64_t redundant;             /* extra copies sent by tail redundancy */

    uint64_t retransmitted, reports_sent, nack_filtered, nack_accepted;
    uint64_t feedback_datagrams, icmp_errors;
    uint64_t receiver_end_ns, sender_start_ns;
    int complete;
};

static void die(const char *message)
{
    perror(message);
    exit(EXIT_FAILURE);
}

static uint64_t realtime_ns(void)
{
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) < 0) die("clock_gettime realtime");
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static uint64_t mono_ns(void)
{
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) die("clock_gettime monotonic");
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static void sleep_ms(long milliseconds)
{
    struct timespec delay = {
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000000L
    };
    while (nanosleep(&delay, &delay) < 0 && errno == EINTR) {}
}

/* Absolute-deadline pacing. If the loop falls more than a few intervals
 * behind (a long feedback drain, scheduler hiccup) the schedule is reset
 * instead of catching up with a burst that the token bucket would drop. */
static void pacer_reset(struct pacer *pacer)
{
    pacer->next_ns = mono_ns();
}

static void pacer_wait(struct pacer *pacer)
{
    if (!pacer->interval_ns) return;
    uint64_t now = mono_ns();
    pacer->next_ns += pacer->interval_ns;
    if (pacer->next_ns + 4 * pacer->interval_ns < now) pacer->next_ns = now;
    if (pacer->next_ns <= now) return;

    struct timespec deadline = {
        .tv_sec = (time_t)(pacer->next_ns / 1000000000ull),
        .tv_nsec = (long)(pacer->next_ns % 1000000000ull)
    };
    while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &deadline, NULL) == EINTR) {}
}

/* send() on a connected UDP socket reports ICMP port-unreachable as
 * ECONNREFUSED. That happens legitimately when the receiver has already
 * finished and closed, so it is counted, not fatal. */
static void send_retry(struct sender *s, const void *buffer, size_t length)
{
    for (;;) {
        ssize_t sent = send(s->sock, buffer, length, 0);
        if (sent == (ssize_t)length) return;
        if (sent < 0 && (errno == EINTR || errno == EAGAIN || errno == ENOBUFS)) {
            sleep_ms(1);
            continue;
        }
        if (sent < 0 && errno == ECONNREFUSED) {
            s->icmp_errors++;
            continue;
        }
        if (sent < 0) die("send");
        fprintf(stderr, "short UDP send: %zd/%zu\n", sent, length);
        exit(EXIT_FAILURE);
    }
}

static void read_chunk(int fd, void *buffer, size_t length, off_t offset)
{
    size_t done = 0;
    while (done < length) {
        ssize_t got = pread(fd, (char *)buffer + done, length - done, offset + (off_t)done);
        if (got > 0) {
            done += (size_t)got;
            continue;
        }
        if (got < 0 && errno == EINTR) continue;
        if (got < 0) die("pread");
        fprintf(stderr, "unexpected EOF while reading source file\n");
        exit(EXIT_FAILURE);
    }
}

static uint32_t elapsed_ms_plus_one(const struct sender *s)
{
    return (uint32_t)((mono_ns() - s->start_mono_ns) / 1000000ull) + 1;
}

static void send_data(struct sender *s, uint64_t sequence)
{
    uint64_t offset = sequence * (uint64_t)s->payload;
    size_t body = s->payload;
    if (s->file_size - offset < body) body = (size_t)(s->file_size - offset);

    read_chunk(s->fd, s->datagram + FT_HDRLEN, body, (off_t)offset);
    make_hdr((struct ft_hdr *)s->datagram, FT_DATA, sequence);
    if (s->sender_start_ns == 0) s->sender_start_ns = realtime_ns();
    send_retry(s, s->datagram, FT_HDRLEN + body);
    s->last_sent_ms[sequence] = elapsed_ms_plus_one(s);
}

static void send_fin(struct sender *s)
{
    uint8_t flags = s->next_new >= s->total_pkts ? FT_FLAG_ALL_SENT : 0;
    s->round++;
    make_hdr_flags((struct ft_hdr *)s->datagram, FT_FIN, flags, s->round);
    send_retry(s, s->datagram, FT_HDRLEN);
    /* After the first pass a lost FIN idles the link for a whole timeout,
     * so it is sent twice; the receiver answers a repeated round only when
     * the report is small. */
    if (flags) {
        sleep_ms(2);
        send_retry(s, s->datagram, FT_HDRLEN);
    }
    s->round_sent_ns = mono_ns();
    s->round_sent_ms[s->round % FT_ROUND_RING] = elapsed_ms_plus_one(s);
    s->sent_at_fin[s->round % FT_ROUND_RING] = s->next_new + s->retransmitted + s->redundant;
    BM_CLEAR(s->round_answered, s->round % FT_ROUND_RING);
    s->outstanding = 1;
    s->reports_sent++;
    if (s->trace)
        fprintf(stderr, "[%8.3f] FIN %" PRIu64 "%s pending=%" PRIu64 "\n",
                (s->round_sent_ns - s->start_mono_ns) / 1e9, s->round,
                flags ? " ALL_SENT" : "", s->pending_count);
}

static void rtt_sample(struct sender *s, uint64_t sample_ns)
{
    if (sample_ns < 100000) sample_ns = 100000;      /* 0.1 ms floor */
    if (!s->rtt_min_ns || sample_ns < s->rtt_min_ns) s->rtt_min_ns = sample_ns;
    if (!s->srtt_ns) s->srtt_ns = sample_ns;
    else s->srtt_ns = (s->srtt_ns * 7 + sample_ns) / 8;
}

/* Derived timers. */
static uint64_t report_interval_ns(const struct sender *s)
{
    uint64_t interval = s->rtt_min_ns;
    if (interval < (uint64_t)FT_MIN_REPORT_MS * 1000000ull) interval = (uint64_t)FT_MIN_REPORT_MS * 1000000ull;
    if (interval > (uint64_t)FT_MAX_REPORT_MS * 1000000ull) interval = (uint64_t)FT_MAX_REPORT_MS * 1000000ull;
    return interval;
}


/* Waiting for a complete report: two RTTs plus room for the receiver's paced
 * feedback burst. */
static uint64_t fin_timeout_ns(const struct sender *s)
{
    uint64_t timeout = 3 * s->srtt_ns / 2 + 100000000ull;
    if (timeout < 150000000ull) timeout = 150000000ull;
    if (timeout > 1500000000ull) timeout = 1500000000ull;
    return timeout;
}

/* A report answers one FIN. The receiver's state in that report reflects
 * every datagram that left the sender before that FIN, because the path is
 * FIFO. So a request for `seq` is stale exactly when the last copy of `seq`
 * was sent after the FIN it answers; it is then still in flight. No RTT
 * estimate is involved. */
static void request_retransmit(struct sender *s, uint64_t sequence, uint32_t fin_sent_ms)
{
    if (sequence >= s->total_pkts) return;
    uint32_t last = s->last_sent_ms[sequence];
    if (last == 0) return;                          /* never sent: cannot be lost yet */
    if (BM_TEST(s->pending, sequence)) return;
    if (last > fin_sent_ms) {
        s->nack_filtered++;
        return;
    }
    BM_MARK(s->pending, sequence);
    s->pending_count++;
    s->nack_accepted++;
}

static void set_rate(struct sender *s, double bps)
{
    if (bps > s->rate_max_bps) bps = s->rate_max_bps;
    if (bps < 1e6) bps = 1e6;
    s->rate_bps = bps;
    s->pacer->interval_ns = (uint64_t)((s->dgram + 28) * 8.0 / bps * 1e9);
    /* The queue in front of the shaper (netem holds up to 1000 datagrams)
     * takes a window to fill or drain after a rate change, so the next
     * window is measured but not judged. */
    s->probe_skip = 1;
}

/* A probe window closes once it spans 1.5 RTTs and enough datagrams for
 * the binomial noise on the delivery ratio (3 sigma) to be under a third
 * of the rate step: n >= 81 p(1-p) / step^2. That is ~500 datagrams on a
 * 1% link and ~5000 on a 20% link at a 5% step. The step starts at 25%
 * (slow-start style) and halves at every back-off down to 2%. */
static void probe_update(struct sender *s, uint64_t round, uint64_t received)
{
    uint64_t sent = s->sent_at_fin[round % FT_ROUND_RING];
    uint64_t fin_ns = (uint64_t)s->round_sent_ms[round % FT_ROUND_RING] * 1000000ull;
    if (s->window_round == 0) {
        s->window_round = round;
        s->window_sent = sent;
        s->window_recv = received;
        s->window_start_ns = fin_ns;
        return;
    }
    if (round <= s->window_round || sent <= s->window_sent) return;
    uint64_t dsent = sent - s->window_sent;
    double ratio = (double)(received - s->window_recv) / (double)dsent;
    double need = 81.0 * ratio * (1.0 - ratio) / (s->probe_step * s->probe_step);
    if (need < 500) need = 500;
    if ((double)dsent < need || fin_ns < s->window_start_ns + 3 * s->rtt_min_ns / 2) return;

    double noise = 3.0 * sqrt(ratio * (1.0 - ratio) / (double)dsent) + 0.005;
    s->probe_windows++;

    double old = s->rate_bps;
    const char *verdict;
    if (s->probe_skip) {
        s->probe_skip = 0;
        verdict = "settling";
    } else if (s->ratio_ref == 0) {
        s->ratio_ref = ratio;
        verdict = "reference";
    } else if (s->probe_hold > 0) {
        s->probe_hold--;
        s->ratio_ref = 0.75 * s->ratio_ref + 0.25 * ratio;
        verdict = "hold";
    } else if (ratio + noise < s->ratio_ref) {
        /* Delivery fell: the last step crossed the link rate. Step back,
         * hold a few windows, and probe more gently from now on. */
        set_rate(s, s->rate_bps / (1.0 + s->probe_step));
        s->probe_hold = 3;
        s->probe_step = s->probe_step > 0.04 ? s->probe_step / 2 : 0.02;
        verdict = "over capacity, step back";
        /* Three back-offs at the smallest step means the knee is found;
         * the link here does not change, so stop hunting above it. */
        if (s->probe_step <= 0.02 && ++s->probe_backoffs >= 3) {
            s->probe_hold = 1 << 30;
            verdict = "over capacity, step back and settle";
        }
    } else {
        /* The reference is the running mean of the ratios measured at
         * rates that were judged fine: on a link with injected loss that
         * is 1 - p, and averaging keeps a single lucky window from setting
         * a bar the next honest window cannot clear. */
        s->ratio_ref = 0.75 * s->ratio_ref + 0.25 * ratio;
        if (s->rate_bps < s->rate_max_bps) {
            set_rate(s, s->rate_bps * (1.0 + s->probe_step));
            verdict = "raise";
        } else {
            verdict = "at maximum";
        }
    }
    if (s->trace)
        fprintf(stderr, "[%8.3f] PROBE window %" PRIu64 " dgrams: delivered %.4f (ref %.4f +-%.4f) -> %s, %.1f -> %.1f Mbit/s\n",
                (mono_ns() - s->start_mono_ns) / 1e9, dsent, ratio, s->ratio_ref, noise,
                verdict, old / 1e6, s->rate_bps / 1e6);

    s->window_round = round;
    s->window_sent = sent;
    s->window_recv = received;
    s->window_start_ns = fin_ns;
}

/* Tail redundancy. Each repair round costs at least one RTT no matter how
 * few chunks it carries, and with loss p a round of M chunks leaves p*M
 * behind. Once the pending set is small enough to be sent well inside one
 * RTT, every chunk is sent k times, with k chosen so that p^k * M < 0.5:
 * the round then almost always finishes the transfer. The extra bytes are
 * negligible; the saved round trips are not. */
static int tail_copies(const struct sender *s)
{
    if (s->next_new < s->total_pkts || s->pending_count == 0) return 1;
    double p = s->loss_estimate;
    if (p < 0.01) p = 0.01;
    if (p > 0.5) p = 0.5;
    double send_time_ns = (double)s->pending_count * (double)s->pacer->interval_ns;
    if (s->pacer->interval_ns && send_time_ns > (double)s->rtt_min_ns) return 1;
    double k = log(0.5 / (double)s->pending_count) / log(p);
    int copies = (int)k + 1;
    if (copies < 1) copies = 1;
    if (copies > 4) copies = 4;
    return copies;
}

/* Consume every datagram queued on the socket. Returns after EAGAIN. */
static void drain_feedback(struct sender *s, char *buffer)
{
    for (int budget = 0; budget < 256; budget++) {
        ssize_t length = recv(s->sock, buffer, MAX_DGRAM, 0);
        if (length < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            if (errno == ECONNREFUSED) {
                s->icmp_errors++;
                continue;
            }
            die("recv");
        }
        if (length < FT_HDRLEN) continue;

        struct ft_hdr header;
        memcpy(&header, buffer, FT_HDRLEN);
        hdr_ntoh(&header);
        if (header.magic != FT_MAGIC) continue;
        s->feedback_datagrams++;

        if (header.type == FT_DONE && length >= FT_HDRLEN + (ssize_t)sizeof(struct ft_done)) {
            struct ft_done done;
            memcpy(&done, buffer + FT_HDRLEN, sizeof(done));
            done_ntoh(&done);
            s->receiver_end_ns = done.receiver_end_realtime_ns;
            s->complete = 1;
            return;
        }

        /* Feedback for a round we never sent, or one too old to track, is
         * ignored; anything still missing shows up in a newer report. */
        if (header.seq > s->round || s->round - header.seq >= FT_ROUND_RING) continue;
        uint32_t fin_sent_ms = s->round_sent_ms[header.seq % FT_ROUND_RING];

        if (header.type == FT_NACK) {
            ssize_t body = length - FT_HDRLEN;
            if (body % (ssize_t)sizeof(uint64_t) != 0) continue;
            size_t count = (size_t)body / sizeof(uint64_t);
            for (size_t index = 0; index < count; index++) {
                uint64_t wire;
                memcpy(&wire, buffer + FT_HDRLEN + index * sizeof(uint64_t), sizeof(wire));
                request_retransmit(s, ft_ntoh64(wire), fin_sent_ms);
            }
        } else if (header.type == FT_NACK_BITMAP) {
            ssize_t body = length - FT_HDRLEN - (ssize_t)sizeof(uint64_t);
            if (body <= 0) continue;
            uint64_t wire_base;
            memcpy(&wire_base, buffer + FT_HDRLEN, sizeof(wire_base));
            uint64_t base = ft_ntoh64(wire_base);
            if (base % 8 != 0 || base >= s->total_pkts) continue;
            const uint8_t *bits = (const uint8_t *)buffer + FT_HDRLEN + sizeof(uint64_t);
            for (ssize_t index = 0; index < body; index++) {
                uint8_t byte = bits[index];
                while (byte) {
                    int bit = __builtin_ctz(byte);
                    byte &= (uint8_t)(byte - 1);
                    request_retransmit(s, base + (uint64_t)index * 8 + (uint64_t)bit,
                                       fin_sent_ms);
                }
            }
        } else if (header.type == FT_NACK_END &&
                   length >= FT_HDRLEN + (ssize_t)sizeof(struct ft_nack_end)) {
            struct ft_nack_end end;
            memcpy(&end, buffer + FT_HDRLEN, sizeof(end));
            nack_end_ntoh(&end);
            /* Only the first NACK_END of a round is an RTT sample; the
             * second copy and late duplicates would bias it upwards. */
            if (!BM_TEST(s->round_answered, header.seq % FT_ROUND_RING)) {
                BM_MARK(s->round_answered, header.seq % FT_ROUND_RING);
                uint64_t sent_ms = s->round_sent_ms[header.seq % FT_ROUND_RING];
                rtt_sample(s, (elapsed_ms_plus_one(s) - sent_ms) * 1000000ull);
                if (header.seq == s->round) s->outstanding = 0;
                if (s->probe && s->next_new < s->total_pkts)
                    probe_update(s, header.seq, end.received_pkts);
                /* Every accepted repair request is one lost datagram, so
                 * the loss rate is simply requests over datagrams sent. */
                uint64_t sent = s->next_new + s->retransmitted + s->redundant;
                if (sent > 1000) s->loss_estimate = (double)s->nack_accepted / (double)sent;
                s->copies = tail_copies(s);
                if (s->trace)
                    fprintf(stderr, "[%8.3f] END %" PRIu64 " missing=%" PRIu64 " of %" PRIu64
                            " pending=%" PRIu64 " copies=%d loss=%.3f\n",
                            (mono_ns() - s->start_mono_ns) / 1e9, header.seq, end.missing,
                            end.reported_limit, s->pending_count, s->copies, s->loss_estimate);
            }
        }
    }
}

/* Pop the lowest pending sequence at or after the cursor, wrapping once. */
static int next_pending(struct sender *s, uint64_t *sequence)
{
    if (s->pending_count == 0) return 0;
    size_t bytes = (size_t)((s->total_pkts + 7) / 8);
    for (size_t step = 0; step < bytes; step++) {
        size_t index = (size_t)((s->pending_cursor + step) % bytes);
        if (!s->pending[index]) continue;
        int bit = __builtin_ctz(s->pending[index]);
        *sequence = (uint64_t)index * 8 + (uint64_t)bit;
        s->pending[index] &= (uint8_t)(s->pending[index] - 1);
        s->pending_count--;
        s->pending_cursor = index;
        return 1;
    }
    s->pending_count = 0;
    return 0;
}

static void wait_readable(int sock, uint64_t until_ns)
{
    uint64_t now = mono_ns();
    int timeout_ms = until_ns > now ? (int)((until_ns - now + 999999) / 1000000ull) : 0;
    struct pollfd pfd = { .fd = sock, .events = POLLIN };
    int ready;
    do {
        ready = poll(&pfd, 1, timeout_ms);
    } while (ready < 0 && errno == EINTR);
    if (ready < 0) die("poll");
}

static void wait_for_ready(struct sender *s, const struct ft_meta *metadata)
{
    char reply[MAX_DGRAM];
    for (int attempt = 1; attempt <= 50; attempt++) {
        struct ft_meta wire = *metadata;
        make_hdr((struct ft_hdr *)s->datagram, FT_META, 0);
        meta_hton(&wire);
        memcpy(s->datagram + FT_HDRLEN, &wire, sizeof(wire));
        uint64_t sent_ns = mono_ns();
        send_retry(s, s->datagram, FT_HDRLEN + sizeof(wire));

        uint64_t deadline = sent_ns + 500000000ull;
        for (;;) {
            wait_readable(s->sock, deadline);
            ssize_t length = recv(s->sock, reply, sizeof(reply), 0);
            if (length < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR || errno == ECONNREFUSED) {
                    if (mono_ns() >= deadline) break;
                    continue;
                }
                die("recv");
            }
            if (length >= FT_HDRLEN) {
                struct ft_hdr header;
                memcpy(&header, reply, FT_HDRLEN);
                hdr_ntoh(&header);
                if (header.magic == FT_MAGIC && header.type == FT_READY) {
                    rtt_sample(s, mono_ns() - sent_ns);
                    fprintf(stderr, "receiver READY after %d META attempt(s), RTT %.1f ms\n",
                            attempt, s->rtt_min_ns / 1e6);
                    return;
                }
            }
            if (mono_ns() >= deadline) break;
        }
    }
    fprintf(stderr, "receiver did not acknowledge META\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[])
{
    if (argc < 4 || argc > 6) {
        fprintf(stderr,
                "usage: %s <host> <port> <file> [dgram_bytes] [mbit]\n"
                "  dgram_bytes: 1472 for MTU 1500; 8972 for 9000; 8973 for 9001\n"
                "  mbit: paced wire rate, default 90; 0 means unpaced;\n"
                "        auto or auto:<max> probes the link rate (max default 100)\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    const char *host = argv[1];
    const char *port = argv[2];
    const char *path = argv[3];
    int dgram = argc >= 5 ? atoi(argv[4]) : 1472;
    double mbit = 90.0, mbit_max = 100.0;
    int probe = 0;
    if (argc >= 6) {
        if (strncmp(argv[5], "auto", 4) == 0) {
            probe = 1;
            if (argv[5][4] == ':') mbit_max = atof(argv[5] + 5);
            mbit = mbit_max / 2.0;
        } else {
            mbit = atof(argv[5]);
        }
    }

    if (dgram < FT_HDRLEN + (int)sizeof(struct ft_meta) || dgram > MAX_DGRAM) {
        fprintf(stderr, "dgram_bytes must be %d..%d\n",
                FT_HDRLEN + (int)sizeof(struct ft_meta), MAX_DGRAM);
        return EXIT_FAILURE;
    }
    if (mbit < 0 || (probe && mbit_max < 2)) {
        fprintf(stderr, "mbit must be non-negative\n");
        return EXIT_FAILURE;
    }

    struct sender s = {0};
    s.dgram = dgram;
    s.payload = (uint32_t)(dgram - FT_HDRLEN);

    struct addrinfo hints = {0}, *addresses = NULL;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    int rc = getaddrinfo(host, port, &hints, &addresses);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo(%s:%s): %s\n", host, port, gai_strerror(rc));
        return EXIT_FAILURE;
    }

    s.sock = socket(addresses->ai_family, addresses->ai_socktype, 0);
    if (s.sock < 0) die("socket");
    if (connect(s.sock, addresses->ai_addr, addresses->ai_addrlen) < 0) die("connect");
    int socket_buffer = 4 * 1024 * 1024;
    setsockopt(s.sock, SOL_SOCKET, SO_SNDBUF, &socket_buffer, sizeof(socket_buffer));
    setsockopt(s.sock, SOL_SOCKET, SO_RCVBUF, &socket_buffer, sizeof(socket_buffer));
    int fl = fcntl(s.sock, F_GETFL, 0);
    if (fl < 0 || fcntl(s.sock, F_SETFL, fl | O_NONBLOCK) < 0) die("fcntl O_NONBLOCK");

    s.fd = open(path, O_RDONLY);
    if (s.fd < 0) die("open source");
    struct stat status;
    if (fstat(s.fd, &status) < 0) die("fstat");
    if (status.st_size <= 0) {
        fprintf(stderr, "source file must not be empty\n");
        return EXIT_FAILURE;
    }

    s.file_size = (uint64_t)status.st_size;
    s.total_pkts = (s.file_size + s.payload - 1) / s.payload;
    uint32_t feedback_mbit = mbit > 0 ? (uint32_t)(mbit / 4.0) : 20;
    if (feedback_mbit < 5) feedback_mbit = 5;
    struct ft_meta metadata = {
        .file_size = s.file_size,
        .total_pkts = s.total_pkts,
        .payload_size = s.payload,
        .feedback_mbit = feedback_mbit
    };

    size_t bitmap_bytes = (size_t)((s.total_pkts + 7) / 8);
    s.datagram = malloc((size_t)dgram);
    char *feedback = malloc(MAX_DGRAM);
    s.pending = calloc(bitmap_bytes, 1);
    s.last_sent_ms = calloc((size_t)s.total_pkts, sizeof(uint32_t));
    if (!s.datagram || !feedback || !s.pending || !s.last_sent_ms)
        die("allocate transfer buffers");

    fprintf(stderr,
            "client: %s -> %s:%s, %" PRIu64 " B in %" PRIu64
            " packets (%u B data + %d B header = %d B UDP)",
            path, host, port, s.file_size, s.total_pkts, s.payload, FT_HDRLEN, dgram);
    if (probe) fprintf(stderr, ", probing the link rate from %.1f up to %.1f Mbit/s\n", mbit, mbit_max);
    else if (mbit > 0) fprintf(stderr, ", paced at %.1f Mbit/s\n", mbit);
    else fprintf(stderr, ", unpaced\n");

    wait_for_ready(&s, &metadata);

    struct pacer pacer = {0};
    if (mbit > 0) pacer.interval_ns = (uint64_t)((dgram + 28) * 8.0 / (mbit * 1e6) * 1e9);
    s.pacer = &pacer;
    s.probe = probe;
    s.rate_bps = mbit * 1e6;
    s.rate_max_bps = mbit_max * 1e6;
    s.probe_step = 0.25;
    s.copies = 1;
    s.trace = getenv("FT_TRACE") != NULL;

    s.start_mono_ns = mono_ns();
    pacer_reset(&pacer);
    uint64_t next_report_ns = s.start_mono_ns + report_interval_ns(&s);
    uint64_t fin_attempts = 0;
    uint64_t initial_pass_end_ns = 0;

    while (!s.complete) {
        drain_feedback(&s, feedback);
        if (s.complete) break;
        uint64_t now = mono_ns();

        if (s.next_new < s.total_pkts) {
            /* Streaming phase: repairs take priority over new data, and a
             * report request goes out every report interval regardless. */
            if (now >= next_report_ns) {
                send_fin(&s);
                next_report_ns = now + report_interval_ns(&s);
            }
            uint64_t sequence;
            if (next_pending(&s, &sequence)) {
                send_data(&s, sequence);
                s.retransmitted++;
            } else {
                send_data(&s, s.next_new++);
                if (s.next_new == s.total_pkts) initial_pass_end_ns = mono_ns();
            }
            pacer_wait(&pacer);
            continue;
        }

        /* Final phase: everything has been sent at least once. */
        uint64_t sequence;
        if (next_pending(&s, &sequence)) {
            for (int copy = 0; copy < s.copies; copy++) {
                send_data(&s, sequence);
                if (copy) s.redundant++;
                pacer_wait(&pacer);
            }
            s.retransmitted++;
            continue;
        }

        if (s.outstanding && now >= s.round_sent_ns + fin_timeout_ns(&s)) {
            s.outstanding = 0;               /* report lost: ask again */
        }
        if (!s.outstanding) {
            if (fin_attempts >= FT_MAX_FIN_ATTEMPTS) break;
            /* The path is FIFO, so a FIN sent right behind the last repair
             * is answered with a report that already reflects it. */
            send_fin(&s);
            fin_attempts++;
            continue;
        }
        wait_readable(s.sock, s.round_sent_ns + fin_timeout_ns(&s));
    }

    if (!s.complete) {
        fprintf(stderr, "receiver did not confirm completion after %" PRIu64 " FIN attempts\n",
                fin_attempts);
        return EXIT_FAILURE;
    }

    /* Echo the completion timestamp so the receiver can close immediately. */
    struct ft_done close_ack = { .receiver_end_realtime_ns = s.receiver_end_ns };
    done_hton(&close_ack);
    make_hdr((struct ft_hdr *)s.datagram, FT_DONE_ACK, 0);
    memcpy(s.datagram + FT_HDRLEN, &close_ack, sizeof(close_ack));
    for (int copy = 0; copy < 5; copy++) {
        send_retry(&s, s.datagram, FT_HDRLEN + sizeof(close_ack));
        sleep_ms(2);
    }

    uint64_t done_ns = mono_ns();
    double initial_seconds = initial_pass_end_ns ? (initial_pass_end_ns - s.start_mono_ns) / 1e9 : 0.0;
    double sender_runtime = (done_ns - s.start_mono_ns) / 1e9;

    printf("sender_start_realtime_ns=%" PRIu64 "\n", s.sender_start_ns);
    printf("--- client sender ---\n");
    printf("  original packets   %" PRIu64 "\n", s.total_pkts);
    printf("  retransmitted      %" PRIu64 " (%.2f%%), plus %" PRIu64 " redundant tail copies\n",
           s.retransmitted, 100.0 * s.retransmitted / (double)s.total_pkts, s.redundant);
    printf("  loss estimate      %.2f%%\n", 100.0 * s.loss_estimate);
    if (probe)
        printf("  rate probe         %d windows, final rate %.1f Mbit/s\n",
               s.probe_windows, s.rate_bps / 1e6);
    printf("  feedback rounds    %" PRIu64 " (%" PRIu64 " after the first pass)\n",
           s.reports_sent, fin_attempts);
    printf("  feedback datagrams %" PRIu64 " received, %" PRIu64 " requests accepted, %" PRIu64
           " stale\n", s.feedback_datagrams, s.nack_accepted, s.nack_filtered);
    printf("  rtt                min %.2f ms, smoothed %.2f ms\n",
           s.rtt_min_ns / 1e6, s.srtt_ns / 1e6);
    printf("  icmp unreachable   %" PRIu64 "\n", s.icmp_errors);
    printf("  initial pass       %.3f s\n", initial_seconds);
    printf("  sender runtime     %.3f s\n", sender_runtime);
    printf("  receiver_end_realtime_ns=%" PRIu64 "\n", s.receiver_end_ns);

    if (s.receiver_end_ns > s.sender_start_ns) {
        double one_way = (s.receiver_end_ns - s.sender_start_ns) / 1e9;
        printf("  one-way elapsed    %.6f s\n", one_way);
        printf("  payload throughput %.2f Mbit/s\n", s.file_size * 8.0 / one_way / 1e6);
    } else {
        printf("  one-way elapsed unavailable: verify VM clock synchronization\n");
    }
    printf("  transfer complete; verify both files with md5sum\n");

    free(s.last_sent_ms);
    free(s.pending);
    free(feedback);
    free(s.datagram);
    close(s.fd);
    close(s.sock);
    freeaddrinfo(addresses);
    return EXIT_SUCCESS;
}
