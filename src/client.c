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
 * Reliability is deliberately simple. The client sends one paced pass and a
 * FIN. The server returns the missing sequence numbers in NACK packets. Only
 * those chunks are retransmitted, and the cycle repeats until FT_DONE.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
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

#define MARK(b, i) ((b)[(i) >> 3] |= (uint8_t)(1u << ((i) & 7)))
#define TEST(b, i) ((b)[(i) >> 3] &  (uint8_t)(1u << ((i) & 7)))

struct pacer {
    uint64_t interval_ns;
    struct timespec next;
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

static double secs_between(struct timespec start, struct timespec end)
{
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

static void sleep_ms(long milliseconds)
{
    struct timespec delay = {
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000000L
    };
    while (nanosleep(&delay, &delay) < 0 && errno == EINTR) {}
}

static void add_ns(struct timespec *time, uint64_t nanoseconds)
{
    time->tv_sec += (time_t)(nanoseconds / 1000000000ull);
    time->tv_nsec += (long)(nanoseconds % 1000000000ull);
    if (time->tv_nsec >= 1000000000L) {
        time->tv_sec++;
        time->tv_nsec -= 1000000000L;
    }
}

static int timespec_before(struct timespec a, struct timespec b)
{
    return a.tv_sec < b.tv_sec || (a.tv_sec == b.tv_sec && a.tv_nsec < b.tv_nsec);
}

static void pacer_reset(struct pacer *pacer)
{
    if (clock_gettime(CLOCK_MONOTONIC, &pacer->next) < 0) die("clock_gettime monotonic");
}

static void pacer_wait(struct pacer *pacer)
{
    if (!pacer->interval_ns) return;
    add_ns(&pacer->next, pacer->interval_ns);

    for (;;) {
        struct timespec now, delay;
        if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) die("clock_gettime monotonic");
        if (!timespec_before(now, pacer->next)) return;
        delay.tv_sec = pacer->next.tv_sec - now.tv_sec;
        delay.tv_nsec = pacer->next.tv_nsec - now.tv_nsec;
        if (delay.tv_nsec < 0) {
            delay.tv_sec--;
            delay.tv_nsec += 1000000000L;
        }
        if (nanosleep(&delay, NULL) == 0) return;
        if (errno != EINTR) die("nanosleep");
    }
}

static void send_retry(int sock, const void *buffer, size_t length)
{
    for (;;) {
        ssize_t sent = send(sock, buffer, length, 0);
        if (sent == (ssize_t)length) return;
        if (sent < 0 && (errno == EINTR || errno == EAGAIN || errno == ENOBUFS)) {
            sleep_ms(1);
            continue;
        }
        if (sent < 0) die("send");
        fprintf(stderr, "short UDP send: %zd/%zu\n", sent, length);
        exit(EXIT_FAILURE);
    }
}

static size_t read_chunk(int fd, void *buffer, size_t length, off_t offset)
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
    return done;
}

static void send_data(int sock, int fd, uint64_t sequence, uint32_t payload,
                      uint64_t file_size, char *datagram, struct pacer *pacer)
{
    uint64_t offset = sequence * (uint64_t)payload;
    size_t body = payload;
    if (file_size - offset < body) body = (size_t)(file_size - offset);

    read_chunk(fd, datagram + FT_HDRLEN, body, (off_t)offset);
    make_hdr((struct ft_hdr *)datagram, FT_DATA, sequence);
    send_retry(sock, datagram, FT_HDRLEN + body);
    pacer_wait(pacer);
}

static int receive_type(int sock, uint8_t wanted, int timeout_ms,
                        char *buffer, size_t capacity, ssize_t *received)
{
    struct pollfd pfd = { .fd = sock, .events = POLLIN };
    int ready;
    do {
        ready = poll(&pfd, 1, timeout_ms);
    } while (ready < 0 && errno == EINTR);
    if (ready < 0) die("poll");
    if (ready == 0) return 0;

    ssize_t length = recv(sock, buffer, capacity, 0);
    if (length < 0) {
        if (errno == EINTR || errno == EAGAIN) return 0;
        die("recv");
    }
    if (length < FT_HDRLEN) return 0;

    struct ft_hdr header;
    memcpy(&header, buffer, FT_HDRLEN);
    hdr_ntoh(&header);
    if (header.magic != FT_MAGIC || header.type != wanted) return 0;
    if (received) *received = length;
    return 1;
}

static void wait_for_ready(int sock, char *datagram, const struct ft_meta *metadata)
{
    char reply[MAX_DGRAM];
    for (int attempt = 1; attempt <= 50; attempt++) {
        struct ft_meta wire = *metadata;
        make_hdr((struct ft_hdr *)datagram, FT_META, 0);
        meta_hton(&wire);
        memcpy(datagram + FT_HDRLEN, &wire, sizeof(wire));
        send_retry(sock, datagram, FT_HDRLEN + sizeof(wire));
        if (receive_type(sock, FT_READY, 500, reply, sizeof(reply), NULL)) {
            fprintf(stderr, "receiver READY after %d META attempt(s)\n", attempt);
            return;
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
                "  mbit: paced wire rate, default 90; 0 means unpaced\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    const char *host = argv[1];
    const char *port = argv[2];
    const char *path = argv[3];
    int dgram = argc >= 5 ? atoi(argv[4]) : 1472;
    double mbit = argc >= 6 ? atof(argv[5]) : 90.0;

    if (dgram < FT_HDRLEN + (int)sizeof(struct ft_meta) || dgram > MAX_DGRAM) {
        fprintf(stderr, "dgram_bytes must be %d..%d\n",
                FT_HDRLEN + (int)sizeof(struct ft_meta), MAX_DGRAM);
        return EXIT_FAILURE;
    }
    if (mbit < 0) {
        fprintf(stderr, "mbit must be non-negative\n");
        return EXIT_FAILURE;
    }
    uint32_t payload = (uint32_t)(dgram - FT_HDRLEN);

    struct addrinfo hints = {0}, *addresses = NULL;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    int rc = getaddrinfo(host, port, &hints, &addresses);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo(%s:%s): %s\n", host, port, gai_strerror(rc));
        return EXIT_FAILURE;
    }

    int sock = socket(addresses->ai_family, addresses->ai_socktype, 0);
    if (sock < 0) die("socket");
    if (connect(sock, addresses->ai_addr, addresses->ai_addrlen) < 0) die("connect");

    int socket_buffer = 4 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &socket_buffer, sizeof(socket_buffer));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &socket_buffer, sizeof(socket_buffer));

    int fd = open(path, O_RDONLY);
    if (fd < 0) die("open source");
    struct stat status;
    if (fstat(fd, &status) < 0) die("fstat");
    if (status.st_size <= 0) {
        fprintf(stderr, "source file must not be empty\n");
        return EXIT_FAILURE;
    }

    uint64_t file_size = (uint64_t)status.st_size;
    uint64_t total_pkts = (file_size + payload - 1) / payload;
    struct ft_meta metadata = {
        .file_size = file_size,
        .total_pkts = total_pkts,
        .payload_size = payload,
        .rsvd = 0
    };

    char *datagram = malloc((size_t)dgram);
    char *feedback = malloc(MAX_DGRAM);
    size_t bitmap_bytes = (size_t)((total_pkts + 7) / 8);
    uint8_t *requested = calloc(bitmap_bytes, 1);
    if (!datagram || !feedback || !requested) die("allocate transfer buffers");

    fprintf(stderr,
            "client: %s -> %s:%s, %" PRIu64 " B in %" PRIu64
            " packets (%u B data + %d B header = %d B UDP)",
            path, host, port, file_size, total_pkts, payload, FT_HDRLEN, dgram);
    if (mbit > 0) fprintf(stderr, ", paced at %.1f Mbit/s\n", mbit);
    else fprintf(stderr, ", unpaced\n");

    wait_for_ready(sock, datagram, &metadata);

    struct pacer pacer = {0};
    if (mbit > 0) {
        pacer.interval_ns = (uint64_t)((dgram + 28) * 8.0 / (mbit * 1e6) * 1e9);
    }
    pacer_reset(&pacer);

    struct timespec monotonic_start, monotonic_after_initial;
    if (clock_gettime(CLOCK_MONOTONIC, &monotonic_start) < 0) die("clock_gettime");
    uint64_t sender_start_ns = realtime_ns();
    printf("sender_start_realtime_ns=%" PRIu64 "\n", sender_start_ns);
    fflush(stdout);

    for (uint64_t sequence = 0; sequence < total_pkts; sequence++) {
        send_data(sock, fd, sequence, payload, file_size, datagram, &pacer);
    }
    if (clock_gettime(CLOCK_MONOTONIC, &monotonic_after_initial) < 0) die("clock_gettime");

    uint64_t retransmitted = 0;
    uint64_t feedback_round = 0;
    uint64_t fin_attempts = 0;
    uint64_t receiver_end_ns = 0;
    int complete = 0;

    while (!complete && fin_attempts < 200) {
        memset(requested, 0, bitmap_bytes);
        uint64_t requested_count = 0;
        make_hdr((struct ft_hdr *)datagram, FT_FIN, feedback_round);
        send_retry(sock, datagram, FT_HDRLEN);
        fin_attempts++;

        struct timespec wait_start, now;
        if (clock_gettime(CLOCK_MONOTONIC, &wait_start) < 0) die("clock_gettime");
        int feedback_finished = 0;

        while (!feedback_finished) {
            if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) die("clock_gettime");
            double waited = secs_between(wait_start, now);
            int remaining_ms = (int)((1.5 - waited) * 1000.0);
            if (remaining_ms <= 0) break;

            struct pollfd pfd = { .fd = sock, .events = POLLIN };
            int ready;
            do {
                ready = poll(&pfd, 1, remaining_ms);
            } while (ready < 0 && errno == EINTR);
            if (ready < 0) die("poll feedback");
            if (ready == 0) break;

            ssize_t length = recv(sock, feedback, MAX_DGRAM, 0);
            if (length < 0) {
                if (errno == EINTR || errno == EAGAIN) continue;
                die("recv feedback");
            }
            if (length < FT_HDRLEN) continue;

            struct ft_hdr header;
            memcpy(&header, feedback, FT_HDRLEN);
            hdr_ntoh(&header);
            if (header.magic != FT_MAGIC) continue;

            if (header.type == FT_DONE &&
                length >= FT_HDRLEN + (ssize_t)sizeof(struct ft_done)) {
                struct ft_done done;
                memcpy(&done, feedback + FT_HDRLEN, sizeof(done));
                done_ntoh(&done);
                receiver_end_ns = done.receiver_end_realtime_ns;
                complete = 1;
                break;
            }
            if (header.seq != feedback_round) continue;

            if (header.type == FT_NACK) {
                ssize_t body = length - FT_HDRLEN;
                if (body < 0 || body % (ssize_t)sizeof(uint64_t) != 0) continue;
                size_t count = (size_t)body / sizeof(uint64_t);
                for (size_t index = 0; index < count; index++) {
                    uint64_t wire_sequence;
                    memcpy(&wire_sequence,
                           feedback + FT_HDRLEN + index * sizeof(uint64_t),
                           sizeof(wire_sequence));
                    uint64_t sequence = ft_ntoh64(wire_sequence);
                    if (sequence < total_pkts && !TEST(requested, sequence)) {
                        MARK(requested, sequence);
                        requested_count++;
                    }
                }
            } else if (header.type == FT_NACK_END) {
                feedback_finished = 1;
            }
        }

        if (complete) break;
        if (requested_count == 0) {
            fprintf(stderr, "feedback timeout/empty round; repeating FIN\n");
            continue;
        }

        fprintf(stderr, "retransmission round %" PRIu64 ": %" PRIu64 " requested\n",
                feedback_round + 1, requested_count);
        pacer_reset(&pacer);
        for (uint64_t sequence = 0; sequence < total_pkts; sequence++) {
            if (TEST(requested, sequence)) {
                send_data(sock, fd, sequence, payload, file_size, datagram, &pacer);
                retransmitted++;
            }
        }
        feedback_round++;
    }

    if (!complete) {
        fprintf(stderr, "receiver did not confirm completion after %" PRIu64 " FIN attempts\n",
                fin_attempts);
        return EXIT_FAILURE;
    }

    struct timespec monotonic_done;
    if (clock_gettime(CLOCK_MONOTONIC, &monotonic_done) < 0) die("clock_gettime");
    double initial_seconds = secs_between(monotonic_start, monotonic_after_initial);
    double sender_runtime = secs_between(monotonic_start, monotonic_done);

    printf("--- client sender ---\n");
    printf("  original packets   %" PRIu64 "\n", total_pkts);
    printf("  retransmitted      %" PRIu64 "\n", retransmitted);
    printf("  feedback rounds    %" PRIu64 "\n", feedback_round);
    printf("  initial pass       %.3f s\n", initial_seconds);
    printf("  sender runtime     %.3f s\n", sender_runtime);
    printf("  receiver_end_realtime_ns=%" PRIu64 "\n", receiver_end_ns);

    if (receiver_end_ns > sender_start_ns) {
        double one_way = (receiver_end_ns - sender_start_ns) / 1e9;
        printf("  one-way elapsed    %.6f s\n", one_way);
        printf("  payload throughput %.2f Mbit/s\n", file_size * 8.0 / one_way / 1e6);
    } else {
        printf("  one-way elapsed unavailable: verify VM clock synchronization\n");
    }
    printf("  transfer complete; verify both files with md5sum\n");

    free(requested);
    free(feedback);
    free(datagram);
    close(fd);
    close(sock);
    freeaddrinfo(addresses);
    return EXIT_SUCCESS;
}
