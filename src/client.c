/* client - sends a file to the server over UDP.
 *
 *   ./client <host> <port> <file> [dgram_bytes] [mbit]
 *   ./client 192.168.10.100 9000 data.bin 1472 100
 *
 * dgram_bytes is the size of the whole UDP payload, header included:
 *   MTU 1500 -> 1472  (1500 - IP 20 - UDP 8)
 *   MTU 9000 -> 8972
 * File data per packet is dgram_bytes - 16, since our header rides inside it.
 *
 * Getting this wrong is expensive and silent. Sending 1472 bytes of *file*
 * data makes a 1516-byte IP packet, 16 bytes over MTU 1500, so every datagram
 * is split into two fragments. Losing either one discards the whole datagram,
 * so 20% packet loss becomes 1 - 0.8^2 = 36% datagram loss - and the doubled
 * packet count then overflows netem's queue, which measured 92% loss.
 *
 * mbit defaults to 0, meaning send as fast as the CPU allows. That is almost
 * always wrong on a real link: it overruns queues and manufactures loss that
 * was not there. Pace slightly below the configured rate limit.
 *
 * This stage is a one-way pipeline: no acknowledgements, no retransmission.
 * On a clean link the file arrives intact; under loss it will not, and the
 * MD5 mismatch is the point - it shows exactly what reliability has to fix.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include "proto.h"

static void die(const char *msg) { perror(msg); exit(EXIT_FAILURE); }

static double secs_between(struct timespec a, struct timespec b)
{
    return (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) / 1e9;
}

int main(int argc, char *argv[])
{
    if (argc < 4 || argc > 6) {
        fprintf(stderr,
            "usage: %s <host> <port> <file> [dgram_bytes] [mbit]\n"
            "  dgram_bytes  total UDP payload incl. our %d-byte header;\n"
            "               1472 for MTU 1500 (default), 8972 for MTU 9000\n"
            "  mbit         target send rate; 0 = unpaced (default)\n",
            argv[0], FT_HDRLEN);
        return EXIT_FAILURE;
    }
    const char *host = argv[1], *port = argv[2], *path = argv[3];
    int dgram  = (argc >= 5) ? atoi(argv[4]) : 1472;
    double mbit = (argc >= 6) ? atof(argv[5]) : 0.0;

    if (dgram <= FT_HDRLEN || dgram > MAX_DGRAM) {
        fprintf(stderr, "dgram_bytes must be %d..%d\n", FT_HDRLEN + 1, MAX_DGRAM);
        return EXIT_FAILURE;
    }
    /* File bytes per packet. The header rides inside the same datagram, so it
     * must be subtracted here or the datagram exceeds the MTU and fragments. */
    int payload = dgram - FT_HDRLEN;

    /* getaddrinfo, not gethostbyname: the latter is deprecated, IPv4-only and
     * not thread-safe. This also accepts a literal address without special
     * casing. */
    struct addrinfo hints = {0}, *ai;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    int rc = getaddrinfo(host, port, &hints, &ai);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo(%s:%s): %s\n", host, port, gai_strerror(rc));
        return EXIT_FAILURE;
    }

    int sock = socket(ai->ai_family, ai->ai_socktype, 0);
    if (sock < 0) die("socket");

    int sndbuf = 4 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    int fd = open(path, O_RDONLY);
    if (fd < 0) die("open");
    struct stat st;
    if (fstat(fd, &st) < 0) die("fstat");

    uint64_t file_size  = (uint64_t)st.st_size;
    uint64_t total_pkts = (file_size + payload - 1) / payload;
    if (total_pkts == 0) total_pkts = 0;

    char *dg = malloc(FT_HDRLEN + payload);
    if (!dg) die("malloc");
    struct ft_hdr *h = (struct ft_hdr *)dg;

    /* --- META, repeated: if it is lost the receiver has no idea what is
     * coming. Repetition is a placeholder for real reliability. --- */
    for (int i = 0; i < 5; i++) {
        memset(h, 0, FT_HDRLEN);
        h->magic = FT_MAGIC; h->type = FT_META; h->seq = 0;
        struct ft_meta m = { .file_size = file_size,
                             .total_pkts = total_pkts,
                             .payload_size = (uint32_t)payload, .rsvd = 0 };
        hdr_hton(h); meta_hton(&m);
        memcpy(dg + FT_HDRLEN, &m, sizeof m);
        if (sendto(sock, dg, FT_HDRLEN + (int)sizeof m, 0,
                   ai->ai_addr, ai->ai_addrlen) < 0) die("sendto META");
        usleep(2000);
    }

    fprintf(stderr, "client: %s -> %s:%s  %" PRIu64 " B in %" PRIu64
                    " pkts (%d B data + %d B hdr = %d B UDP, %d B on wire)",
            path, host, port, file_size, total_pkts,
            payload, FT_HDRLEN, dgram, dgram + 28);
    uint64_t interval_ns = 0;
    if (mbit > 0) {
        /* Pace on what actually goes on the wire: our datagram plus UDP and
         * IP headers, otherwise we systematically overshoot the target rate. */
        interval_ns = (uint64_t)((dgram + 28) * 8.0 / (mbit * 1e6) * 1e9);
        fprintf(stderr, "  paced at %.1f Mbit/s\n", mbit);
    } else {
        fprintf(stderr, "  UNPACED\n");
    }

    /* --- DATA: one pass, no waiting. This is the shape a file transfer needs;
     * a send-then-wait loop would cap at one packet per round trip. --- */
    struct timespec t0, t1, next;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    next = t0;

    uint64_t sent = 0, send_errs = 0;
    for (uint64_t i = 0; i < total_pkts; i++) {
        ssize_t got = pread(fd, dg + FT_HDRLEN, payload, (off_t)(i * (uint64_t)payload));
        if (got < 0) die("pread");

        memset(h, 0, FT_HDRLEN);
        h->magic = FT_MAGIC; h->type = FT_DATA; h->seq = i;
        hdr_hton(h);

        if (sendto(sock, dg, FT_HDRLEN + got, 0, ai->ai_addr, ai->ai_addrlen) < 0) {
            send_errs++;
            if (errno != ENOBUFS && errno != EAGAIN) die("sendto DATA");
        } else {
            sent++;
        }

        if (interval_ns) {
            next.tv_nsec += interval_ns;
            while (next.tv_nsec >= 1000000000L) { next.tv_nsec -= 1000000000L; next.tv_sec++; }
            clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* FIN, repeated for the same reason as META. */
    for (int i = 0; i < 5; i++) {
        memset(h, 0, FT_HDRLEN);
        h->magic = FT_MAGIC; h->type = FT_FIN; h->seq = total_pkts;
        hdr_hton(h);
        sendto(sock, dg, FT_HDRLEN, 0, ai->ai_addr, ai->ai_addrlen);
        usleep(2000);
    }

    double el = secs_between(t0, t1);
    printf("--- client ---\n");
    printf("  sent       %" PRIu64 " / %" PRIu64 " pkts\n", sent, total_pkts);
    if (send_errs) printf("  send errors %" PRIu64 "\n", send_errs);
    printf("  file       %.1f MB\n", file_size / 1e6);
    printf("  elapsed    %.3f s\n", el);
    printf("  wire rate  %.2f Mbit/s\n", file_size * 8.0 / el / 1e6);

    free(dg); close(fd); close(sock); freeaddrinfo(ai);
    return EXIT_SUCCESS;
}
