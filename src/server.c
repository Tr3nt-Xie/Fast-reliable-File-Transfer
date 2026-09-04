/* server - receives a file sent by client over UDP.
 *
 *   ./server <port> <outfile> [rcvbuf_bytes]
 *   ./server 9000 received.bin 4194304
 *
 * Waits for FT_META to learn the file size and layout, then writes each
 * FT_DATA chunk straight to its final offset with pwrite(). Out-of-order
 * arrival therefore costs nothing - the file system does the reassembly, and
 * no reordering buffer is needed.
 *
 * Arrivals are tracked in a bitmap, one bit per chunk. At 1 GB / 1472 B that
 * is ~730k bits = 90 KB. The same structure is what a NACK scheme would walk
 * to find gaps, so it is worth building properly now.
 *
 * This stage has no retransmission. Under loss the file will be incomplete
 * and MD5 will not match; the report tells you exactly how much is missing.
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
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "proto.h"

#define MARK(b,i) ((b)[(i)>>3] |=  (uint8_t)(1u << ((i)&7)))
#define TEST(b,i) ((b)[(i)>>3] &   (uint8_t)(1u << ((i)&7)))

static void die(const char *msg) { perror(msg); exit(EXIT_FAILURE); }

static double secs_between(struct timespec a, struct timespec b)
{
    return (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) / 1e9;
}

/* One field from the Udp: line of /proc/net/snmp, or -1. Used to tell
 * "the network dropped it" from "we could not drain the socket in time". */
static long long udp_stat(const char *field)
{
    FILE *f = fopen("/proc/net/snmp", "r");
    if (!f) return -1;
    char hdr[1024], val[1024];
    long long out = -1;
    while (fgets(hdr, sizeof hdr, f)) {
        if (strncmp(hdr, "Udp:", 4)) continue;
        if (!fgets(val, sizeof val, f)) break;
        char *ht, *vt;
        char *hp = strtok_r(hdr + 4, " \t\n", &ht);
        char *vp = strtok_r(val + 4, " \t\n", &vt);
        while (hp && vp) {
            if (!strcmp(hp, field)) { out = atoll(vp); break; }
            hp = strtok_r(NULL, " \t\n", &ht);
            vp = strtok_r(NULL, " \t\n", &vt);
        }
        break;
    }
    fclose(f);
    return out;
}

int main(int argc, char *argv[])
{
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "usage: %s <port> <outfile> [rcvbuf_bytes]\n", argv[0]);
        return EXIT_FAILURE;
    }
    int port     = atoi(argv[1]);
    const char *out = argv[2];
    int want_buf = (argc == 4) ? atoi(argv[3]) : 4 * 1024 * 1024;

    if (port < 1 || port > 65535) {
        fprintf(stderr, "invalid port: %s\n", argv[1]);
        return EXIT_FAILURE;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) die("socket");

    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    /* Read the granted size back: the kernel clamps to net.core.rmem_max and
     * reports twice what it actually gave. A too-small buffer shows up as
     * packet loss on a link that is not losing anything. */
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &want_buf, sizeof(want_buf));
    int got_buf = 0; socklen_t gl = sizeof got_buf;
    getsockopt(sock, SOL_SOCKET, SO_RCVBUF, &got_buf, &gl);

    struct sockaddr_in me = {0};
    me.sin_family = AF_INET;
    me.sin_addr.s_addr = htonl(INADDR_ANY);
    me.sin_port = htons((uint16_t)port);
    if (bind(sock, (struct sockaddr *)&me, sizeof me) < 0) die("bind");

    fprintf(stderr, "server: listening on :%d -> %s   SO_RCVBUF asked %d, got %d\n",
            port, out, want_buf, got_buf);
    fprintf(stderr, "waiting for META...\n");

    char *dg = malloc(MAX_DGRAM);
    if (!dg) die("malloc");

    uint64_t file_size = 0, total_pkts = 0;
    uint32_t payload = 0;
    uint8_t *seen = NULL;
    int fd = -1, have_meta = 0, started = 0;
    uint64_t recvd = 0, dup = 0, bogus = 0, maxseq = 0;
    long long rbe0 = udp_stat("RcvbufErrors"), ie0 = udp_stat("InErrors");
    struct timespec t0, t1;

    /* Block indefinitely for the first packet - the server is normally started
     * long before the client. Only arm the idle timeout once data is flowing,
     * otherwise the server gives up before the client has begun. */
    const int IDLE_TIMEOUT_SEC = 5;

    for (;;) {
        struct sockaddr_in from; socklen_t fl = sizeof from;
        ssize_t n = recvfrom(sock, dg, MAX_DGRAM, 0, (struct sockaddr *)&from, &fl);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;   /* stream ended */
            die("recvfrom");
        }
        if (n < FT_HDRLEN) { bogus++; continue; }

        struct ft_hdr h;
        memcpy(&h, dg, FT_HDRLEN);
        hdr_ntoh(&h);
        if (h.magic != FT_MAGIC) { bogus++; continue; }   /* not ours */

        if (h.type == FT_META) {
            if (have_meta) continue;                      /* repeats are expected */
            if (n < FT_HDRLEN + (ssize_t)sizeof(struct ft_meta)) { bogus++; continue; }
            struct ft_meta m;
            memcpy(&m, dg + FT_HDRLEN, sizeof m);
            meta_ntoh(&m);
            file_size = m.file_size; total_pkts = m.total_pkts; payload = m.payload_size;

            char ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &from.sin_addr, ip, sizeof ip);
            fprintf(stderr, "META from %s: %" PRIu64 " B, %" PRIu64 " pkts of %u B\n",
                    ip, file_size, total_pkts, payload);

            fd = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) die("open outfile");
            if (ftruncate(fd, (off_t)file_size) < 0) die("ftruncate");
            seen = calloc((total_pkts + 7) / 8, 1);
            if (!seen) die("calloc");
            have_meta = 1;
            continue;
        }

        if (h.type == FT_FIN) continue;   /* sender done; idle timeout ends us */

        if (h.type != FT_DATA || !have_meta) { bogus++; continue; }

        if (!started) {
            clock_gettime(CLOCK_MONOTONIC, &t0);
            started = 1;
            struct timeval tv = { .tv_sec = IDLE_TIMEOUT_SEC, .tv_usec = 0 };
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        }
        clock_gettime(CLOCK_MONOTONIC, &t1);

        uint64_t s = h.seq;
        if (s > maxseq) maxseq = s;
        if (s >= total_pkts) { bogus++; continue; }
        if (TEST(seen, s))   { dup++;   continue; }

        /* pwrite to the chunk's final offset: order of arrival is irrelevant. */
        size_t body = (size_t)(n - FT_HDRLEN);
        if (pwrite(fd, dg + FT_HDRLEN, body, (off_t)(s * (uint64_t)payload)) < 0)
            die("pwrite");
        MARK(seen, s); recvd++;
    }

    long long rbe1 = udp_stat("RcvbufErrors"), ie1 = udp_stat("InErrors");

    printf("--- server ---\n");
    if (!have_meta) {
        printf("  no META received; nothing was written.\n");
        return EXIT_FAILURE;
    }
    if (fd >= 0) { fsync(fd); close(fd); }

    double el = started ? secs_between(t0, t1) : 0;
    if (el < 1e-6) el = 1e-6;
    uint64_t missing = total_pkts > recvd ? total_pkts - recvd : 0;

    printf("  received     %" PRIu64 " / %" PRIu64 " pkts\n", recvd, total_pkts);
    printf("  missing      %" PRIu64 "  (%.2f%%)\n",
           missing, 100.0 * missing / (double)(total_pkts ? total_pkts : 1));
    printf("  duplicates   %" PRIu64 "\n", dup);
    printf("  bogus        %" PRIu64 "\n", bogus);
    printf("  max seq seen %" PRIu64 "   (expect %" PRIu64 ")\n",
           maxseq, total_pkts ? total_pkts - 1 : 0);
    printf("  elapsed      %.3f s\n", el);
    printf("  goodput      %.2f Mbit/s\n", recvd * (double)payload * 8 / el / 1e6);
    printf("  RcvbufErrors +%lld   InErrors +%lld\n", rbe1 - rbe0, ie1 - ie0);
    if (missing) {
        printf("  >> file is INCOMPLETE - md5 will not match. This stage has no\n"
               "     retransmission; that is the next design decision.\n");
        if (rbe1 > rbe0)
            printf("  >> RcvbufErrors grew: some loss is ours, not the network's.\n"
                   "     Raise SO_RCVBUF / net.core.rmem_max, or slow the sender.\n");
    } else {
        printf("  >> complete. Verify with: md5sum %s\n", out);
    }

    free(seen); free(dg); close(sock);
    return missing ? EXIT_FAILURE : EXIT_SUCCESS;
}
