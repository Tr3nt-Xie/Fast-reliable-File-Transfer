/* server - receives one file reliably over UDP.
 *
 *   ./server <port> <outfile> [rcvbuf_bytes]
 *   ./server 9000 received.bin 4194304
 *
 * DATA chunks are written directly to seq * payload_size with pwrite(). A
 * bitmap records arrivals. Each FIN causes the server to return either the
 * missing sequence numbers (NACK...NACK_END) or FT_DONE.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "proto.h"

#define MARK(b, i) ((b)[(i) >> 3] |= (uint8_t)(1u << ((i) & 7)))
#define TEST(b, i) ((b)[(i) >> 3] &  (uint8_t)(1u << ((i) & 7)))

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

static void set_timeout(int sock, int seconds)
{
    struct timeval timeout = { .tv_sec = seconds, .tv_usec = 0 };
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
        die("setsockopt SO_RCVTIMEO");
}

static int same_peer(const struct sockaddr_in *left, const struct sockaddr_in *right)
{
    return left->sin_family == right->sin_family &&
           left->sin_port == right->sin_port &&
           left->sin_addr.s_addr == right->sin_addr.s_addr;
}

static void sendto_retry(int sock, const void *buffer, size_t length,
                         const struct sockaddr_in *peer)
{
    for (;;) {
        ssize_t sent = sendto(sock, buffer, length, 0,
                              (const struct sockaddr *)peer, sizeof(*peer));
        if (sent == (ssize_t)length) return;
        if (sent < 0 && (errno == EINTR || errno == EAGAIN || errno == ENOBUFS)) {
            sleep_ms(1);
            continue;
        }
        if (sent < 0) die("sendto feedback");
        fprintf(stderr, "short UDP send: %zd/%zu\n", sent, length);
        exit(EXIT_FAILURE);
    }
}

static void pwrite_all(int fd, const void *buffer, size_t length, off_t offset)
{
    size_t done = 0;
    while (done < length) {
        ssize_t written = pwrite(fd, (const char *)buffer + done,
                                 length - done, offset + (off_t)done);
        if (written > 0) {
            done += (size_t)written;
            continue;
        }
        if (written < 0 && errno == EINTR) continue;
        if (written < 0) die("pwrite");
        fprintf(stderr, "pwrite returned zero\n");
        exit(EXIT_FAILURE);
    }
}

static long long udp_stat(const char *field)
{
    FILE *file = fopen("/proc/net/snmp", "r");
    if (!file) return -1;

    char header[1024], values[1024];
    long long result = -1;
    while (fgets(header, sizeof(header), file)) {
        if (strncmp(header, "Udp:", 4) != 0) continue;
        if (!fgets(values, sizeof(values), file)) break;

        char *header_state = NULL, *value_state = NULL;
        char *name = strtok_r(header + 4, " \t\n", &header_state);
        char *value = strtok_r(values + 4, " \t\n", &value_state);
        while (name && value) {
            if (strcmp(name, field) == 0) {
                result = atoll(value);
                break;
            }
            name = strtok_r(NULL, " \t\n", &header_state);
            value = strtok_r(NULL, " \t\n", &value_state);
        }
        break;
    }
    fclose(file);
    return result;
}

static void send_ready(int sock, char *datagram, const struct sockaddr_in *peer)
{
    make_hdr((struct ft_hdr *)datagram, FT_READY, 0);
    sendto_retry(sock, datagram, FT_HDRLEN, peer);
}

static void send_done(int sock, char *datagram, const struct sockaddr_in *peer,
                      uint64_t receiver_end_ns)
{
    struct ft_done done = { .receiver_end_realtime_ns = receiver_end_ns };
    done_hton(&done);
    for (int copy = 0; copy < 5; copy++) {
        make_hdr((struct ft_hdr *)datagram, FT_DONE, 0);
        memcpy(datagram + FT_HDRLEN, &done, sizeof(done));
        sendto_retry(sock, datagram, FT_HDRLEN + sizeof(done), peer);
        sleep_ms(2);
    }
}

static uint64_t send_nacks(int sock, char *datagram, uint32_t payload,
                           const struct sockaddr_in *peer, const uint8_t *seen,
                           uint64_t total_pkts, uint64_t feedback_round)
{
    size_t max_sequences = payload / sizeof(uint64_t);
    if (max_sequences == 0) {
        fprintf(stderr, "payload is too small for NACK feedback\n");
        exit(EXIT_FAILURE);
    }

    size_t count = 0;
    uint64_t missing = 0;
    for (uint64_t sequence = 0; sequence < total_pkts; sequence++) {
        if (TEST(seen, sequence)) continue;
        uint64_t wire = ft_hton64(sequence);
        memcpy(datagram + FT_HDRLEN + count * sizeof(uint64_t), &wire, sizeof(wire));
        count++;
        missing++;

        if (count == max_sequences) {
            make_hdr((struct ft_hdr *)datagram, FT_NACK, feedback_round);
            sendto_retry(sock, datagram, FT_HDRLEN + count * sizeof(uint64_t), peer);
            count = 0;
        }
    }
    if (count > 0) {
        make_hdr((struct ft_hdr *)datagram, FT_NACK, feedback_round);
        sendto_retry(sock, datagram, FT_HDRLEN + count * sizeof(uint64_t), peer);
    }

    for (int copy = 0; copy < 3; copy++) {
        make_hdr((struct ft_hdr *)datagram, FT_NACK_END, feedback_round);
        sendto_retry(sock, datagram, FT_HDRLEN, peer);
        sleep_ms(2);
    }
    return missing;
}

int main(int argc, char *argv[])
{
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "usage: %s <port> <outfile> [rcvbuf_bytes]\n", argv[0]);
        return EXIT_FAILURE;
    }

    int port = atoi(argv[1]);
    const char *output_path = argv[2];
    int requested_buffer = argc == 4 ? atoi(argv[3]) : 4 * 1024 * 1024;
    if (port < 1 || port > 65535 || requested_buffer <= 0) {
        fprintf(stderr, "invalid port or receive-buffer size\n");
        return EXIT_FAILURE;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) die("socket");
    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &requested_buffer, sizeof(requested_buffer));
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &requested_buffer, sizeof(requested_buffer));

    int granted_buffer = 0;
    socklen_t granted_length = sizeof(granted_buffer);
    getsockopt(sock, SOL_SOCKET, SO_RCVBUF, &granted_buffer, &granted_length);

    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons((uint16_t)port);
    if (bind(sock, (struct sockaddr *)&local, sizeof(local)) < 0) die("bind");

    fprintf(stderr, "server: listening on :%d -> %s, SO_RCVBUF asked %d, got %d\n",
            port, output_path, requested_buffer, granted_buffer);
    fprintf(stderr, "waiting for META...\n");

    char *datagram = malloc(MAX_DGRAM);
    if (!datagram) die("malloc datagram");

    uint64_t file_size = 0, total_pkts = 0, received_pkts = 0;
    uint64_t received_bytes = 0, duplicates = 0, bogus = 0, max_sequence = 0;
    uint64_t receiver_end_ns = 0;
    uint32_t payload = 0;
    uint8_t *seen = NULL;
    int output_fd = -1;
    int have_meta = 0, started = 0, complete = 0, synced = 0;
    struct sockaddr_in peer = {0};
    struct timespec first_arrival = {0}, last_arrival = {0};
    long long rcvbuf_before = udp_stat("RcvbufErrors");
    long long errors_before = udp_stat("InErrors");

    for (;;) {
        struct sockaddr_in from = {0};
        socklen_t from_length = sizeof(from);
        ssize_t length = recvfrom(sock, datagram, MAX_DGRAM, 0,
                                  (struct sockaddr *)&from, &from_length);
        if (length < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (complete) break;
                fprintf(stderr, "receiver timed out before file completion\n");
                break;
            }
            die("recvfrom");
        }
        if (length < FT_HDRLEN) {
            bogus++;
            continue;
        }

        struct ft_hdr header;
        memcpy(&header, datagram, FT_HDRLEN);
        hdr_ntoh(&header);
        if (header.magic != FT_MAGIC) {
            bogus++;
            continue;
        }

        if (header.type == FT_META) {
            if (length < FT_HDRLEN + (ssize_t)sizeof(struct ft_meta)) {
                bogus++;
                continue;
            }
            struct ft_meta metadata;
            memcpy(&metadata, datagram + FT_HDRLEN, sizeof(metadata));
            meta_ntoh(&metadata);

            if (have_meta) {
                if (same_peer(&peer, &from) && metadata.file_size == file_size &&
                    metadata.total_pkts == total_pkts &&
                    metadata.payload_size == payload) {
                    send_ready(sock, datagram, &peer);
                } else {
                    bogus++;
                }
                continue;
            }

            if (metadata.file_size == 0 || metadata.payload_size == 0 ||
                metadata.payload_size > MAX_DGRAM - FT_HDRLEN) {
                fprintf(stderr, "invalid META sizes\n");
                return EXIT_FAILURE;
            }
            uint64_t expected_packets =
                (metadata.file_size + metadata.payload_size - 1) / metadata.payload_size;
            if (metadata.total_pkts != expected_packets ||
                metadata.total_pkts > (uint64_t)SIZE_MAX * 8ull) {
                fprintf(stderr, "invalid META packet count\n");
                return EXIT_FAILURE;
            }

            file_size = metadata.file_size;
            total_pkts = metadata.total_pkts;
            payload = metadata.payload_size;
            peer = from;

            output_fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (output_fd < 0) die("open output");
            if (ftruncate(output_fd, (off_t)file_size) < 0) die("ftruncate");
            seen = calloc((size_t)((total_pkts + 7) / 8), 1);
            if (!seen) die("calloc bitmap");
            have_meta = 1;
            set_timeout(sock, 20);

            char address[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &peer.sin_addr, address, sizeof(address));
            fprintf(stderr, "META from %s:%u: %" PRIu64 " B, %" PRIu64
                    " packets of %u B\n",
                    address, ntohs(peer.sin_port), file_size, total_pkts, payload);
            send_ready(sock, datagram, &peer);
            continue;
        }

        if (!have_meta || !same_peer(&peer, &from)) {
            bogus++;
            continue;
        }

        if (header.type == FT_DATA) {
            if (header.seq >= total_pkts) {
                bogus++;
                continue;
            }
            uint64_t offset = header.seq * (uint64_t)payload;
            size_t expected = payload;
            if (file_size - offset < expected) expected = (size_t)(file_size - offset);
            size_t body = (size_t)(length - FT_HDRLEN);
            if (body != expected) {
                bogus++;
                continue;
            }
            if (TEST(seen, header.seq)) {
                duplicates++;
                continue;
            }

            struct timespec arrival;
            if (clock_gettime(CLOCK_MONOTONIC, &arrival) < 0) die("clock_gettime");
            uint64_t arrival_realtime_ns = realtime_ns();
            if (!started) {
                first_arrival = arrival;
                started = 1;
            }

            pwrite_all(output_fd, datagram + FT_HDRLEN, body, (off_t)offset);
            MARK(seen, header.seq);
            received_pkts++;
            received_bytes += body;
            last_arrival = arrival;
            if (header.seq > max_sequence) max_sequence = header.seq;

            if (received_pkts == total_pkts) {
                receiver_end_ns = arrival_realtime_ns;
                complete = 1;
                printf("receiver_end_realtime_ns=%" PRIu64 "\n", receiver_end_ns);
                fflush(stdout);
            }
            continue;
        }

        if (header.type == FT_DONE_ACK) {
            if (complete && synced &&
                length == FT_HDRLEN + (ssize_t)sizeof(struct ft_done)) {
                struct ft_done ack;
                memcpy(&ack, datagram + FT_HDRLEN, sizeof(ack));
                done_ntoh(&ack);
                if (ack.receiver_end_realtime_ns == receiver_end_ns) {
                    /* Drain repeated close acknowledgements before closing the
                     * socket, avoiding ICMP errors at the connected sender. */
                    set_timeout(sock, 1);
                    continue;
                }
            }
            bogus++;
            continue;
        }

        if (header.type == FT_FIN) {
            if (complete) {
                if (!synced) {
                    if (fsync(output_fd) < 0) die("fsync");
                    synced = 1;
                }
                send_done(sock, datagram, &peer, receiver_end_ns);
                set_timeout(sock, FT_COMPLETION_LINGER_SECONDS);
            } else {
                uint64_t missing = send_nacks(sock, datagram, payload, &peer,
                                              seen, total_pkts, header.seq);
                fprintf(stderr, "feedback round %" PRIu64 ": %" PRIu64 " missing\n",
                        header.seq, missing);
            }
            continue;
        }

        bogus++;
    }

    long long rcvbuf_after = udp_stat("RcvbufErrors");
    long long errors_after = udp_stat("InErrors");
    if (output_fd >= 0) {
        if (!synced && fsync(output_fd) < 0) die("fsync");
        close(output_fd);
    }

    uint64_t missing = total_pkts > received_pkts ? total_pkts - received_pkts : 0;
    double elapsed = started ? secs_between(first_arrival, last_arrival) : 0.0;
    if (elapsed < 1e-9) elapsed = 1e-9;

    printf("--- server receiver ---\n");
    if (!have_meta) {
        printf("  no META received; nothing written\n");
    } else {
        printf("  received packets   %" PRIu64 " / %" PRIu64 "\n",
               received_pkts, total_pkts);
        printf("  received bytes     %" PRIu64 " / %" PRIu64 "\n",
               received_bytes, file_size);
        printf("  missing            %" PRIu64 " (%.2f%%)\n", missing,
               100.0 * missing / (double)total_pkts);
        printf("  duplicates         %" PRIu64 "\n", duplicates);
        printf("  bogus              %" PRIu64 "\n", bogus);
        printf("  max sequence       %" PRIu64 " (expect %" PRIu64 ")\n",
               max_sequence, total_pkts - 1);
        printf("  receive span       %.3f s\n", elapsed);
        printf("  receive goodput    %.2f Mbit/s\n",
               received_bytes * 8.0 / elapsed / 1e6);
        printf("  receiver_end_realtime_ns=%" PRIu64 "\n", receiver_end_ns);
        printf("  RcvbufErrors +%lld, InErrors +%lld\n",
               rcvbuf_after - rcvbuf_before, errors_after - errors_before);
        if (missing == 0) printf("  transfer complete; verify with md5sum %s\n", output_path);
        else printf("  transfer incomplete\n");
    }

    free(seen);
    free(datagram);
    close(sock);
    return have_meta && missing == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
