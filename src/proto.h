/* EE542 Lab 2 - shared UDP wire format.
 *
 * The protocol intentionally stays small:
 *   META -> READY -> DATA... interleaved with FIN/NACK feedback -> DONE -> DONE_ACK
 *
 * Feedback is sender driven. Every report interval the sender emits a FIN
 * (a "report request") and the receiver answers with the set of sequence
 * numbers it has not seen yet, followed by NACK_END. While the initial pass
 * is still streaming, the FIN carries no flags and the receiver only reports
 * gaps below the highest sequence it has seen so far. Once every chunk has
 * been sent at least once the FIN carries FT_FLAG_ALL_SENT and the report
 * covers the whole file. A complete receiver answers any FIN with DONE.
 *
 * The missing set is encoded either as a list of 64-bit sequence numbers
 * (NACK) or as a bitmap (NACK_BITMAP). The receiver picks whichever is
 * smaller on the wire: the list wins below ~1.6% loss, the bitmap above it.
 */
#ifndef PROTO_H
#define PROTO_H

#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>

#define FT_MAGIC 0x46543032u        /* "FT02" */

enum ft_type {
    FT_META        = 1,
    FT_DATA        = 2,
    FT_FIN         = 3,   /* report request; flags may carry FT_FLAG_ALL_SENT */
    FT_READY       = 4,
    FT_NACK        = 5,   /* body: uint64_t missing sequence numbers */
    FT_NACK_END    = 6,   /* body: struct ft_nack_end */
    FT_DONE        = 7,   /* body: struct ft_done */
    FT_DONE_ACK    = 8,   /* body: struct ft_done (echo) */
    FT_NACK_BITMAP = 9    /* body: uint64_t base_seq, then bitmap (1 = missing) */
};

/* FT_FIN flags. */
#define FT_FLAG_ALL_SENT 0x01

/* Sender-side defaults. Timeouts are derived from the measured RTT at run
 * time; these bound the derivation. */
#define FT_MIN_REPORT_MS      20
#define FT_MAX_REPORT_MS      400
#define FT_MAX_FIN_ATTEMPTS   400

/* Once the receiver has sent DONE it lingers this long for DONE_ACK. The
 * sender tolerates ICMP port-unreachable, so a short linger is safe. */
#define FT_COMPLETION_LINGER_SECONDS 10

/* Fixed 16-byte header. For FT_FIN, FT_NACK, FT_NACK_BITMAP and FT_NACK_END,
 * seq is the feedback round number. */
struct ft_hdr {
    uint32_t magic;
    uint8_t  type;
    uint8_t  flags;
    uint16_t rsvd;
    uint64_t seq;
};

/* FT_META body. feedback_mbit tells the receiver how fast it may send its
 * feedback datagrams; the reverse path is rate limited just like the
 * forward one, so an unpaced burst of NACKs would be tail-dropped. */
struct ft_meta {
    uint64_t file_size;
    uint64_t total_pkts;
    uint32_t payload_size;
    uint32_t feedback_mbit;
};

/* FT_NACK_END body: what the report covered, so the sender can log the
 * receiver's view without re-deriving it. */
struct ft_nack_end {
    uint64_t reported_limit;    /* sequences [0, reported_limit) were checked */
    uint64_t missing;           /* count of missing sequences in that range */
    uint64_t received_pkts;     /* unique chunks received so far */
};

/* FT_DONE body. The receiver records this when recvfrom() has delivered the
 * last unique file chunk. CLOCK_REALTIME is used so the sender can combine
 * this value with its own start timestamp after the VM clocks are verified. */
struct ft_done {
    uint64_t receiver_end_realtime_ns;
};

static inline uint64_t ft_hton64(uint64_t value)
{
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    return value;
#else
    return __builtin_bswap64(value);
#endif
}

static inline uint64_t ft_ntoh64(uint64_t value)
{
    return ft_hton64(value);
}

static inline void hdr_hton(struct ft_hdr *h)
{
    h->magic = htonl(h->magic);
    h->rsvd  = htons(h->rsvd);
    h->seq   = ft_hton64(h->seq);
}

static inline void hdr_ntoh(struct ft_hdr *h)
{
    h->magic = ntohl(h->magic);
    h->rsvd  = ntohs(h->rsvd);
    h->seq   = ft_ntoh64(h->seq);
}

static inline void meta_hton(struct ft_meta *m)
{
    m->file_size     = ft_hton64(m->file_size);
    m->total_pkts    = ft_hton64(m->total_pkts);
    m->payload_size  = htonl(m->payload_size);
    m->feedback_mbit = htonl(m->feedback_mbit);
}

static inline void meta_ntoh(struct ft_meta *m)
{
    m->file_size     = ft_ntoh64(m->file_size);
    m->total_pkts    = ft_ntoh64(m->total_pkts);
    m->payload_size  = ntohl(m->payload_size);
    m->feedback_mbit = ntohl(m->feedback_mbit);
}

static inline void nack_end_hton(struct ft_nack_end *e)
{
    e->reported_limit = ft_hton64(e->reported_limit);
    e->missing        = ft_hton64(e->missing);
    e->received_pkts  = ft_hton64(e->received_pkts);
}

static inline void nack_end_ntoh(struct ft_nack_end *e)
{
    e->reported_limit = ft_ntoh64(e->reported_limit);
    e->missing        = ft_ntoh64(e->missing);
    e->received_pkts  = ft_ntoh64(e->received_pkts);
}

static inline void done_hton(struct ft_done *d)
{
    d->receiver_end_realtime_ns = ft_hton64(d->receiver_end_realtime_ns);
}

static inline void done_ntoh(struct ft_done *d)
{
    d->receiver_end_realtime_ns = ft_ntoh64(d->receiver_end_realtime_ns);
}

static inline void make_hdr_flags(struct ft_hdr *h, uint8_t type, uint8_t flags,
                                  uint64_t seq)
{
    memset(h, 0, sizeof(*h));
    h->magic = FT_MAGIC;
    h->type = type;
    h->flags = flags;
    h->seq = seq;
    hdr_hton(h);
}

static inline void make_hdr(struct ft_hdr *h, uint8_t type, uint64_t seq)
{
    make_hdr_flags(h, type, 0, seq);
}

/* Bitmap helpers shared by both ends. */
#define BM_MARK(b, i)  ((b)[(i) >> 3] |= (uint8_t)(1u << ((i) & 7)))
#define BM_CLEAR(b, i) ((b)[(i) >> 3] &= (uint8_t)~(1u << ((i) & 7)))
#define BM_TEST(b, i)  ((b)[(i) >> 3] &  (uint8_t)(1u << ((i) & 7)))

#define FT_HDRLEN ((int)sizeof(struct ft_hdr))
#define MAX_DGRAM 65507

#endif
