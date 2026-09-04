/* EE542 Lab 2 - shared UDP wire format.
 *
 * The protocol intentionally stays small:
 *   META -> READY -> DATA... -> FIN
 *                         missing: NACK... -> NACK_END -> retransmit -> FIN
 *                         complete: DONE
 */
#ifndef PROTO_H
#define PROTO_H

#include <arpa/inet.h>
#include <stdint.h>
#include <string.h>

#define FT_MAGIC 0x46543032u        /* "FT02" */

enum ft_type {
    FT_META     = 1,
    FT_DATA     = 2,
    FT_FIN      = 3,
    FT_READY    = 4,
    FT_NACK     = 5,
    FT_NACK_END = 6,
    FT_DONE     = 7
};

/* Fixed 16-byte header. For FT_NACK, seq is the feedback round number. */
struct ft_hdr {
    uint32_t magic;
    uint8_t  type;
    uint8_t  flags;
    uint16_t rsvd;
    uint64_t seq;
};

/* FT_META body. */
struct ft_meta {
    uint64_t file_size;
    uint64_t total_pkts;
    uint32_t payload_size;
    uint32_t rsvd;
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
    m->file_size    = ft_hton64(m->file_size);
    m->total_pkts   = ft_hton64(m->total_pkts);
    m->payload_size = htonl(m->payload_size);
    m->rsvd         = htonl(m->rsvd);
}

static inline void meta_ntoh(struct ft_meta *m)
{
    m->file_size    = ft_ntoh64(m->file_size);
    m->total_pkts   = ft_ntoh64(m->total_pkts);
    m->payload_size = ntohl(m->payload_size);
    m->rsvd         = ntohl(m->rsvd);
}

static inline void done_hton(struct ft_done *d)
{
    d->receiver_end_realtime_ns = ft_hton64(d->receiver_end_realtime_ns);
}

static inline void done_ntoh(struct ft_done *d)
{
    d->receiver_end_realtime_ns = ft_ntoh64(d->receiver_end_realtime_ns);
}

static inline void make_hdr(struct ft_hdr *h, uint8_t type, uint64_t seq)
{
    memset(h, 0, sizeof(*h));
    h->magic = FT_MAGIC;
    h->type = type;
    h->seq = seq;
    hdr_hton(h);
}

#define FT_HDRLEN ((int)sizeof(struct ft_hdr))
#define MAX_DGRAM 65507

#endif
