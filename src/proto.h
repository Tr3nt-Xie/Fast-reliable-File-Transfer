/* EE542 Lab 2 - wire format shared by client (sender) and server (receiver).
 *
 * Both ends include this file. It is the contract: change it and both sides
 * must be rebuilt together.
 *
 * Header size is 16 bytes. On a 1472-byte datagram that is 1.1% overhead.
 *
 * Design notes, so the choices are not mysterious later:
 *
 *   magic   Catches datagrams that are not ours - a stray packet, or an old
 *           build still running on the same port. Without it garbage would be
 *           written into the output file and only show up as an MD5 mismatch.
 *
 *   seq     64-bit even though 1 GB / 1472 B = ~730k packets fits easily in 32.
 *           The extra 4 bytes cost 0.3% of the transfer and buy immunity from
 *           sequence wraparound, which is a miserable class of bug to debug
 *           once retransmission logic exists.
 *
 *   no len  Deliberately absent. recvfrom() returns the datagram length, so
 *           UDP preserves message boundaries for free. Carrying a length field
 *           would be importing TCP's framing problem into a protocol that does
 *           not have it.
 *
 *   META    File size and packet count are sent once in their own packet
 *           rather than repeated in every header. If META is lost the receiver
 *           does not know what to expect - that is our first reliability
 *           problem, and for now the sender simply repeats it.
 */
#ifndef PROTO_H
#define PROTO_H

#include <stdint.h>
#include <endian.h>

#define FT_MAGIC 0x46543032u        /* "FT02" */

enum ft_type {
    FT_META = 1,      /* transfer description: size, packet count, payload size */
    FT_DATA = 2,      /* one chunk of the file, at offset seq * payload_size    */
    FT_FIN  = 3       /* sender has emitted everything                          */
};

struct ft_hdr {
    uint32_t magic;
    uint8_t  type;
    uint8_t  flags;      /* unused for now; keeps the struct 8-byte aligned */
    uint16_t rsvd;
    uint64_t seq;        /* DATA: chunk index. META/FIN: unused. */
};

/* Body of an FT_META packet, immediately after the header. */
struct ft_meta {
    uint64_t file_size;      /* bytes */
    uint64_t total_pkts;     /* number of FT_DATA packets that will follow */
    uint32_t payload_size;   /* bytes of file data per FT_DATA packet */
    uint32_t rsvd;
};

/* All multi-byte fields travel big-endian. The two lab VMs are both aarch64
 * so this is not strictly required, but a protocol that only works between
 * identical machines is not a protocol. */
static inline void hdr_hton(struct ft_hdr *h)
{
    h->magic = htobe32(h->magic);
    h->rsvd  = htobe16(h->rsvd);
    h->seq   = htobe64(h->seq);
}
static inline void hdr_ntoh(struct ft_hdr *h)
{
    h->magic = be32toh(h->magic);
    h->rsvd  = be16toh(h->rsvd);
    h->seq   = be64toh(h->seq);
}
static inline void meta_hton(struct ft_meta *m)
{
    m->file_size    = htobe64(m->file_size);
    m->total_pkts   = htobe64(m->total_pkts);
    m->payload_size = htobe32(m->payload_size);
}
static inline void meta_ntoh(struct ft_meta *m)
{
    m->file_size    = be64toh(m->file_size);
    m->total_pkts   = be64toh(m->total_pkts);
    m->payload_size = be32toh(m->payload_size);
}

#define FT_HDRLEN   ((int)sizeof(struct ft_hdr))
#define MAX_DGRAM   65507

#endif
