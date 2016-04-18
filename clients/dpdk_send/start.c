/*-
 *   BSD LICENSE
 *
 *   Copyright(c) ????. All rights reserved.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <sched.h>
#include <sys/queue.h>

#include <rte_atomic.h>
#include <rte_common.h>
#include <rte_cpuflags.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_memory.h>
#include <rte_memzone.h>
#include <rte_launch.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_per_lcore.h>
#include <rte_ring.h>
#include <rte_lcore.h>
#include <rte_debug.h>

#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <rte_spinlock.h>

#define DELOS_BENCHMARK 1

#include <math.h>

#define MULTIAPPEND_KIND 2
#define DATA_KIND 1


struct OrderIndex {
	uint32_t order;
	uint32_t index;
};

struct write_header {
	uint64_t id[2];
	uint8_t kind;
	uint8_t padding[1];
	uint16_t data_bytes;
	uint16_t dep_bytes;
	uint32_t loc[2];
};

struct multi_header {
	uint64_t id[2];
	uint8_t kind;
	uint8_t padding[1];
	uint16_t data_bytes;
	uint16_t dep_bytes;
	uint16_t cols;
};

typedef union delos_header {
	struct write_header write;
	struct multi_header multi;
} delos_header;

_Static_assert(sizeof(union delos_header) == sizeof(struct write_header), "bad header size");

//struct clients { void *_phantom; };

struct clients;

struct clients *alloc_clients(uint16_t num_clients);
uint16_t get_packets_to_send(struct clients *clients, struct rte_mbuf** bufs, uint16_t len);
void handle_received_packets(struct clients *clients, uint64_t recv_time, struct rte_mbuf* const* bufs, uint16_t len);
void clients_finished(struct clients *clients);

void rust_exit(void);
struct rte_mbuf* alloc_mbuf(void);
void dpdk_free_mbuf(struct rte_mbuf *mbuf);
void copy_mbuf(struct rte_mbuf *restrict dst, const struct rte_mbuf *restrict src);
struct write_header *mbuf_into_write_packet(struct rte_mbuf *);
struct multi_header *mbuf_into_multi_packet(struct rte_mbuf *);
struct write_header *mbuf_into_read_packet(struct rte_mbuf *);
union delos_header *mbuf_as_packet(struct rte_mbuf *mbuf);
void prep_mbuf(struct rte_mbuf *mbuf, uint16_t data_size, uint16_t src_port);
void mbuf_set_src_port(struct rte_mbuf *mbuf, uint16_t port);
uint16_t *mbuf_get_src_port_ptr(struct rte_mbuf *mbuf);


#define BURST_SIZE 8
#define NUM_SAMPLES 1000

const uint8_t port = 1; //TODO
static const char *_DELOS_MBUF_POOL = "DELOS_MBUF_POOL";
//static const unsigned NUM_MBUFS = 2047;
static const unsigned NUM_MBUFS = 0xffff;
static const unsigned MBUF_CACHE_SIZE = 16;
static const uint16_t DELOS_MBUF_DATA_SIZE = 8192;

//TODO take option
//e4:1d:2d:0c:86:40
static const struct ether_addr dest_addr = {.addr_bytes = {0xe4, 0x1d, 0x2d, 0x0c, 0x86, 0x00}};
static const struct ether_addr source_addr = {.addr_bytes = {0xe4, 0x1d, 0x2d, 0x0c, 0x86, 0x40}};

//TODO take option
static const uint32_t dest_ip = IPv4(10, 21, 7, 4);
static const uint32_t source_ip = IPv4(10, 21, 7, 5);
static RTE_DEFINE_PER_LCORE(uint32_t, source_ip) __rte_cache_aligned;

#define IP_DN_FRAGMENT_FLAG 0x0040
#define IP_DEFTTL 64

static int64_t sends[RTE_MAX_LCORE] __rte_cache_aligned;
static int64_t recvs[RTE_MAX_LCORE] __rte_cache_aligned;
static int64_t sent_bytes[RTE_MAX_LCORE] __rte_cache_aligned;
static int64_t recv_bytes[RTE_MAX_LCORE] __rte_cache_aligned;
struct rte_mempool *packet_pool;

static const uint64_t tx_ol_flags =  PKT_TX_IPV4 | PKT_TX_IP_CKSUM | PKT_TX_UDP_CKSUM;
#define HEADERS_SIZE (sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr))
#define DATA_SIZE 1000
//#define DATA_SIZE (sizeof(struct write_header) + sizeof(uint64_t))

static uint32_t core_id[RTE_MAX_LCORE];
struct rte_ring *recv_rings[RTE_MAX_LCORE];
struct rte_ring *send_rings[RTE_MAX_LCORE];
//static rte_atomic16_t start = {.cnt = 0};
static uint64_t tsc_hz = 0;
static uint64_t start_time;
static rte_atomic32_t cores_ready = {.cnt = 0};

static RTE_DEFINE_PER_LCORE(uint16_t, packet_id) = 0;

static uint32_t iteration;
static uint32_t num_client_cores;

rte_spinlock_t max_tx_lock;
uint32_t max_tx[8] = {0, 0, 0, 0, 0, 0, 0, 0};

#if defined(DELOS_DEBUG_ALL)
#define debug_misc(fmt, ...) printf(fmt, score_id, ## __VA_ARGS__)
#else
#define debug_misc(...) do { } while (0)
#endif

void
rust_exit(void)
{
	rte_exit(EXIT_FAILURE, "rust panicked.\n");
}

struct rte_mbuf*
alloc_mbuf(void)
{
	return rte_pktmbuf_alloc(packet_pool);
}

void
dpdk_free_mbuf(struct rte_mbuf *mbuf)
{
	rte_pktmbuf_free(mbuf);
}

inline static uint16_t
ipv4_packet_size(uint16_t data_size)
{
	return data_size + sizeof(struct ipv4_hdr)
			+ sizeof(struct udp_hdr);
}

inline static uint16_t
delos_entry_size(uint16_t total_packet_size)
{
	return total_packet_size - (sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr));
}

void
copy_mbuf(struct rte_mbuf *restrict dst, const struct rte_mbuf *restrict src)
{
	debug_misc("copying %p to %p.\n", src, dst);
	dst->data_off = src->data_off;
	dst->nb_segs = src->nb_segs;
	dst->port = src->port;
	assert(src->next == NULL);
	dst->next = src->next;
	dst->data_len = src->data_len;
	assert(src->data_len > 0);
	assert(delos_entry_size(src->data_len) > 0);
	debug_misc("copying %u bytes.\n", src->data_len);
	dst->pkt_len = src->pkt_len;
	dst->ol_flags = src->ol_flags;
	dst->l2_len = src->l2_len;
	dst->l3_len = src->l3_len;
	dst->l4_len = src->l4_len;
	dst->packet_type = src->packet_type;
	rte_memcpy(rte_pktmbuf_mtod(dst, char *), rte_pktmbuf_mtod(src, const char *), src->data_len);
}

inline static uint16_t
total_packet_size(uint16_t data_size)
{
	return ipv4_packet_size(data_size) + sizeof(struct ether_hdr);
}

inline static void
set_mbuf_metadata(struct rte_mbuf *mbuf, uint16_t data_size)
{
	mbuf->data_off = RTE_PKTMBUF_HEADROOM;
	mbuf->nb_segs = 1;
	mbuf->port = port;
	mbuf->next = NULL;
	mbuf->data_len = total_packet_size(data_size);
	mbuf->pkt_len = total_packet_size(data_size);
	mbuf->ol_flags |= tx_ol_flags;
	mbuf->l2_len = sizeof(struct ether_hdr);
	mbuf->l3_len = sizeof(struct ipv4_hdr);
	mbuf->l4_len = sizeof(struct udp_hdr);
	mbuf->packet_type = RTE_PTYPE_L2_ETHER | RTE_PTYPE_L3_IPV4 | RTE_PTYPE_L4_UDP;
}

inline static void
set_ether_header(struct rte_mbuf *mbuf)
{
	struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
	eth->d_addr = dest_addr;
	eth->s_addr = source_addr;
	eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);
}

inline static void //note ip addr in cpu order
set_ipv4_header(struct rte_mbuf *mbuf, uint32_t src_addr, uint32_t dst_addr, uint16_t data_size)
{
	struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(mbuf, struct ipv4_hdr *,
			sizeof(struct ether_hdr));
	ip->version_ihl = 0x45;
	ip->type_of_service = 0;
	ip->total_length = rte_cpu_to_be_16(ipv4_packet_size(data_size));
	ip->packet_id = rte_cpu_to_be_16(RTE_PER_LCORE(packet_id)++); //TODO
	ip->fragment_offset = IP_DN_FRAGMENT_FLAG;
	ip->time_to_live = IP_DEFTTL;
	assert(IPPROTO_UDP == 0x11);
	ip->next_proto_id = IPPROTO_UDP;
	ip->hdr_checksum = 0;
	ip->src_addr = rte_cpu_to_be_32(src_addr);
	ip->dst_addr = rte_cpu_to_be_32(dst_addr);
}

inline static void
set_udp_header(struct rte_mbuf *mbuf, uint16_t src_port, uint16_t dst_port, uint16_t data_size)
{
	struct udp_hdr *udp = rte_pktmbuf_mtod_offset(mbuf, struct udp_hdr *,
						sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr));
	udp->dst_port = rte_cpu_to_be_16(dst_port);
	udp->src_port = rte_cpu_to_be_16(src_port);
	udp->dgram_cksum = 0;
	udp->dgram_len = rte_cpu_to_be_16(data_size);
}

void
mbuf_set_src_port(struct rte_mbuf *mbuf, uint16_t port)
{
	rte_pktmbuf_mtod_offset(mbuf, struct udp_hdr *,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr))->src_port = port;
}

uint16_t *
mbuf_get_src_port_ptr(struct rte_mbuf *mbuf)
{
	return &rte_pktmbuf_mtod_offset(mbuf, struct udp_hdr *,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr))->dst_port;
}

void
prep_mbuf(struct rte_mbuf *mbuf, uint16_t data_size, uint16_t src_port)
{
	set_mbuf_metadata(mbuf, data_size);
	set_ether_header(mbuf);
	set_ipv4_header(mbuf, RTE_PER_LCORE(source_ip), dest_ip, data_size);
	set_udp_header(mbuf, src_port, 13265, data_size); //TODO port
}

struct write_header *
mbuf_into_write_packet(struct rte_mbuf *mbuf)
{
	struct write_header *wheader = rte_pktmbuf_mtod_offset(mbuf, struct write_header*,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
	wheader->kind = DATA_KIND;
	return wheader;
}

struct multi_header *
mbuf_into_multi_packet(struct rte_mbuf *mbuf)
{
	struct multi_header *wheader = rte_pktmbuf_mtod_offset(mbuf, struct multi_header*,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
	wheader->kind = MULTIAPPEND_KIND;
	return wheader;
}

union delos_header *
mbuf_as_packet(struct rte_mbuf *mbuf) {
	return rte_pktmbuf_mtod_offset(mbuf, union delos_header *,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
}

struct write_header *
mbuf_into_read_packet(struct rte_mbuf *mbuf)
{
	struct write_header *wheader = rte_pktmbuf_mtod_offset(mbuf, struct write_header*,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
	memset(wheader, 0, sizeof(*wheader));
	wheader->kind = DATA_KIND | MULTIAPPEND_KIND;
	return wheader;
}

inline static size_t
total_header_size(void)
{
	return sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) +
			sizeof(struct udp_hdr) + sizeof(struct write_header);
}

inline static size_t
max_data_size(void)
{
	return DELOS_MBUF_DATA_SIZE - total_header_size();
}

inline static void
tx_burst(struct rte_mbuf **bufs, uint16_t score_id, uint16_t to_tx)
{
	uint16_t nb_tx = rte_eth_tx_burst(port/* TODO */, score_id /* TODO */, bufs, to_tx);
	while (unlikely(nb_tx < to_tx)) {
		nb_tx += rte_eth_tx_burst(port, score_id, &bufs[nb_tx], to_tx - nb_tx);
	}
}

inline static void
send_to(struct rte_ring *ring, struct rte_mbuf **bufs, uint16_t to_tx)
{
	assert(ring);
	assert(bufs);
	uint16_t nb_tx = rte_ring_sp_enqueue_burst(ring, (void* const*)bufs, to_tx);
	while (unlikely(nb_tx < to_tx)) {
		nb_tx += rte_ring_sp_enqueue_burst(ring, (void* const*)&bufs[nb_tx], to_tx);
	}
}

inline static uint16_t
recv_from(struct rte_ring *ring, struct rte_mbuf **bufs, uint16_t burst_size)
{
	assert(ring);
	assert(bufs);
	return rte_ring_sc_dequeue_burst(ring, (void**)bufs, burst_size);
}

inline static struct rte_mbuf *
try_rx_mbuf(struct rte_ring *recv_ring) { //TODO burst? (probably not, will need to do so much work it will be irrelevant)
	struct rte_mbuf *mbuf = NULL;
	int err = rte_ring_sc_dequeue(recv_ring, (void **)&mbuf);
	if(err != 0) return NULL;
	return mbuf;
}


int lcore_client(void *);
int lcore_recv(void *);
int lcore_send(void *);

#define ROUND_SECONDS 5 //TODO

static inline int
round_finished(uint64_t start_time)
{
	return start_time - rte_rdtsc() < ROUND_SECONDS * tsc_hz;
}

int
lcore_client(__attribute__((unused)) void *arg)
{
	const uint32_t score_id = core_id[rte_lcore_id()];
	struct rte_ring *const restrict recv_ring = recv_rings[score_id]; //TODO
	struct rte_ring *const restrict send_ring = send_rings[score_id];
	assert(recv_ring);
	assert(send_ring);
	assert(score_id < 255 && "score_id to large to use as part of ip addr");
	RTE_PER_LCORE(source_ip) = IPv4(10, 21, 7, (uint8_t)score_id);
	uint32_t num_clients = 0;
	if(iteration < num_client_cores) {
		if(iteration <= score_id) {
			printf("no clients on %2u:%2u.\n", rte_lcore_id(), score_id);
			rte_atomic32_inc(&cores_ready);
			return 0;
		}
		num_clients = 1;
	}
	else {
		num_clients = iteration / num_client_cores;
		if(score_id < iteration % num_client_cores) num_clients += 1;
	}

	printf("starting %u clients on %2u:%2u.\n", num_clients, rte_lcore_id(), score_id);

	struct clients *clients = alloc_clients(num_clients);

	rte_atomic32_inc(&cores_ready);
	while(*(volatile uint64_t *)&start_time == 0); //while not started

	while(!round_finished(start_time)) {
		struct rte_mbuf *bufs[BURST_SIZE];

		uint16_t to_tx = get_packets_to_send(clients, bufs, BURST_SIZE);
		if(to_tx > 0) send_to(send_ring, bufs, to_tx);

		uint16_t nb_rx = recv_from(recv_ring, bufs, BURST_SIZE);
		uint64_t recv_time = rte_rdtsc();
		if(nb_rx > 0) handle_received_packets(clients, recv_time, bufs, nb_rx);
	}
	clients_finished(clients);
	return 0;
}

int
lcore_recv(__attribute__((unused)) void *arg)
{
	printf("starting recv on %u.\n", rte_lcore_id());
	rte_atomic32_inc(&cores_ready);
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE];
		uint16_t nb_rx = 0;
		nb_rx = rte_eth_rx_burst(port, 0, bufs, BURST_SIZE);
		if(nb_rx > 0) printf("received %u packets.\n", nb_rx);
		for(uint16_t i = 0; i < nb_rx; i++) {
			struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(bufs[i], struct ipv4_hdr *,
					sizeof(struct ether_hdr));
			uint8_t dst = rte_be_to_cpu_32(ip->dst_addr) & 0xff;
			printf("routing packet to %u.\n", dst);

			if(dst < num_client_cores) send_to(recv_rings[dst], &bufs[i], 1); //assert(dst < num_client_cores);
			else rte_pktmbuf_free(bufs[i]);
		}
	}
	return 0;
}

int
lcore_send(__attribute__((unused)) void *arg)
{
	printf("starting send on %u.\n", rte_lcore_id());
	rte_atomic32_inc(&cores_ready);
	while(1) {
		for(unsigned i = 0; i < num_client_cores; i++) {
			struct rte_mbuf *bufs[BURST_SIZE];

			uint16_t nb_rx = recv_from(send_rings[i], bufs, BURST_SIZE);
			if(nb_rx > 0) printf("sending %u packets.\n", nb_rx);
			uint16_t nb_tx = rte_eth_tx_burst(port, 0, bufs, nb_rx);
			while (unlikely(nb_tx < nb_rx)) {
				nb_tx += rte_eth_tx_burst(port, 0, &bufs[nb_tx], nb_rx - nb_tx);
			}
		}
	}
	return 0;
}

static void
print_packets(void)
{
	uint64_t sent_packets = 0;
	uint64_t recv_packets = 0;
	uint64_t total_sent_bytes = 0, total_recv_bytes = 0;
	for(int i = 0; i < RTE_MAX_LCORE; i++) {
		sent_packets += sends[i];
		total_sent_bytes += sent_bytes[i];
		recv_packets += recvs[i];
		total_recv_bytes += recv_bytes[i];
		sends[i] = 0;
		recvs[i] = 0;
		sent_bytes[i] = 0;
		recv_bytes[i] = 0;
	}
	printf("sent %12"PRIi64" packets.\n", sent_packets);
	printf("recv %12"PRIi64" packets.\n", recv_packets);

	printf("sent %12"PRIi64" bytes.\n", total_sent_bytes);
	printf("recv %12"PRIi64" bytes.\n", total_recv_bytes);

	printf("sent %12"PRIi64" packets/s.\n", sent_packets / ROUND_SECONDS);
	printf("recv %12"PRIi64" packets/s.\n", recv_packets / ROUND_SECONDS);
	printf("sent %12"PRIi64" bytes/s.\n", total_sent_bytes / ROUND_SECONDS);
	printf("recv %12"PRIi64" bytes/s.\n", total_recv_bytes / ROUND_SECONDS);
}

#define TEST_TRANSACTION (!0)

static inline void
start_other_cores(void)
{
	*(volatile uint64_t *) &start_time = 0;
	rte_smp_wmb();
	rte_atomic32_init(&cores_ready);
	rte_atomic32_set(&cores_ready, 1);
	{
		int recv_started = false;
		int send_started = false;
		long long num_lcores = rte_lcore_count();
		unsigned i = 0;
		unsigned ether_socket = rte_eth_dev_socket_id(port);
		unsigned lcore_id = 0;
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			if(!recv_started &&
					(rte_lcore_to_socket_id(lcore_id) == ether_socket ||
							i == num_lcores - 1)) {
				rte_eal_remote_launch(lcore_recv, NULL, lcore_id);
				recv_started = true;
			}
			else if(!send_started &&
					(rte_lcore_to_socket_id(lcore_id) == ether_socket ||
							i == num_lcores - 2)) {
				rte_eal_remote_launch(lcore_send, NULL, lcore_id);
				send_started = true;
			}
			else {
				assert(i < num_client_cores);
				assert(send_rings[i] != NULL);
				assert(recv_rings[i] != NULL);
				core_id[lcore_id] = i;
				i += 1;
				rte_eal_remote_launch(lcore_client, NULL, lcore_id);
			}
		}
		assert(num_client_cores == i);
		while(rte_atomic32_read(&cores_ready) < num_lcores);
		printf("All cores ready.\n");
    }
}

int
main(int argc, char **argv)
{
	int ret;

	//rte_set_log_level (DEBUG);
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_panic("Cannot init EAL\n");

	ret = rte_eth_dev_count();
	if (ret == 0) rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
	assert(ret == 2);

	packet_pool = rte_pktmbuf_pool_create(_DELOS_MBUF_POOL, NUM_MBUFS,
			MBUF_CACHE_SIZE, 0, DELOS_MBUF_DATA_SIZE,
			rte_eth_dev_socket_id(port));
			//SOCKET_ID_ANY);
			//rte_socket_id());
	if (packet_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get memory pool for buffers due to %s\n", rte_strerror(rte_errno));
	}

	printf("%d lcores\n", rte_lcore_count());
	if(rte_lcore_count() <= 4) rte_exit(EXIT_FAILURE, "Benchmark needs at least 4 lcores to run.\n");
	num_client_cores = rte_lcore_count() - 3; //1 master core 1 recv core, and 1 send core

	for(unsigned i = 0; i < num_client_cores; i++) {
		char ring_name[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0};

		snprintf(ring_name, 16, "send_ring:%u", i);
		send_rings[i] = rte_ring_create(ring_name, 128, rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ);
		assert(send_rings[i] != NULL);

		snprintf(ring_name, 16, "recv_ring:%u", i);
		recv_rings[i] = rte_ring_create(ring_name, 128, rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ);
		assert(recv_rings[i] != NULL);
	}

	{
		struct rte_eth_dev_info info;
		int retval;
		const uint16_t rx_rings = 1, tx_rings = 1; //TODO
		struct rte_eth_conf port_conf = {
						.rxmode = {
							//.mq_mode	= ETH_MQ_RX_RSS,
							.split_hdr_size = 0,
							.header_split   = 0, /**< Header Split disabled */
							.hw_ip_checksum = 1, /**< IP checksum offload enabled */
							.hw_vlan_filter = 0, /**< VLAN filtering disabled */
							.jumbo_frame    = 1, /**< Jumbo Frame Support enabled */
							.hw_strip_crc   = 1, /**< CRC stripped by hardware */
							.max_rx_pkt_len = ETHER_MAX_LEN,
						},
						//.rx_adv_conf = {
						//	.rss_conf = {
						//		.rss_key = NULL,
						//		.rss_hf = ETH_RSS_UDP,
						//	},
						//},
						.txmode = {
							.mq_mode = ETH_MQ_TX_NONE,
						}
				};
		rte_eth_dev_info_get(0, &info);

		retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
		if (retval < 0) rte_exit(EXIT_FAILURE, "Config failed\n");


		for(int i = 0; i < rx_rings; i++) {
			retval = rte_eth_rx_queue_setup(port, i, 64,
				rte_eth_dev_socket_id(port),
				&info.default_rxconf, //TODO
				packet_pool);
			if (retval < 0) rte_exit(EXIT_FAILURE, "RX queue failed\n");
		}

		for(int i = 0; i < tx_rings; i++) {
			retval = rte_eth_tx_queue_setup(port, i, 64,
					rte_eth_dev_socket_id(port),
					NULL);
			if (retval < 0) rte_exit(EXIT_FAILURE, "TX queue failed\n");
		}

		retval = rte_eth_dev_start(port);

		struct ether_addr addr;
		rte_eth_macaddr_get(port, &addr);
		printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
				" %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
				(unsigned)port,
				addr.addr_bytes[0], addr.addr_bytes[1],
				addr.addr_bytes[2], addr.addr_bytes[3],
				addr.addr_bytes[4], addr.addr_bytes[5]);
	}
	tsc_hz = rte_get_tsc_hz();
	rte_spinlock_init(&max_tx_lock);
	//rte_atomic16_init(&start);
	//rte_atomic16_set(&start, 0);
#define MAX_ITERS 1 //TODO
	printf("starting %u client cores.\n", num_client_cores);
	for(iteration = 1; iteration <= MAX_ITERS; iteration++) {
		//TODO set metadata
		//rte_atomic16_set(&start, !0);
		start_other_cores();
		rte_smp_wmb();
		*(volatile uint64_t *) &start_time = rte_rdtsc();
		rte_smp_wmb();
		rte_eal_mp_wait_lcore();
		//rte_atomic16_set(&start, 0);
		*(volatile uint64_t *) &start_time = 0;
		rte_smp_wmb();
		print_packets(); //TODO
	}

	return 0;
}
