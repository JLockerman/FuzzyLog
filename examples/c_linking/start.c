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
#define DATA_KIND 2


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

struct clients { void *_phantom; };

extern void *init_log(void);
extern void handle_packet(void*, void*);
extern void handle_multiappend(uint32_t, uint32_t, void*, void*);
extern uint32_t rss_log(uint32_t, uint16_t, void *);
extern void *rss_log_init(void);

struct write_header *mbuf_into_write_packet(struct rte_mbuf *);
struct multi_header *mbuf_into_multi_packet(struct rte_mbuf *);
struct write_header *mbuf_into_read_packet(struct rte_mbuf *);
void prep_mbuf(struct rte_mbuf *mbuf, uint16_t data_size);


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

#define IP_DN_FRAGMENT_FLAG 0x0040
#define IP_DEFTTL 64

static int64_t sends[RTE_MAX_LCORE] __rte_cache_aligned;
static int64_t recvs[RTE_MAX_LCORE] __rte_cache_aligned;
static int64_t sent_bytes[RTE_MAX_LCORE] __rte_cache_aligned;
static int64_t recv_bytes[RTE_MAX_LCORE] __rte_cache_aligned;

#define NUM_SECS 5
static uint64_t SEC = 0;

static const uint64_t tx_ol_flags =  PKT_TX_IPV4 | PKT_TX_IP_CKSUM | PKT_TX_UDP_CKSUM;
#define HEADERS_SIZE (sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr))
#define DATA_SIZE 1000
//#define DATA_SIZE (sizeof(struct write_header) + sizeof(uint64_t))


static uint32_t core_id[RTE_MAX_LCORE];
struct rte_ring *recv_rings[RTE_MAX_LCORE];
static rte_atomic16_t start = {.cnt = 0};
static rte_atomic32_t cores_ready = {.cnt = 0};

static RTE_DEFINE_PER_LCORE(uint16_t, packet_id) = 0;

static uint32_t iteration;
static uint32_t num_client_cores;

rte_spinlock_t max_tx_lock;
uint32_t max_tx[8] = {0, 0, 0, 0, 0, 0, 0, 0};

inline static uint16_t
ipv4_packet_size(uint16_t data_size)
{
	return data_size + sizeof(struct ipv4_hdr)
			+ sizeof(struct udp_hdr);
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
prep_mbuf(struct rte_mbuf *mbuf, uint16_t data_size, uint16_t src_port)
{
	set_mbuf_metadata(mbuf, data_size);
	set_ether_header(mbuf);
	set_ipv4_header(mbuf, source_ip, dest_ip, data_size);
	set_udp_header(mbuf, src_port, 0, data_size); //TODO port
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
	return rte_pktmbuf_mtod_offset(mbuf, union delos_header,
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

inline static struct rte_mbuf *
try_rx_mbuf(struct rte_ring *recv_ring) { //TODO burst? (probably not, will need to do so much work it will be irrelevant)
	struct rte_mbuf *mbuf = NULL;
	int err = rte_ring_sc_dequeue(recv_ring, (void **)&mbuf);
	if(err != 0) return NULL;
	return mbuf;
}


int lcore_client(void *);
int lcore_recv(void *);

int
lcore_client(__attribute__((unused)) void *arg)
{
	//TODO setup
	assert(0 && "unimplemented");

	const uint32_t score_id = core_id[rte_lcore_id()];
	const struct rte_ring *recv_ring = recv_rings[score_id]; //TODO

	uint32_t num_clients = 0;
	if(iteration < num_client_cores) {
		if(score_id < iteration) return 0;
		num_clients = 1;
	}
	else {
		num_clients = iteration / num_client_cores;
		if(score_id < iteration % num_client_cores) num_clients += 1;
	}
	struct clients *clients = alloc_clients(num_clients);

	uint32_t num_waiting_clients = 0;
	unsigned to_tx = 0;

	rte_atomic32_inc(&cores_ready);
	while(!started()); //TODO

	uint64_t start_time = rte_rdtsc();
	while(not_done(start_time)) {
		{ //do sends
			struct rte_mbuf *bufs[BURST_SIZE];
			while(num_waiting_clients < num_clients && to_tx < burst_size) {
				bufs[to_tx] = get_packet_from_client(clients);
				to_tx += 1;
				num_waiting_clients += 1;
			}
			if(to_tx > 0) tx_burst(bufs, score_id, to_tx);
		}

		if(num_waiting_clients > 0) { //do receives
			struct rte_mbuf *mbuf = try_rx_mbuf();
			while(mbuf != NULL) {
				give_packet_to_client(clients, mbuf);
				num_waiting_clients -= 1;
				mbuf = try_rx_mbuf(recv_ring);
			}
		}
	}
	return 0;
}

int
lcore_recv(__attribute__((unused)) void *arg)
{
	assert(0 && "unimplemented");
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

	printf("sent %12"PRIi64" packets/s.\n", sent_packets / NUM_SECS);
	printf("recv %12"PRIi64" packets/s.\n", recv_packets / NUM_SECS);
	printf("sent %12"PRIi64" bytes/s.\n", total_sent_bytes / NUM_SECS);
	printf("recv %12"PRIi64" bytes/s.\n", total_recv_bytes / NUM_SECS);
}

#define TEST_TRANSACTION (!0)

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

	SEC = rte_get_tsc_hz();

	struct rte_mempool *packet_pool = rte_pktmbuf_pool_create(_DELOS_MBUF_POOL, NUM_MBUFS,
			MBUF_CACHE_SIZE, 0, DELOS_MBUF_DATA_SIZE,
			rte_eth_dev_socket_id(port));
			//SOCKET_ID_ANY);
			//rte_socket_id());
	if (packet_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get memory pool for buffers due to %s\n", rte_strerror(rte_errno));
	}

	printf("%d lcores\n", rte_lcore_count());
	if(rte_lcore_count() <= 2) rte_exit(EXIT_FAILURE, "Benchmark needs at least 3 lcores to run.\n");
	num_client_cores = rte_lcore_count() - 2; //1 master core and 1 recv core

	{
		struct rte_eth_dev_info info;
		int retval;
		const uint16_t rx_rings = 1, tx_rings = rte_lcore_count(); //TODO
		struct rte_eth_conf port_conf = {
						.rxmode = {
							.mq_mode	= ETH_MQ_RX_RSS,
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
	rte_spinlock_init(&max_tx_lock);
	rte_atomic16_init(&start);
	rte_atomic16_set(&start, 0);
	rte_atomic32_init(&cores_ready);
	rte_atomic32_set(&cores_ready, 1);
	{
		int recv_started = false;
		unsigned num_lcores = rte_lcore_count();
		unsigned i = 0;
		unsigned ether_socket = rte_eth_dev_socket_id(port);
		unsigned lcore_id = 0;
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			core_id[lcore_id] = i;
			if(!recv_started &&
					(rte_lcore_to_socket_id(lcore_id) == ether_socket ||
							i == num_lcores - 1)) {
				rte_eal_remote_launch(lcore_recv, NULL, lcore_id);
				recv_started = true;
			}
			else rte_eal_remote_launch(lcore_client, NULL, lcore_id);
			i++;
		}
		while(rte_atomic32_read(&cores_ready) < num_lcores);
	}
#define MAX_ITERS 25 //TODO
	for(iteration = 0; iteration < MAX_ITERS; iteration++) {
		//TODO set metadata
		rte_atomic16_set(&start, !0);
		rte_eal_mp_wait_lcore();
		rte_atomic16_set(&start, 0);
		print_packets(); //TODO
	}

	return 0;
}
