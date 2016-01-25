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
#include <errno.h>
#include <sys/queue.h>

#include <rte_common.h>
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

//#define DELOS_BENCHMARK 1

#ifdef DELOS_BENCHMARK
#include <math.h>
#endif

struct delos_header {
	uint32_t kind;
    uint8_t id[16];
	uint32_t chain;
    uint32_t entry;
};

extern void *init_log(void);
extern void handle_packet(void*, uint8_t*);
extern uint32_t rss_log(uint32_t, uint16_t, void *);
extern void *rss_log_init(void);

#define BURST_SIZE 10
#define NUM_SAMPLES 1000

static uint16_t core_id[RTE_MAX_LCORE];
static struct rte_ring *distributor_rings[RTE_MAX_LCORE];

const uint8_t port = 1; //TODO
static const char *_DELOS_MBUF_POOL = "DELOS_MBUF_POOL";
static const unsigned NUM_MBUFS = 2047;
static const unsigned MBUF_CACHE_SIZE = 16;
//static const uint16_t DELOS_MBUF_DATA_SIZE = RTE_MBUF_DEFAULT_BUF_SIZE
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 - 64 * 4; // 3968
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 + sizeof(struct ether_hdr) +
//		sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *);
static const uint16_t DELOS_MBUF_DATA_SIZE = 8192;

static struct rte_mempool *audit_pool;

#ifdef DELOS_BENCHMARK
static void
print_stats(const unsigned lcore, const uint64_t *durations)
{
	uint64_t mean = 0;
	for(int i = 0; i < NUM_SAMPLES; i++) {
		mean += durations[i];
	}
	mean /= NUM_SAMPLES;
	uint64_t var = 0;
	for(int i = 0; i < NUM_SAMPLES; i++) {
		uint64_t dif = (mean - durations[i]);
		var += dif * dif;
	}
	var /= NUM_SAMPLES;
	//uint64_t hz = rte_get_tsc_hz();
	uint64_t stddev = (uint64_t) sqrt(var);
	//printf("lcore %u: mean time %"PRIu64", σ %"PRIu64", %"PRIu64"hz\n", lcore, mean, stddev, hz);
	//TODO get hz from rte_get_tsc_hz
	printf("lcore %u: mean time %"PRIu64"ns, σ %"PRIu64"ns\n", lcore, (uint64_t)(mean / 2.5), (uint64_t)(stddev / 2.5));
}
#endif

//static int
//handle_multiappend(struct rte_mbuf *mbuf, void *log, const unsigned lcore_id) {
//	struct delos_header *header = rte_pktmbuf_mtod_offset(mbuf, struct delos_header*,
//			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)
//			+ sizeof(struct udp_hdr));
//}

static __attribute__((noreturn)) int
lcore_chain(__attribute__((unused)) void *arg) {
#ifdef DELOS_BENCHMARK
	uint64_t dist_dur[NUM_SAMPLES];
	int iters = 0;
#endif
	if (rte_eth_dev_socket_id(port) > 0 &&
			rte_eth_dev_socket_id(port) != (int)rte_socket_id()) {
		printf("WARNING, port %u is on remote NUMA node %u to "
			"polling thread on %u.\n\tPerformance will "
			"not be optimal.\n", port, rte_eth_dev_socket_id(port), rte_socket_id());
	}

	void *log = init_log();
	const unsigned lcore_id = rte_lcore_id();
	printf("Starting chain-server on core %u log %p.\n", lcore_id, log);

	const uint32_t score_id = core_id[lcore_id];
	struct rte_ring *ring = distributor_rings[score_id];
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE];
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		const unsigned nb_rx = rte_ring_sc_dequeue_burst(ring, (void **)bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;

		for(unsigned i = 0; i < nb_rx; i++) {
			struct rte_mbuf *mbuf = bufs[i];
			{
				struct ether_addr ether_temp;
				struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
				ether_addr_copy(&eth->d_addr, &ether_temp);
				ether_addr_copy(&eth->s_addr, &eth->d_addr);
				ether_addr_copy(&ether_temp, &eth->s_addr);
			}
			{
				struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(mbuf, struct ipv4_hdr *,
						sizeof(struct ether_hdr));
				uint32_t ip_temp = ip->dst_addr;
				ip->dst_addr = ip->src_addr;
				ip->src_addr = ip_temp;
				//TODO checksum?
			}
			{
				struct udp_hdr *udp = rte_pktmbuf_mtod_offset(mbuf, struct udp_hdr *,
						sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)); //TODO IP IHL?
				uint16_t udp_temp = udp->dst_port;
				udp->dst_port = udp->src_port;
				udp->src_port = udp_temp;
				mbuf->ol_flags |= PKT_TX_UDP_CKSUM;
			}
			uint8_t *data = rte_pktmbuf_mtod_offset(mbuf, uint8_t *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *));
			handle_packet(log , data);
		}

		unsigned to_tx = nb_rx;
		const uint16_t nb_tx = rte_eth_tx_burst(port, score_id, bufs, to_tx);
		if (unlikely(nb_tx < nb_rx)) {
			for (unsigned buf = nb_tx; buf < nb_rx; buf++) rte_pktmbuf_free(bufs[buf]);
		}
#ifdef DELOS_BENCHMARK
		dist_dur[iters] = rte_rdtsc() - start_tsc;
		iters += 1;
		if(unlikely(iters > NUM_SAMPLES)) {
			iters = 0;
			print_stats(lcore_id, dist_dur);
		}
#endif
	}
}

static void
distribute(const uint32_t ring_mask, uint32_t num_slave_cores) {
#ifdef DELOS_BENCHMARK
	uint64_t dist_dur[NUM_SAMPLES];
	int iters = 0;
#endif
	const unsigned lcore = rte_lcore_id();
	const uint16_t rx_queue_id = 0;
	printf("Starting distributor on lcore %u.\n", lcore);
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE];

#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		const uint16_t nb_rx = rte_eth_rx_burst(port, rx_queue_id, bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;

		for(int i = 0; i < nb_rx; i++) {
			struct rte_mbuf *mbuf = bufs[i];
			//TODO prefetch?
			struct delos_header *header = rte_pktmbuf_mtod_offset(mbuf, struct delos_header*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)
					+ sizeof(struct udp_hdr));
			//printf("header\n\tchain %u\n\tentry %u\n\t kind %u\n", header->chain, header->entry, header->kind);
			if(unlikely(header->kind) == 3) { //TODO header format...
				//TODO transactions
				//printf("dst_ring %u: %p\n", dst, dst_ring);
				printf("ERROR, trasaction.\n");
				rte_pktmbuf_refcnt_update(mbuf, num_slave_cores);
				for(unsigned j = 0; j < num_slave_cores; j++) {
					struct rte_ring* dst_ring = distributor_rings[j];
					rte_ring_sp_enqueue(dst_ring, mbuf);
				}
				//TODO ack immediately?
			}
			else {
				uint32_t dst = header->chain & ring_mask;
				//if(rss_log(dst, header->chain, seen_set) != 0) rte_exit(EXIT_FAILURE, "chain dupe\n");
				struct rte_ring* dst_ring = distributor_rings[dst];
				rte_ring_sp_enqueue(dst_ring, mbuf);
			}

#ifdef DELOS_BENCHMARK
			dist_dur[iters] = rte_rdtsc() - start_tsc;
			iters += 1;
			if(unlikely(iters > NUM_SAMPLES)) {
				iters = 0;
				print_stats(lcore, dist_dur);
			}
#endif
		}

	}
}

static uint32_t
next_power_of_2(uint32_t val)
{ //TODO
	val--;
	val |= val >> 1;
	val |= val >> 2;
	val |= val >> 4;
	val |= val >> 8;
	val |= val >> 16;
	val++;
	return val;
}

int
main(int argc, char **argv)
{
	int ret;
	unsigned lcore_id;
	struct rte_mempool *packet_pool;
	uint32_t num_slave_cores = 0;
	uint32_t num_rings = 0;
	uint32_t ring_mask = 0;

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
	audit_pool = packet_pool;
	{
		char ring_name[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			core_id[lcore_id] = num_slave_cores;
			snprintf(ring_name, 10, "d%u", num_slave_cores);
			distributor_rings[num_slave_cores] = rte_ring_create(ring_name, 16, rte_socket_id(),
					RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO size
			if(distributor_rings[num_slave_cores] == NULL) {
				rte_exit(EXIT_FAILURE, "Cannot init distributor ring %d memory "
						"pool due to:\n\t%s\n", num_slave_cores, rte_strerror(rte_errno));
			}
			num_slave_cores += 1;
		}
	}
	assert(num_slave_cores == rte_lcore_count() - 1);
	num_rings = next_power_of_2(num_slave_cores);
	ring_mask = num_rings - 1;
	printf("num scores %d\n", num_slave_cores);
	printf("num rings  %d\n", num_rings);
	printf("ring mask  0x%x\n", ring_mask);

	for(uint32_t i = num_slave_cores; i < num_rings; i++) {
		distributor_rings[i] = distributor_rings[i % num_slave_cores];
	}
	{
		struct rte_eth_dev_info info;
		int retval;
		struct ether_addr addr;
		const uint16_t rx_rings = 1, tx_rings = num_slave_cores;
		struct rte_eth_conf port_conf = {
						.rxmode = {
							.mq_mode	= ETH_MQ_RX_RSS,
							.split_hdr_size = 0,
							.header_split   = 0, /**< Header Split disabled */
							.hw_ip_checksum = 1, /**< IP checksum offload enabled */
							.hw_vlan_filter = 0, /**< VLAN filtering disabled */
							.jumbo_frame    = 1, /**< Jumbo Frame Support enabled */
							.hw_strip_crc   = 0, /**< CRC stripped by hardware */
							.max_rx_pkt_len = ETHER_MAX_LEN
						},
						.rx_adv_conf = {
							.rss_conf = {
								.rss_key = NULL,
								.rss_hf = ETH_RSS_UDP,
							},
						},
						.txmode = {
							.mq_mode = ETH_MQ_TX_NONE,
						}
				};
		rte_eth_dev_info_get(0, &info);

		retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
		if (retval < 0)
			rte_exit(EXIT_FAILURE, "Config failed\n");


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

		//rte_eth_macaddr_get

		retval = rte_eth_dev_start(port);

		rte_eth_macaddr_get(port, &addr);
		printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
				" %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
				(unsigned)port,
				addr.addr_bytes[0], addr.addr_bytes[1],
				addr.addr_bytes[2], addr.addr_bytes[3],
				addr.addr_bytes[4], addr.addr_bytes[5]);
	}


	//rte_mempool_list_dump(stdout);
	//printf("free elems %d\n", rte_mempool_free_count(packet_pool));
	//printf("mempool full %d\n", rte_mempool_full(packet_pool));
	//printf("mempool empty %d\n", rte_mempool_empty(packet_pool));

	//int i;
	//for(i = 0; i < 128; i++) {
	//	void *obj = NULL;
	//	rte_mempool_get(packet_pool, &obj);
	//	printf("%d: %p -> %p\n", i, obj, (void *)rte_mempool_virt2phy(packet_pool, obj));
	//}

	//rte_mempool_list_dump(stdout);
	//printf("free elems %d\n", rte_mempool_free_count(packet_pool));
	//printf("mempool full %d\n", rte_mempool_full(packet_pool));
	//printf("mempool empty %d\n", rte_mempool_empty(packet_pool));

	//seen_set = rss_log_init();
	/* call lcore_hello() on every slave lcore */
	RTE_LCORE_FOREACH_SLAVE(lcore_id) {
		rte_eal_remote_launch(lcore_chain, NULL, lcore_id);
	}
	//TODO delay...
	distribute(ring_mask, num_slave_cores);

	rte_eal_mp_wait_lcore();
	return 0;
}
