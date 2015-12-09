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
#include <rte_eal.h>
#include <rte_memory.h>
#include <rte_memzone.h>
#include <rte_launch.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_debug.h>

#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>

extern void *init_log(void);
extern void handle_packet(void*, uint8_t*);

#define BURST_SIZE 10

const uint8_t port = 1;

static __attribute__((noreturn)) void
lcore_main(void) {
	if (rte_eth_dev_socket_id(port) > 0 &&
			rte_eth_dev_socket_id(port) != (int)rte_socket_id()) {
		printf("WARNING, port %u is on remote NUMA node %u to "
			"polling thread on %u.\n\tPerformance will "
			"not be optimal.\n", port, rte_eth_dev_socket_id(port), rte_socket_id());
	}
	void *log = init_log();
	printf("Starting server on log %p.\n", log);
	while(1) {
		const uint16_t queue_id = 0;
		struct rte_mbuf *bufs[BURST_SIZE];
		const uint16_t nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;

		//printf("nb_rx %d\n", nb_rx);
		uint16_t i;
		for(i = 0; i < nb_rx; i++) {
			//printf("packt %d\n", i);
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

		const uint16_t nb_tx = rte_eth_tx_burst(port, queue_id, bufs, nb_rx);

		if (unlikely(nb_tx < nb_rx)) {
			uint16_t buf;
			for (buf = nb_tx; buf < nb_rx; buf++) rte_pktmbuf_free(bufs[buf]);
		}
	}
}

static int
lcore_hello(__attribute__((unused)) void *arg)
{
	unsigned lcore_id;
	lcore_id = rte_lcore_id();
	printf("hello from core %u\n", lcore_id);
	lcore_main();
}

static const char *_DELOS_MBUF_POOL = "DELOS_MBUF_POOL";
//static const unsigned NUM_MBUFS = 0xffff;
static const unsigned NUM_MBUFS = 2000;
static const unsigned MBUFF_CACHE_SIZE = 64;
static const unsigned MBUF_CACHE_SIZE = 64;
//static const uint16_t DELOS_MBUF_DATA_SIZE = RTE_MBUF_DEFAULT_BUF_SIZE
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 - 64 * 4; // 3968
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 + sizeof(struct ether_hdr) +
//		sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *);
static const uint16_t DELOS_MBUF_DATA_SIZE = 8192;


int
main(int argc, char **argv)
{
	int ret;
	//unsigned lcore_id;
	struct rte_mempool *packet_pool;

	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_panic("Cannot init EAL\n");

	ret = rte_eth_dev_count();
	if (ret == 0)
			rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
	assert(ret == 2);


	packet_pool = rte_pktmbuf_pool_create(_DELOS_MBUF_POOL, NUM_MBUFS,
			MBUFF_CACHE_SIZE, 0, DELOS_MBUF_DATA_SIZE,
			SOCKET_ID_ANY);
			//rte_socket_id());
	if (packet_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get memory pool for buffers due to %s\n", rte_strerror(rte_errno));
	}

	{
		struct rte_eth_dev_info info;
		int retval;
		struct ether_addr addr;
		const uint16_t rx_rings = 1, tx_rings = 1;
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
								.rss_hf = ETH_RSS_IP,
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

		retval = rte_eth_rx_queue_setup(port, 0, 64,
						rte_eth_dev_socket_id(port),
						&info.default_rxconf,
						packet_pool);
		if (retval < 0)
				rte_exit(EXIT_FAILURE, "RX queue failed\n");

		retval = rte_eth_tx_queue_setup(port, 0, 64,
						rte_eth_dev_socket_id(port),
						NULL);
		if (retval < 0)
				rte_exit(EXIT_FAILURE, "TX queue failed\n");

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

	/* call lcore_hello() on every slave lcore */
	//RTE_LCORE_FOREACH_SLAVE(lcore_id) {
	//	rte_eal_remote_launch(lcore_hello, NULL, lcore_id);
	//}

	/* call it on master lcore too */
	lcore_hello(NULL);

	rte_eal_mp_wait_lcore();
	return 0;
}
