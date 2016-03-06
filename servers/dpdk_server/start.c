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
#include <sched.h>
#include <sys/queue.h>

#include <rte_alarm.h>
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

/*#[repr(C)]
pub struct Entry<V, F: ?Sized = [u8; MAX_DATA_LEN2]> {
    _pd: PhantomData<V>,
    pub id: Uuid,
    pub kind: EntryKind::Kind,
    pub _padding: [u8; 1],
    pub data_bytes: u16,
    pub dependency_bytes: u16,
    pub flex: F,
}*/

#define MAX_MULTIAPPEND_CHAINS 509
#define MULTIAPPEND_KIND 2


struct OrderIndex {
	uint32_t order;
	uint32_t index;
};

/*union delos_flex {
	uint32_t loc[2];
	uint16_t cols[2];
};
*/

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

extern void *init_log(void);
extern void handle_packet(void*, void*);
extern void handle_multiappend(uint32_t, uint32_t, void*, void*);
extern uint32_t rss_log(uint32_t, uint16_t, void *);
extern void *rss_log_init(void);

//#define BURST_SIZE 10
#define BURST_SIZE 16
#define NUM_SAMPLES 100000

static uint16_t core_id[RTE_MAX_LCORE];
static struct rte_ring *distributor_rings[RTE_MAX_LCORE];
static struct rte_ring *to_multiappend_ack;
static uint32_t ring_mask = 0;

const uint8_t port = 1; //TODO
static const char *_DELOS_MBUF_POOL = "DELOS_MBUF_POOL";
static const unsigned NUM_MBUFS = 2047;
//static const unsigned NUM_MBUFS = 3000000;
static const unsigned NUM_ALLOC = 0x37FFFF;
static const unsigned NUM_ALLOC2 = 2000000;
static const unsigned MBUF_CACHE_SIZE = 16;
//static const uint16_t DELOS_MBUF_DATA_SIZE = RTE_MBUF_DEFAULT_BUF_SIZE
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 - 64 * 4; // 3968
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 + sizeof(struct ether_hdr) +
//		sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *);
static const uint16_t DELOS_MBUF_DATA_SIZE = 8192;
static struct rte_mempool *packet_pool;
static struct rte_mempool *alloc_pool;
static struct rte_mempool *alloc_pool2;


struct inout {
	uint64_t in;
	uint64_t out;
	uint64_t mean;
	uint64_t per_packet;
	//double var;
	int64_t samples;
} __attribute__ ((aligned (64)));

static struct inout stats[128];

static struct rte_mempool *audit_pool;

#ifdef UNDEF
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

//struct sched_param schedp = {.sched_priority = 99};
static void
print_inout_stats(__attribute__((unused)) void *arg) {
	uint64_t in = 0, out = 0;
	printf("--------------------------------------------------------------------------------\n");
	printf("dist\n\t  in: %12"PRIu64 ",    out: %12"PRIu64 "\n", stats[0].in, stats[0].out);
#ifdef DELOS_BENCHMARK
	uint64_t dmean = stats[0].mean, dvar = stats[0].var, diters = stats[0].samples;
	printf("\tmean: %12"PRIi64 "ns,  sig: %12"PRIi64"ns, iters: %10"PRIu64"\n",
		(int64_t)(dmean/ 2.5), (int64_t) sqrt(dvar / (double)(diters - 1)), diters);
#endif
	//uint64_t dmean = stats[0].mean, diters = stats[0].samples;
	//printf("\tmean: %12"PRIi64 "ns,  iters: %10"PRIu64"\n",
	//		(int64_t)((dmean / diters)/ 2.5), diters);
	for(unsigned i = 1; i < rte_lcore_count(); i++) {
		in += stats[i].in; out += stats[i].out;
		uint64_t mean = stats[i].mean, samples = stats[i].samples;
		if(samples > 0 && in > 0) {
			printf("score %3d\n\t  in: %12"PRIu64 ",    out: %12"PRIu64 ", per_packet: %12"PRIu64 "\n",
					i - 1, stats[i].in, stats[i].out, (int64_t)(stats[i].per_packet / 2.5));
#ifdef DELOS_BENCHMARK
			uint64_t mean = stats[i].mean, var = stats[i].var, iters = stats[i].samples;
			printf("\tmean: %12"PRIi64 "ns,  sig: %12"PRIi64"ns, iters: %10"PRIu64"\n",
					(int64_t)(mean/ 2.5), (int64_t) sqrt(var / (iters - 1)), iters);
#else
			printf("\tmean: %12"PRIi64 "ns,  iters: %10"PRIu64"\n",
					(int64_t)((mean / 2.5) / samples), samples);
		}
#endif
	}

	printf("totals\n\tin: %12"PRIu64 ",  out: %12"PRIu64 "\n", in, out);
	rte_eal_alarm_set(15 * US_PER_S, print_inout_stats, NULL);
}

static inline void
update_packet_addresses(struct rte_mbuf *mbuf) {
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
}

static __attribute__((noreturn)) int
lcore_ack(__attribute__((unused)) void *arg) {
	const unsigned lcore_id = rte_lcore_id();
	const uint32_t score_id = core_id[lcore_id];

	const char * ring_name = "to_ack";
	struct rte_ring * const to_ack = rte_ring_create(ring_name,
			128, rte_socket_id(),
			RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO wha? size?
	struct rte_ring * const ring = to_multiappend_ack;
	unsigned entries = 0;
	rte_eal_alarm_set(15 * US_PER_S, print_inout_stats, NULL);
	printf("Starting ack on lcore %u.\n", lcore_id);
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE * rte_lcore_count()];
		const unsigned nb_rx = rte_ring_sc_dequeue_burst(ring, (void **)bufs, BURST_SIZE);
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
		}

		int err = rte_ring_sp_enqueue_bulk(to_ack, (void * const*) bufs, nb_rx);
		if(err != 0) { //TODO handle -EDQUOT instead
			//TODO
			assert(0 && "out of space in ack ring");
		}
		entries += nb_rx;
		unsigned to_tx = 0;
		for(unsigned i = 0; i < entries; i++) { //TODO this is dumb
			struct rte_mbuf *curr_buf;
			err = rte_ring_sc_dequeue(to_ack, (void **)&curr_buf);
			assert(err == 0);
			struct multi_header *header = rte_pktmbuf_mtod_offset(curr_buf, struct multi_header*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)
					+ sizeof(struct udp_hdr));
			const uint16_t cols = header->cols;
			struct OrderIndex *locations = (struct OrderIndex *)(char *)(&header->cols + 1); //how to top UB?
			int append_finished = 1;
			for(uint16_t j = 0; j < cols && append_finished; j++) {
				append_finished = append_finished && (locations->index != 0);
			}
			if(append_finished) {
				//TODO send in bursts
				bufs[to_tx] = curr_buf;
				to_tx += 1;
				entries -= 1;
			}
			else {
				rte_ring_sp_enqueue(to_ack, curr_buf);
			}
		}
		if(to_tx > 0) {
			//printf("acking %u\n", to_tx);
			const uint16_t nb_tx = rte_eth_tx_burst(port, score_id, bufs, to_tx);
			//TODO handle nb_tx
			assert(nb_tx);
		}
	}
}

static __attribute__((noreturn)) int
lcore_chain(__attribute__((unused)) void *arg) {
#ifdef DELOS_BENCHMARK
	double samples = 0, mean = 0, var = 0;
#endif
	uint64_t samples = 0 , mean = 0;
	uint64_t in = 0, out = 0;
	const uint32_t local_ring_mask = ring_mask;
	if (rte_eth_dev_socket_id(port) > 0 &&
			rte_eth_dev_socket_id(port) != (int)rte_socket_id()) {
		printf("WARNING, port %u is on remote NUMA node %u to "
			"polling thread on %u.\n\tPerformance will "
			"not be optimal.\n", port, rte_eth_dev_socket_id(port), rte_socket_id());
	}

	void *log = init_log();
	const unsigned lcore_id = rte_lcore_id();
	//int err = sched_setscheduler(0, SCHED_FIFO, &schedp);
	//assert(err != -1 && "Could not set scheduler priority.");

	const uint32_t score_id = core_id[lcore_id];
	printf("Starting chain-server on core %u id %u log %p.\n", lcore_id, score_id, log);
	struct rte_ring *ring = distributor_rings[score_id];
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE];
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		uint64_t start_tsc = rte_rdtsc();
		const unsigned nb_rx = rte_ring_sc_dequeue_burst(ring, (void **)bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;
		in += nb_rx;

		for(unsigned i = 0; i < nb_rx; i++) {
			struct rte_mbuf *mbuf = bufs[i];
			struct write_header *data = rte_pktmbuf_mtod_offset(mbuf, struct write_header*,
				sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *));

			if(unlikely(data->kind) == MULTIAPPEND_KIND) { //TODO unlikely?
				handle_multiappend(score_id, local_ring_mask, log, data);
				break;
			}

			update_packet_addresses(mbuf);

			handle_packet(log, data);
		}

		unsigned to_tx = nb_rx;
		const uint16_t nb_tx = rte_eth_tx_burst(port, score_id, bufs, to_tx);
		out += nb_tx;
		if (unlikely(nb_tx < nb_rx)) {
			for (unsigned buf = nb_tx; buf < nb_rx; buf++) rte_pktmbuf_free(bufs[buf]);
		}

		uint64_t duration = rte_rdtsc() - start_tsc;
		mean += duration;
		samples += 1;
		stats[score_id + 1].in = in;
		stats[score_id + 1].out = out;
		stats[score_id + 1].mean = mean;
		stats[score_id + 1].samples = samples;
#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		if(unlikely(samples == 1)) {
			mean = duration;
			var = 0;
		}
		else {
			int64_t delta = duration - mean;
			mean += (delta / samples);
			var += delta * (duration - mean);
			stats[score_id + 1].samples = samples;
			stats[score_id + 1].mean = mean;
			stats[score_id + 1].var = var;
		}
#endif
	}
}

static __attribute__((noreturn)) int
chain(__attribute__((unused)) void *arg) {
#ifdef DELOS_BENCHMARK
	double samples = 0, mean = 0, var = 0;
#endif
	uint64_t samples = 0, mean = 0, per_packet = 0;
	uint64_t in = 0, out = 0;
	const uint32_t local_ring_mask = ring_mask;
	if (rte_eth_dev_socket_id(port) > 0 &&
			rte_eth_dev_socket_id(port) != (int)rte_socket_id()) {
		printf("WARNING, port %u is on remote NUMA node %u to "
			"polling thread on %u.\n\tPerformance will "
			"not be optimal.\n", port, rte_eth_dev_socket_id(port), rte_socket_id());
	}

	void *log = init_log();
	const unsigned lcore_id = rte_lcore_id();
	//int err = sched_setscheduler(0, SCHED_FIFO, &schedp);
	//assert(err != -1 && "Could not set scheduler priority.");

	const uint32_t score_id = core_id[lcore_id];
	printf("Starting chain-server on core %u id %u log %p.\n", lcore_id, score_id, log);
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE];
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		uint64_t start_tsc = rte_rdtsc();
		const unsigned nb_rx = rte_eth_rx_burst(port, 0, bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;
		in += nb_rx;
		stats[score_id + 1].in = in;

		for(unsigned i = 0; i < nb_rx; i++) {
			uint64_t packet_start = rte_rdtsc();
			struct rte_mbuf *mbuf = bufs[i];
			struct write_header *data = rte_pktmbuf_mtod_offset(mbuf, struct write_header*,
				sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *));

			if(unlikely(data->kind) == MULTIAPPEND_KIND) { //TODO unlikely?
				handle_multiappend(score_id, local_ring_mask, log, data);
				break;
			}

			update_packet_addresses(mbuf);

			handle_packet(log, data);
			//rte_prefetch0(bufs[i + 1]);
			per_packet = rte_rdtsc() - packet_start;
		}

		unsigned to_tx = nb_rx;
		const uint16_t nb_tx = rte_eth_tx_burst(port, score_id, bufs, to_tx);
		out += nb_tx;
		stats[score_id + 1].out = out;
		if (unlikely(nb_tx < nb_rx)) {
			for (unsigned buf = nb_tx; buf < nb_rx; buf++) rte_pktmbuf_free(bufs[buf]);
		}
		uint64_t duration = rte_rdtsc() - start_tsc;
		mean += duration;
		samples += 1;
		stats[score_id + 1].in = in;
		stats[score_id + 1].out = out;
		stats[score_id + 1].mean = mean;
		stats[score_id + 1].samples = samples;
		stats[score_id + 1].per_packet = per_packet;
#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		stats[score_id + 1].mean = mean + duration;
		samples += 1;
		stats[score_id + 1].samples = samples;

		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		if(unlikely(samples == 1)) {
			mean = duration;
			var = 0;
		}
		else {
			int64_t delta = duration - mean;
			mean += (delta / samples);
			var += delta * (duration - mean);
			stats[score_id + 1].samples = samples;
			stats[score_id + 1].mean = mean;
			stats[score_id + 1].var = var;
		}
#endif
	}
}

static __attribute__((noreturn)) int
echo(__attribute__((unused)) void *arg) {
#ifdef DELOS_BENCHMARK
	double samples = 0, mean = 0, var = 0;
#endif
	uint64_t in = 0, out = 0;
	if (rte_eth_dev_socket_id(port) > 0 &&
			rte_eth_dev_socket_id(port) != (int)rte_socket_id()) {
		printf("WARNING, port %u is on remote NUMA node %u to "
			"polling thread on %u.\n\tPerformance will "
			"not be optimal.\n", port, rte_eth_dev_socket_id(port), rte_socket_id());
	}

	void *log = init_log();
	const unsigned lcore_id = rte_lcore_id();
	//int err = sched_setscheduler(0, SCHED_FIFO, &schedp);
	//assert(err != -1 && "Could not set scheduler priority.");

	const uint32_t score_id = core_id[lcore_id];
	printf("Starting chain-server on core %u id %u log %p.\n", lcore_id, score_id, log);
	while(1) {
		struct rte_mbuf *bufs[BURST_SIZE];
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		const unsigned nb_rx = rte_eth_rx_burst(port, 0, bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;
		in += nb_rx;
		stats[score_id + 1].in = in;

		for(unsigned i = 0; i < nb_rx; i++) {
			struct rte_mbuf *mbuf = bufs[i];
			update_packet_addresses(mbuf);
		}

		unsigned to_tx = nb_rx;
		const uint16_t nb_tx = rte_eth_tx_burst(port, score_id, bufs, to_tx);
		out += nb_tx;
		stats[score_id + 1].out = out;
		if (unlikely(nb_tx < nb_rx)) {
			for (unsigned buf = nb_tx; buf < nb_rx; buf++) rte_pktmbuf_free(bufs[buf]);
		}
#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		stats[score_id + 1].mean = mean + duration;
		samples += 1;
		stats[score_id + 1].samples = samples;

		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		if(unlikely(samples == 1)) {
			mean = duration;
			var = 0;
		}
		else {
			int64_t delta = duration - mean;
			mean += (delta / samples);
			var += delta * (duration - mean);
			stats[score_id + 1].samples = samples;
			stats[score_id + 1].mean = mean;
			stats[score_id + 1].var = var;
		}
#endif
	}
}

#ifdef UNDEF
static void
print_buf(int packet_id, struct rte_mbuf *buf)
{
	uint8_t *bytes = rte_pktmbuf_mtod(buf, uint8_t *);
	printf("packet id: %d.", packet_id);
	for(int i = 0; i < 96; i += 2) {//48
		if (i % 16 == 0) printf("\n");
			printf("%02" PRIx8 "%02" PRIx8 " ", bytes[i], bytes[i + 1]);
	}
	printf("\n");
	printf("ol_flags 0x%06"PRIx64"\n", buf->ol_flags);
	printf("tci %x outer %x\n", buf->vlan_tci, buf->vlan_tci_outer);
}
#endif

static void
distribute(const uint32_t ring_mask, uint32_t num_slave_cores) {
#ifdef DELOS_BENCHMARK
	double samples = 0, mean = 0, var = 0;
#endif
	uint64_t in = 0, out = 0;
	const unsigned lcore = rte_lcore_id();
	const uint16_t rx_queue_id = 0;
	assert(num_slave_cores < 65);
	uint64_t multi_mask = 0; //TODO handle more than 64 cores? __int128?
	for(uint32_t i = 0; i < num_slave_cores; i++) {
		multi_mask |= 1 << i;
	}

	//int err = sched_setscheduler(0, SCHED_FIFO, &schedp);
	//assert(err != -1);
	printf("Starting distributor on lcore %u.\n", lcore);
	while(1) {
		//TODO handle chain 0
		struct rte_mbuf *bufs[BURST_SIZE];

#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		const uint16_t nb_rx = rte_eth_rx_burst(port, rx_queue_id, bufs, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;
		in += nb_rx;
		stats[0].in = in;

		for(int i = 0; i < nb_rx; i++) {
			struct rte_mbuf *mbuf = bufs[i];

			//TODO prefetch?
			delos_header *header = rte_pktmbuf_mtod_offset(mbuf, delos_header*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)
					+ sizeof(struct udp_hdr));
			//printf("header\n\tchain %u\n\tentry %u\n\t kind %u\n", header->chain, header->entry, header->kind);
			if(unlikely(header->write.kind) == MULTIAPPEND_KIND) { //TODO header format...
				//TODO non pow of 2 cores? transactions
				//printf("dst_ring %u: %p\n", dst, dst_ring);
				//printf("ERROR, trasaction.\n");
				//rte_pktmbuf_refcnt_update(mbuf, num_slave_cores);
				//printf("got multiappend\n");
				struct multi_header* mheader = &header->multi;
				uint16_t chains = mheader->cols;
				if (chains > MAX_MULTIAPPEND_CHAINS) {
					//TODO skip
					assert(0 && "too much append");
				}
				if(chains == 0) {
					printf("header {\n");
					printf("\t        id = 0x%"PRIx64"%"PRIx64"\n;", mheader->id[0], mheader->id[1]);
					printf("\t      kind = 0x%x\n;", mheader->kind);
					printf("\t   padding = 0x%"PRIx8"\n;", mheader->padding[0]);
					printf("\tdata_bytes = 0x%u\n;", mheader->data_bytes);
					printf("\t dep_bytes = 0x%u\n;", mheader->dep_bytes);
					printf("\t    chains = 0x%"PRIx16"\n;", mheader->cols);
					printf("}\n");
					assert(0 && "too little append");
				}
				//printf("it's valid\n");
				struct OrderIndex *locations = (struct OrderIndex *)(char *)(&mheader->cols + 1);
				uint64_t remaining_mask = multi_mask; //TODO handle chain 0
				//for(uint16_t j = 0; j < chains; j++) {
				//	printf("(%u, %u)\n", locations[j].order, locations[j].index);
				//}
				for(uint16_t j = 0; j < chains; j++) {
					uint32_t dst = locations[j].order & ring_mask;
					assert(locations[j].order != 0);
					if((remaining_mask & (1 << dst)) != 0) {
						//printf("multi sending %u to %u\n", locations[j].order, dst);
						remaining_mask &= ~(1 << dst);
						struct rte_ring* dst_ring = distributor_rings[dst];
						rte_ring_sp_enqueue(dst_ring, mbuf);
					}
				}
				rte_ring_sp_enqueue(to_multiappend_ack, mbuf);
				//TODO ack immediately?
			}
			else {
				struct write_header* wheader = &header->write;
				uint32_t dst = wheader->loc[0] & ring_mask;
				//printf("single sending %u to %u\n", wheader->loc[0], dst);
				//if(rss_log(dst, header->chain, seen_set) != 0) rte_exit(EXIT_FAILURE, "chain dupe\n");
				struct rte_ring* dst_ring = distributor_rings[dst];
				int r = rte_ring_sp_enqueue(dst_ring, mbuf);
				if(likely(r == 0)) {
					out += 1;
					stats[0].out = out;
				}
				else {
					rte_pktmbuf_free(mbuf);
				}
			}

#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		if(unlikely(samples == 1)) {
			mean = duration;
			var = 0;
		}
		else {
			int64_t delta = duration - mean;
			mean += (delta / samples);
			var += delta * (duration - mean);
			stats[0].samples = samples;
			stats[0].mean = mean;
			stats[0].var = var;
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

void * alloc_seg(void);
void * ualloc_seg(void);

void *
alloc_seg(void) {
	//printf("alloc start.\n");
	void *obj = NULL;
	int ret = rte_mempool_mc_get(alloc_pool, &obj);
	//assert(buf != NULL && "out of mem");
	if(ret != 0) {
		int ret = rte_mempool_mc_get(alloc_pool2, &obj);
		if(ret != 0) {
			assert(0 && "out of mem");
			return NULL;
		}
	}
	assert(DELOS_MBUF_DATA_SIZE == 8192);
	memset(obj, 0, DELOS_MBUF_DATA_SIZE);
	//printf("alloc done.\n");
	return obj;
}

void *
ualloc_seg(void) {
	//printf("alloc start.\n");
	void *obj = NULL;
	int ret = rte_mempool_mc_get(alloc_pool, &obj);
	//assert(buf != NULL && "out of mem");
	if(ret != 0) {
		int ret = rte_mempool_mc_get(alloc_pool2, &obj);
		if(ret != 0) {
			assert(0 && "out of mem");
			return NULL;
		}
	}
	assert(DELOS_MBUF_DATA_SIZE == 8192);
	//printf("alloc done.\n");
	rte_prefetch0(obj);
	return obj;
}

int
main(int argc, char **argv)
{
	int ret;
	unsigned lcore_id, ack_core_id = 0;
	uint32_t num_rings = 0;

	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_panic("Cannot init EAL\n");

	ret = rte_eth_dev_count();
	if (ret == 0) rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
	assert(ret == 2);

	alloc_pool = rte_mempool_create("DELOS_ALLOC_POOL", NUM_ALLOC, 8192, 128, 0, NULL, NULL, NULL, NULL,
			SOCKET_ID_ANY, 0);
	if (alloc_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get alloc pool due to %s\n", rte_strerror(rte_errno));
	}

	alloc_pool2 = rte_mempool_create("DELOS_ALLOC_POOL2", NUM_ALLOC2, 8192, 128, 0, NULL, NULL, NULL, NULL,
			SOCKET_ID_ANY, 0);
	if (alloc_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get alloc pool due to %s\n", rte_strerror(rte_errno));
	}

	packet_pool = rte_pktmbuf_pool_create(_DELOS_MBUF_POOL, NUM_MBUFS,
			MBUF_CACHE_SIZE, 0, DELOS_MBUF_DATA_SIZE,
			rte_eth_dev_socket_id(port));
			//SOCKET_ID_ANY);
			//rte_socket_id());
	if (packet_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get memory pool for buffers due to %s\n", rte_strerror(rte_errno));
	}
	unsigned number_chain_cores = rte_lcore_count() - 2; //TODO
	audit_pool = packet_pool;
	{
		uint32_t score_id = 0;
		char ring_name[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			core_id[lcore_id] = score_id;
			snprintf(ring_name, 10, "d%u", score_id);
			if(score_id < number_chain_cores) {
				distributor_rings[score_id] = rte_ring_create(ring_name, 16, rte_socket_id(),
					RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO size
				if(distributor_rings[score_id] == NULL) {
					rte_exit(EXIT_FAILURE, "Cannot init distributor ring %d memory "
						"pool due to:\n\t%s\n", score_id, rte_strerror(rte_errno));
				}
			}
			else {
				//last core acks multiappend packets
				ack_core_id = lcore_id;
				to_multiappend_ack = rte_ring_create(ring_name, 16, rte_socket_id(),
					RING_F_SP_ENQ | RING_F_SC_DEQ);
			}
			score_id += 1;
		}
		assert(score_id == rte_lcore_count() - 1);
	}
	num_rings = next_power_of_2(number_chain_cores);
	ring_mask = num_rings - 1;
	printf("num scores %u\n", rte_lcore_count() - 1);
	printf("num ccores %u\n", number_chain_cores);
	printf("num rings  %d\n", num_rings);
	printf("ring mask  0x%x\n", ring_mask);

	for(uint32_t i = number_chain_cores; i < num_rings; i++) {
		distributor_rings[i] = distributor_rings[i % number_chain_cores];
		assert(!!0);
	}
	{
		struct rte_eth_dev_info info;
		int retval;
		struct ether_addr addr;
		const uint16_t rx_rings = 1, tx_rings = rte_lcore_count() - 1; //TODO
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
			retval = rte_eth_rx_queue_setup(port, i, 128,
				rte_eth_dev_socket_id(port),
				&info.default_rxconf, //TODO
				packet_pool);
			if (retval < 0) rte_exit(EXIT_FAILURE, "RX queue failed\n");
		}

		for(int i = 0; i < tx_rings; i++) {
			retval = rte_eth_tx_queue_setup(port, i, 128,
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
	//{
	//	assert(ack_core_id > 0);
	//	RTE_LCORE_FOREACH_SLAVE(lcore_id) {
	//		if(lcore_id != ack_core_id) {
	//			rte_eal_remote_launch(lcore_chain, NULL, lcore_id);
	//		}
	//		else {
	//			rte_eal_remote_launch(lcore_ack, NULL, lcore_id);
	//		}
	//	}
	//}
	rte_eal_remote_launch(lcore_ack, NULL, ack_core_id);
	//TODO delay...
	//distribute(ring_mask, number_chain_cores);
	chain(distribute);
	//echo(distribute);

	rte_eal_mp_wait_lcore();
	return 0;
}
