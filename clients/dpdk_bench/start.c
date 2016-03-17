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

#define BURST_SIZE 8
#define NUM_SAMPLES 1000

const uint8_t port = 1; //TODO
static const char *_DELOS_MBUF_POOL = "DELOS_MBUF_POOL";
//static const unsigned NUM_MBUFS = 2047;
//static const unsigned NUM_MBUFS = 0x1f100;
static const unsigned NUM_MBUFS = 0xffff;
static const unsigned MBUF_CACHE_SIZE = 16;
//static const uint16_t DELOS_MBUF_DATA_SIZE = RTE_MBUF_DEFAULT_BUF_SIZE
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 - 64 * 4; // 3968
//static const uint16_t DELOS_MBUF_DATA_SIZE = 4096 + sizeof(struct ether_hdr) +
//		sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *);
static const uint16_t DELOS_MBUF_DATA_SIZE = 8192;

//static struct rte_mempool *audit_pool;
struct rte_mempool *packet_pool;
#ifdef NNNNNNNNNN
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

//struct sched_param schedp = {.sched_priority = 99};

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
	uint64_t dist_dur[NUM_SAMPLES];
	int iters = 0;
#endif

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
		const unsigned nb_rx = rte_ring_sc_dequeue_burst(ring, (void **)bufs, BURST_SIZE);

		if (unlikely(nb_rx == 0)) continue;

		for(unsigned i = 0; i < nb_rx; i++) {
			struct rte_mbuf *mbuf = bufs[i];
			struct write_header *data = rte_pktmbuf_mtod_offset(mbuf, struct write_header*,
				sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr *));

			if(unlikely(data->kind) == MULTIAPPEND_KIND) { //TODO unlikely?
				handle_multiappend(score_id, local_ring_mask, log, data);
				break;
			}

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

			handle_packet(log, data);
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
#endif
//TODO take option
//e4:1d:2d:0c:86:40
static const struct ether_addr dest_addr = {.addr_bytes = {0xe4, 0x1d, 0x2d, 0x0c, 0x86, 0x00}};
static const struct ether_addr source_addr = {.addr_bytes = {0xe4, 0x1d, 0x2d, 0x0c, 0x86, 0x40}};

//TODO take option
static const uint32_t dest_ip = IPv4(10, 21, 7, 4);
static const uint32_t source_ip = IPv4(10, 21, 7, 5);

static rte_atomic16_t start = {.cnt = 0};

#define IP_DN_FRAGMENT_FLAG 0x0040
#define IP_DEFTTL 64

static int64_t sends[50];
static int64_t recvs[50];
static int64_t deltas[50];

#define NUM_SECS 5
//#define SEC 2500000000ull

static uint64_t SEC = 0;

rte_spinlock_t max_tx_lock;
uint32_t max_tx[8] = {0, 0, 0, 0, 0, 0, 0, 0};

//__attribute__((unused))
//static __attribute__((noreturn)) int
static int
lcore_rx(void *arg) {
	//printf("recv on core %d\n", rte_lcore_id());
	const int id = (*(int *)arg) - 1;
	assert(id < 8);
	uint32_t max_rx[8] = {0, 0, 0, 0, 0, 0, 0, 0};
	while(rte_atomic16_read(&start) == 0);
	int64_t received_packets = 0;
	//int64_t delta = 0;
	double mean = 0.0;
	double var = 0.0;
	uint64_t start_time = 0;
	while(start_time == 0) {
		struct rte_mbuf *mbuf[BURST_SIZE];
		const uint16_t nb_rx = rte_eth_rx_burst(port, id, mbuf, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;
		start_time = rte_rdtsc();
		for (unsigned buf = 0; buf < nb_rx; buf++) {
			struct write_header* w = rte_pktmbuf_mtod_offset(mbuf[buf], struct write_header *,
								sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			//assert(w->kind & 0x80);
			if(w->kind & 0x80) {
				received_packets += 1;
				//printf("%08"PRIx8"\n", w->kind);
				int64_t* ts = rte_pktmbuf_mtod_offset(mbuf[buf], int64_t *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct write_header));
				int64_t end = rte_rdtsc();
				double duration = ((double)(end - *ts)) / 2.5;
				//printf("(%u, %u)\n ", w->loc[0], w->loc[1]);
				if(unlikely(received_packets == 1)) {
					mean = duration;
					var = 0.0;
				}
				else {
					int64_t delta = duration - mean;
					mean += (((double)delta) / (double)received_packets);
					var += delta * (duration - mean);
				}
			}
			rte_pktmbuf_free(mbuf[buf]);
		}
	}

	while(rte_rdtsc() - start_time < NUM_SECS * SEC) { //TODO
		struct rte_mbuf *mbuf[BURST_SIZE];
		const uint16_t nb_rx = rte_eth_rx_burst(port, id, mbuf, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;

		// nb_rx;
		for (unsigned buf = 0; buf < nb_rx; buf++) {
			struct write_header* w = rte_pktmbuf_mtod_offset(mbuf[buf], struct write_header *,
								sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			//assert(w->kind & 0x80);
			if(w->kind & 0x80) {
				received_packets += 1;
				//printf("%08"PRIx8"\n", w->kind);
				//printf("(%u, %u)\n ", w->loc[0], w->loc[1]);
				int64_t* ts = rte_pktmbuf_mtod_offset(mbuf[buf], int64_t *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct write_header));
				int64_t end = rte_rdtsc();
				double duration = ((double)(end - *ts)) / 2.5;
				uint32_t chain = w->loc[0], entry = w->loc[1];
				assert(chain > 0 && chain < 9);
				if(entry > max_rx[chain - 1]) max_rx[chain - 1] = entry;
				if(unlikely(received_packets == 1)) {
					mean = duration;
					var = 0.0;
				}
				else {
					int64_t delta = duration - mean;
					mean += (((double)delta) / (double)received_packets);
					var += delta * (duration - mean);
				}
			}
			rte_pktmbuf_free(mbuf[buf]);
		}
	}

	recvs[id] = received_packets;
	//delta = ((double)delta / (double)received_packets);
	deltas[id] = mean;
	rte_spinlock_lock(&max_tx_lock);
	for(int i = 0; i < 8; i++) {
		if(max_rx[i] > max_tx[i]) max_tx[i] = max_rx[i];
	}
	rte_spinlock_unlock(&max_tx_lock);
	printf("done on  recv %d, recv %"PRIi64" packets, mean time %"PRIi64"ns, σ %"PRIu64".\n",
			id, received_packets, (int64_t) mean, (int64_t) sqrt(var / (double)(received_packets - 1)));
	return 0;
}

static int
lcore_rx_transaction(void *arg) {
	//printf("recv on core %d\n", rte_lcore_id());
	const int id = (*(int *)arg) - 1;
	assert(id < 9);
	uint32_t max_rx[8] = {0, 0, 0, 0, 0, 0, 0, 0};
	while(rte_atomic16_read(&start) == 0);
	int64_t received_packets = 0;
	//int64_t delta = 0;
	double mean = 0.0;
	double var = 0.0;
	uint64_t start_time = 0;
	while(start_time == 0) {
		struct rte_mbuf *mbuf[BURST_SIZE];
		const uint16_t nb_rx = rte_eth_rx_burst(port, id, mbuf, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;
		start_time = rte_rdtsc();
		for (unsigned buf = 0; buf < nb_rx; buf++) {
			//struct multi_header* w = rte_pktmbuf_mtod_offset(mbuf[buf], struct multi_header *,
			//		sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			//assert(w->kind & 0x80);
			//if(w->kind & 0x80) {
				received_packets += 1;
				//printf("%08"PRIx8"\n", w->kind);
				int64_t* ts = rte_pktmbuf_mtod_offset(mbuf[buf], int64_t *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr)
					+ sizeof(struct multi_header) + sizeof(uint64_t) * 4);
				int64_t end = rte_rdtsc();
				double duration = ((double)(end - *ts)) / 2.5;
				//printf("(%u, %u)\n ", w->loc[0], w->loc[1]);
				if(unlikely(received_packets == 1)) {
					mean = duration;
					var = 0.0;
				}
				else {
					int64_t delta = duration - mean;
					mean += (((double)delta) / (double)received_packets);
					var += delta * (duration - mean);
				}
			//}
			rte_pktmbuf_free(mbuf[buf]);
		}
	}

	while(rte_rdtsc() - start_time < NUM_SECS * SEC) { //TODO
		struct rte_mbuf *mbuf[BURST_SIZE];
		const uint16_t nb_rx = rte_eth_rx_burst(port, id, mbuf, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;

		// nb_rx;
		for (unsigned buf = 0; buf < nb_rx; buf++) {
			struct multi_header* w = rte_pktmbuf_mtod_offset(mbuf[buf], struct multi_header *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			//assert(w->kind & 0x80);
			//if(w->kind & 0x80) {
				received_packets += 1;
				//printf("%08"PRIx8"\n", w->kind);
				//printf("(%u, %u)\n ", w->loc[0], w->loc[1]);
				int64_t* ts = rte_pktmbuf_mtod_offset(mbuf[buf], int64_t *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr)
					+ sizeof(struct multi_header) + sizeof(struct OrderIndex) * 2);
				struct OrderIndex* locs = (struct OrderIndex *)(char *)(&w->cols + 1);
				int64_t end = rte_rdtsc();
				double duration = ((double)(end - *ts)) / 2.5;

				uint32_t chain0 = locs[0].order, entry0 = locs[0].index,
						chain1 = locs[1].order, entry1 = locs[1].index;

				if(!(chain0 > 0 && chain0 < 9)) printf("0: (%u, %u).\n", chain0, entry0);
				if(!(chain1 > 0 && chain1 < 9)) printf("1: (%u, %u).\n", chain1, entry1);
				if(entry0 > max_rx[chain0 - 1]) max_rx[chain0 - 1] = entry0;
				if(entry1 > max_rx[chain1 - 1]) max_rx[chain1 -1 ] = entry1;
				if(unlikely(received_packets == 1)) {
					mean = duration;
					var = 0.0;
				}
				else {
					int64_t delta = duration - mean;
					mean += (((double)delta) / (double)received_packets);
					var += delta * (duration - mean);
				}
			//}
			rte_pktmbuf_free(mbuf[buf]);
		}
	}

	recvs[id] = received_packets;
	//delta = ((double)delta / (double)received_packets);
	deltas[id] = mean;
	rte_spinlock_lock(&max_tx_lock);
	for(int i = 0; i < 8; i++) {
		if(max_rx[i] > max_tx[i]) max_tx[i] = max_rx[i];
	}
	rte_spinlock_unlock(&max_tx_lock);
	printf("done on  recv %d, recv %"PRIi64" packets, mean time %"PRIi64"ns, σ %"PRIu64".\n",
			id, received_packets, (int64_t) mean, (int64_t) sqrt(var / (double)(received_packets - 1)));
	return 0;
}

static const uint64_t tx_ol_flags =  PKT_TX_IPV4 | PKT_TX_IP_CKSUM | PKT_TX_UDP_CKSUM;
#define HEADERS_SIZE (sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr))
#define DATA_SIZE 1000
//#define DATA_SIZE (sizeof(struct write_header) + sizeof(uint64_t))


static int
lcore_tx_transaction(void *chain_num) {
	const int chain = *(int *)chain_num;
	assert(0 < chain && chain <= 8);
	uint16_t packet_id = chain * 1000 + 1;
	uint64_t sent_packets = 0, packets = 1;

	//const struct ether_addr src_addr = source_addr;
	//printf("starting send for chain %d\n", chain);

	const struct multi_header base_header = {
		.id = {0, 0},
		.kind = 2,
		.data_bytes = DATA_SIZE - sizeof(struct multi_header),
		.dep_bytes = 0,
		.cols = 2,
	};

	while(rte_atomic16_read(&start) == 0);

	const uint64_t start_time = rte_rdtsc();
	while(rte_rdtsc() - start_time < NUM_SECS * SEC){//(not_out_of_time()) {
		//printf("sending from %d.\n", chain);
		struct rte_mbuf *mbuf[BURST_SIZE];
		for(int i = 0; i < BURST_SIZE; i++) {
			mbuf[i] = rte_pktmbuf_alloc(packet_pool);
			//TODO prefetch
		}

		for(int j = 0; j < BURST_SIZE; j++)
		{
			mbuf[j]->data_off = RTE_PKTMBUF_HEADROOM;
			mbuf[j]->nb_segs = 1;
			mbuf[j]->port = port;
			mbuf[j]->next = NULL;
			mbuf[j]->data_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);//TODO packet len
			mbuf[j]->pkt_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);
			mbuf[j]->ol_flags |= tx_ol_flags;// | PKT_TX_VLAN_PKT;
			mbuf[j]->l2_len = sizeof(struct ether_hdr);
			mbuf[j]->l3_len = sizeof(struct ipv4_hdr);
			mbuf[j]->l4_len = sizeof(struct udp_hdr);
			mbuf[j]->packet_type = RTE_PTYPE_L2_ETHER | RTE_PTYPE_L3_IPV4 | RTE_PTYPE_L4_UDP;


			struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf[j], struct ether_hdr *);
			eth->d_addr = dest_addr;
			eth->s_addr = source_addr;
			eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

			struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(mbuf[j], struct ipv4_hdr *,
					sizeof(struct ether_hdr));
			//ip->version_ihl = 0x45; //TODO (4 << 4) | 5;//rte_cpu_to_be_16?
			ip->version_ihl = 0x45;//
			ip->type_of_service = 0;
			ip->total_length = rte_cpu_to_be_16(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			assert(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) < UINT16_MAX);
			ip->packet_id = rte_cpu_to_be_16(packet_id++);
			ip->fragment_offset = IP_DN_FRAGMENT_FLAG;
			ip->time_to_live = IP_DEFTTL;
			assert(IPPROTO_UDP == 0x11);
			ip->next_proto_id = IPPROTO_UDP;
			ip->hdr_checksum = 0; //TODO
			ip->src_addr = rte_cpu_to_be_32(source_ip);
			ip->dst_addr = rte_cpu_to_be_32(dest_ip);
			//ip->hdr_checksum = rte_ipv4_cksum(ip);

			struct udp_hdr *udp = rte_pktmbuf_mtod_offset(mbuf[j], struct udp_hdr *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)); //TODO IP IHL?
			//TODO
			udp->dst_port = rte_cpu_to_be_16(chain);
			udp->src_port = rte_cpu_to_be_16(chain);
			//udp->dst_port = rte_cpu_to_be_16(chain + 0xf);
			//udp->src_port = rte_cpu_to_be_16(chain ^ 0xf);
			//udp->dst_port = rte_cpu_to_be_16(chain + j);
			//udp->src_port = rte_cpu_to_be_16(chain + j);
			//udp->dst_port = rte_cpu_to_be_16(chain + j);
			//udp->src_port = rte_cpu_to_be_16(chain + j);
			//udp->dgram_cksum = rte_ipv4_phdr_cksum(ip, mbuf[j]->ol_flags | tx_ol_flags);
			udp->dgram_cksum = 0;
			udp->dgram_len = rte_cpu_to_be_16(DATA_SIZE);

			struct multi_header* multi = rte_pktmbuf_mtod_offset(mbuf[j], struct multi_header*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			*multi = base_header;
			multi->id[1] = sent_packets;

			struct OrderIndex* locs = (struct OrderIndex *)(char *)(&multi->cols + 1);
			locs[0].order = chain;
			locs[1].order = ((chain + 1) % 9) != 0 ? (chain + 1) % 9 : 1;

			uint64_t* ts = rte_pktmbuf_mtod_offset(mbuf[j], uint64_t*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr)
					+ sizeof(struct multi_header) + sizeof(uint64_t) * 4);
			*ts = rte_rdtsc();
			packets += 1;
		}

		//{
		//	uint8_t *bytes = rte_pktmbuf_mtod(mbuf[0], uint8_t *);
		//	if (chain == 1) {
		//		printf("packet id: %d.", packet_id);
		//		for(int i = 0; i < 96; i += 2) {
		//			if (i % 16 == 0) printf("\n");
		//			printf("%02" PRIx8 "%02" PRIx8 " ", bytes[i], bytes[i + 1]);
		//		}
		//		printf("\n");
		//	}
		//}

		//TODO core body

		//const uint16_t nb_tx = rte_eth_tx_burst(port, chain, mbuf, 1);
		const uint16_t nb_tx = rte_eth_tx_burst(port, chain - 1, mbuf, BURST_SIZE);
		sent_packets += nb_tx;
		if (unlikely(nb_tx < BURST_SIZE)) {
			for (unsigned buf = nb_tx; buf < BURST_SIZE; buf++) rte_pktmbuf_free(mbuf[buf]);
		}
		//if(nb_tx != BURST_SIZE) printf("nb_tx %u at packet id %u\n", nb_tx, packet_id);
		//assert(nb_tx == BURST_SIZE);
	}

	sends[chain - 1] = sent_packets;
	printf("done on chain %d, sent %"PRIi64" packets.\n", chain, sent_packets);
	return 0;
}

//__attribute__((unused))
static int
lcore_tx(void *chain_num) {
	const int chain = *(int *)chain_num;
	assert(0 < chain && chain < 9);
	uint16_t packet_id = chain * 1000 + 1;
	uint64_t sent_packets = 0, packets = 1;

	//const struct ether_addr src_addr = source_addr;
	//printf("starting send for chain %d\n", chain);

	const struct write_header base_header = {
		.id = {0, 0},
		.kind = 1,
		.loc = {chain, 1},
		.data_bytes = DATA_SIZE - sizeof(struct write_header),
		.dep_bytes = 0,
	};

	while(rte_atomic16_read(&start) == 0);

	const uint64_t start_time = rte_rdtsc();
	while(rte_rdtsc() - start_time < NUM_SECS * SEC){//(not_out_of_time()) {
		//printf("sending from %d.\n", chain);
		struct rte_mbuf *mbuf[BURST_SIZE];
		for(int i = 0; i < BURST_SIZE; i++) {
			mbuf[i] = rte_pktmbuf_alloc(packet_pool);
			//TODO prefetch
		}

		for(int j = 0; j < BURST_SIZE; j++)
		{
			mbuf[j]->data_off = RTE_PKTMBUF_HEADROOM;
			mbuf[j]->nb_segs = 1;
			mbuf[j]->port = port;
			mbuf[j]->next = NULL;
			mbuf[j]->data_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);//TODO packet len
			mbuf[j]->pkt_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);
			mbuf[j]->ol_flags |= tx_ol_flags;// | PKT_TX_VLAN_PKT;
			mbuf[j]->l2_len = sizeof(struct ether_hdr);
			mbuf[j]->l3_len = sizeof(struct ipv4_hdr);
			mbuf[j]->l4_len = sizeof(struct udp_hdr);
			mbuf[j]->packet_type = RTE_PTYPE_L2_ETHER | RTE_PTYPE_L3_IPV4 | RTE_PTYPE_L4_UDP;


			struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf[j], struct ether_hdr *);
			eth->d_addr = dest_addr;
			eth->s_addr = source_addr;
			eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

			struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(mbuf[j], struct ipv4_hdr *,
					sizeof(struct ether_hdr));
			//ip->version_ihl = 0x45; //TODO (4 << 4) | 5;//rte_cpu_to_be_16?
			ip->version_ihl = 0x45;//
			ip->type_of_service = 0;
			ip->total_length = rte_cpu_to_be_16(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			assert(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) < UINT16_MAX);
			ip->packet_id = rte_cpu_to_be_16(packet_id++);
			ip->fragment_offset = IP_DN_FRAGMENT_FLAG;
			ip->time_to_live = IP_DEFTTL;
			assert(IPPROTO_UDP == 0x11);
			ip->next_proto_id = IPPROTO_UDP;
			ip->hdr_checksum = 0; //TODO
			ip->src_addr = rte_cpu_to_be_32(source_ip);
			ip->dst_addr = rte_cpu_to_be_32(dest_ip);
			//ip->hdr_checksum = rte_ipv4_cksum(ip);

			struct udp_hdr *udp = rte_pktmbuf_mtod_offset(mbuf[j], struct udp_hdr *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)); //TODO IP IHL?
			//TODO
			udp->dst_port = rte_cpu_to_be_16(chain);
			udp->src_port = rte_cpu_to_be_16(chain);
			//udp->dst_port = rte_cpu_to_be_16(chain + 0xf);
			//udp->src_port = rte_cpu_to_be_16(chain ^ 0xf);
			//udp->dst_port = rte_cpu_to_be_16(chain + j);
			//udp->src_port = rte_cpu_to_be_16(chain + j);
			//udp->dst_port = rte_cpu_to_be_16(chain + j);
			//udp->src_port = rte_cpu_to_be_16(chain + j);
			//udp->dgram_cksum = rte_ipv4_phdr_cksum(ip, mbuf[j]->ol_flags | tx_ol_flags);
			udp->dgram_cksum = 0;
			udp->dgram_len = rte_cpu_to_be_16(DATA_SIZE);

			struct write_header* write = rte_pktmbuf_mtod_offset(mbuf[j], struct write_header*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			*write = base_header;
			//write->id[1] = packets;
			//write->loc[1] = packets;
			write->id[1] = sent_packets;
			write->loc[1] = sent_packets + 1;
			//write->loc[0] = chain;
			uint64_t* ts = rte_pktmbuf_mtod_offset(mbuf[j], uint64_t*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct write_header));
			*ts = rte_rdtsc();
			packets += 1;
		}

		//{
		//	uint8_t *bytes = rte_pktmbuf_mtod(mbuf[0], uint8_t *);
		//	if (chain == 1) {
		//		printf("packet id: %d.", packet_id);
		//		for(int i = 0; i < 96; i += 2) {
		//			if (i % 16 == 0) printf("\n");
		//			printf("%02" PRIx8 "%02" PRIx8 " ", bytes[i], bytes[i + 1]);
		//		}
		//		printf("\n");
		//	}
		//}

		//TODO core body

		//const uint16_t nb_tx = rte_eth_tx_burst(port, chain, mbuf, 1);
		const uint16_t nb_tx = rte_eth_tx_burst(port, chain - 1, mbuf, BURST_SIZE);
		sent_packets += nb_tx;
		if (unlikely(nb_tx < BURST_SIZE)) {
			for (unsigned buf = nb_tx; buf < BURST_SIZE; buf++) rte_pktmbuf_free(mbuf[buf]);
		}
		//if(nb_tx != BURST_SIZE) printf("nb_tx %u at packet id %u\n", nb_tx, packet_id);
		//assert(nb_tx == BURST_SIZE);
	}

	sends[chain - 1] = sent_packets;
	printf("done on chain %d, sent %"PRIi64" packets.\n", chain, sent_packets);
	return 0;
}


static int
lcore_tx2(void *chain_num) {
	const int chain = *(int *)chain_num;
	assert(0 < chain && chain < 9);
	uint16_t packet_id = chain * 1000 + 1;
	uint64_t sent_packets = 0, packets = 1;
	const uint32_t max_loc = max_tx[chain-1];

	//const struct ether_addr src_addr = source_addr;
	//printf("starting send for chain %d\n", chain);

	const struct write_header base_header = {
		.id = {0, 0},
		.kind = 3,
		.loc = {chain, 1},
		.data_bytes = DATA_SIZE - sizeof(struct write_header),
		.dep_bytes = 0,
	};

	while(rte_atomic16_read(&start) == 0);

	const uint64_t start_time = rte_rdtsc();
	while(rte_rdtsc() - start_time < NUM_SECS * SEC){//(not_out_of_time()) {
		//printf("sending from %d.\n", chain);
		struct rte_mbuf *mbuf[BURST_SIZE];
		for(int i = 0; i < 1; i++) {
			mbuf[i] = rte_pktmbuf_alloc(packet_pool);
			//TODO prefetch
		}

		for(int j = 0; j < BURST_SIZE; j++)
		{
			mbuf[j]->data_off = RTE_PKTMBUF_HEADROOM;
			mbuf[j]->nb_segs = 1;
			mbuf[j]->port = port;
			mbuf[j]->next = NULL;
			mbuf[j]->data_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);//TODO packet len
			mbuf[j]->pkt_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);
			mbuf[j]->ol_flags |= tx_ol_flags;// | PKT_TX_VLAN_PKT;
			mbuf[j]->l2_len = sizeof(struct ether_hdr);
			mbuf[j]->l3_len = sizeof(struct ipv4_hdr);
			mbuf[j]->l4_len = sizeof(struct udp_hdr);
			mbuf[j]->packet_type = RTE_PTYPE_L2_ETHER | RTE_PTYPE_L3_IPV4 | RTE_PTYPE_L4_UDP;


			struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf[j], struct ether_hdr *);
			eth->d_addr = dest_addr;
			eth->s_addr = source_addr;
			eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

			struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(mbuf[j], struct ipv4_hdr *,
					sizeof(struct ether_hdr));
			//ip->version_ihl = 0x45; //TODO (4 << 4) | 5;//rte_cpu_to_be_16?
			ip->version_ihl = 0x45;//
			ip->type_of_service = 0;
			ip->total_length = rte_cpu_to_be_16(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			assert(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) < UINT16_MAX);
			ip->packet_id = rte_cpu_to_be_16(packet_id++);
			ip->fragment_offset = IP_DN_FRAGMENT_FLAG;
			ip->time_to_live = IP_DEFTTL;
			assert(IPPROTO_UDP == 0x11);
			ip->next_proto_id = IPPROTO_UDP;
			ip->hdr_checksum = 0; //TODO
			ip->src_addr = rte_cpu_to_be_32(source_ip);
			ip->dst_addr = rte_cpu_to_be_32(dest_ip);
			//ip->hdr_checksum = rte_ipv4_cksum(ip);

			struct udp_hdr *udp = rte_pktmbuf_mtod_offset(mbuf[j], struct udp_hdr *,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)); //TODO IP IHL?
			//TODO
			udp->dst_port = rte_cpu_to_be_16(chain);
			udp->src_port = rte_cpu_to_be_16(chain);
			//udp->dst_port = rte_cpu_to_be_16(chain + 0xf);
			//udp->src_port = rte_cpu_to_be_16(chain ^ 0xf);
			//udp->dst_port = rte_cpu_to_be_16(chain + j);
			//udp->src_port = rte_cpu_to_be_16(chain + j);
			//udp->dst_port = rte_cpu_to_be_16(chain + j);
			//udp->src_port = rte_cpu_to_be_16(chain + j);
			//udp->dgram_cksum = rte_ipv4_phdr_cksum(ip, mbuf[j]->ol_flags | tx_ol_flags);
			udp->dgram_cksum = 0;
			udp->dgram_len = rte_cpu_to_be_16(DATA_SIZE);

			struct write_header* write = rte_pktmbuf_mtod_offset(mbuf[j], struct write_header*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
			*write = base_header;
			//write->id[1] = packets;
			//write->loc[1] = packets;
			write->id[1] = sent_packets;
			write->loc[1] = (sent_packets + 1) % max_loc;
			//write->loc[0] = chain;
			uint64_t* ts = rte_pktmbuf_mtod_offset(mbuf[j], uint64_t*,
					sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct write_header));
			*ts = rte_rdtsc();
			packets += 1;
		}

		//{
		//	uint8_t *bytes = rte_pktmbuf_mtod(mbuf[0], uint8_t *);
		//	if (chain == 1) {
		//		printf("packet id: %d.", packet_id);
		//		for(int i = 0; i < 96; i += 2) {
		//			if (i % 16 == 0) printf("\n");
		//			printf("%02" PRIx8 "%02" PRIx8 " ", bytes[i], bytes[i + 1]);
		//		}
		//		printf("\n");
		//	}
		//}

		//TODO core body

		//const uint16_t nb_tx = rte_eth_tx_burst(port, chain, mbuf, 1);
		const uint16_t nb_tx = rte_eth_tx_burst(port, chain - 1, mbuf, BURST_SIZE);
		sent_packets += nb_tx;
		if (unlikely(nb_tx < BURST_SIZE)) {
			for (unsigned buf = nb_tx; buf < BURST_SIZE; buf++) rte_pktmbuf_free(mbuf[buf]);
		}
		//if(nb_tx != BURST_SIZE) printf("nb_tx %u at packet id %u\n", nb_tx, packet_id);
		//assert(nb_tx == BURST_SIZE);
	}

	sends[chain] = sent_packets;
	printf("done on chain %d, sent %"PRIi64" packets.\n", chain, sent_packets);
	return 0;
}

void * init_map(void);
void put_time(void *, uint64_t, int64_t);
int64_t take_time(void*, uint64_t, int64_t);

__attribute__((unused))
static int
lcore_txrx(void *chain_num) {
	const int id = *(int *)chain_num;
	assert(id == 0);
	uint16_t packet_id = 1000 + 1;
	uint64_t sent_packets = 0, packets = 1;
	int64_t received_packets = 0, valid_packets = 0;
	int64_t delta = 0;

	//const struct ether_addr src_addr = source_addr;
	//printf("starting send for chain %d\n", chain);

	const struct write_header base_header = {
		.id = {0, 0},
		.kind = 1,
		.loc = {1, 1},
		.data_bytes = DATA_SIZE - sizeof(struct write_header),
		.dep_bytes = 0,
	};

	void *map = init_map();

	const uint64_t start_time = rte_rdtsc();
	while(rte_rdtsc() - start_time < NUM_SECS * SEC){//(not_out_of_time()) {
		//printf("sending from %d.\n", chain);
		{
			struct rte_mbuf *mbuf[BURST_SIZE];
			for(int i = 0; i < BURST_SIZE; i++) {
				mbuf[i] = rte_pktmbuf_alloc(packet_pool);
				//TODO prefetch
			}

			for(int j = 0; j < BURST_SIZE; j++)
			{
				mbuf[j]->data_off = RTE_PKTMBUF_HEADROOM;
				mbuf[j]->nb_segs = 1;
				mbuf[j]->port = port;
				mbuf[j]->next = NULL;
				mbuf[j]->data_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);//TODO packet len
				mbuf[j]->pkt_len = DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct ether_hdr);
				mbuf[j]->ol_flags |= tx_ol_flags;// | PKT_TX_VLAN_PKT;
				mbuf[j]->l2_len = sizeof(struct ether_hdr);
				mbuf[j]->l3_len = sizeof(struct ipv4_hdr);
				mbuf[j]->l4_len = sizeof(struct udp_hdr);
				mbuf[j]->packet_type = RTE_PTYPE_L2_ETHER | RTE_PTYPE_L3_IPV4 | RTE_PTYPE_L4_UDP;


				struct ether_hdr *eth = rte_pktmbuf_mtod(mbuf[j], struct ether_hdr *);
				eth->d_addr = dest_addr;
				eth->s_addr = source_addr;
				eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

				struct ipv4_hdr *ip = rte_pktmbuf_mtod_offset(mbuf[j], struct ipv4_hdr *,
						sizeof(struct ether_hdr));
				//ip->version_ihl = 0x45; //TODO (4 << 4) | 5;//rte_cpu_to_be_16?
				ip->version_ihl = 0x45;//
				ip->type_of_service = 0;
				ip->total_length = rte_cpu_to_be_16(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
				assert(DATA_SIZE + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) < UINT16_MAX);
				ip->packet_id = rte_cpu_to_be_16(packet_id++);
				ip->fragment_offset = IP_DN_FRAGMENT_FLAG;
				ip->time_to_live = IP_DEFTTL;
				assert(IPPROTO_UDP == 0x11);
				ip->next_proto_id = IPPROTO_UDP;
				ip->hdr_checksum = 0; //TODO
				ip->src_addr = rte_cpu_to_be_32(source_ip);
				ip->dst_addr = rte_cpu_to_be_32(dest_ip);
				//ip->hdr_checksum = rte_ipv4_cksum(ip);

				struct udp_hdr *udp = rte_pktmbuf_mtod_offset(mbuf[j], struct udp_hdr *,
						sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)); //TODO IP IHL?
				//TODO
				udp->dst_port = rte_cpu_to_be_16(8000);
				udp->src_port = rte_cpu_to_be_16(8000);
				//udp->dst_port = rte_cpu_to_be_16(chain + 0xf);
				//udp->src_port = rte_cpu_to_be_16(chain ^ 0xf);
				//udp->dst_port = rte_cpu_to_be_16(chain + j);
				//udp->src_port = rte_cpu_to_be_16(chain + j);
				//udp->dst_port = rte_cpu_to_be_16(chain + j);
				//udp->src_port = rte_cpu_to_be_16(chain + j);
				//udp->dgram_cksum = rte_ipv4_phdr_cksum(ip, mbuf[j]->ol_flags | tx_ol_flags);
				udp->dgram_cksum = 0;
				udp->dgram_len = rte_cpu_to_be_16(DATA_SIZE);

				struct write_header* write = rte_pktmbuf_mtod_offset(mbuf[j], struct write_header*,
						sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
				*write = base_header;
				write->id[1] = packets;
				write->loc[1] = packets;
				//write->loc[0] = chain;
				int64_t start = rte_rdtsc();
				put_time(map, packets, start);
				int64_t* ts = rte_pktmbuf_mtod_offset(mbuf[j], int64_t*,
						sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct write_header));
				*ts = start;
				packets += 1;
			}

			//{
			//	uint8_t *bytes = rte_pktmbuf_mtod(mbuf[0], uint8_t *);
			//	if (chain == 1) {
			//		printf("packet id: %d.", packet_id);
			//		for(int i = 0; i < 96; i += 2) {
			//			if (i % 16 == 0) printf("\n");
			//			printf("%02" PRIx8 "%02" PRIx8 " ", bytes[i], bytes[i + 1]);
			//		}
			//		printf("\n");
			//	}
			//}

			//TODO core body

			//const uint16_t nb_tx = rte_eth_tx_burst(port, chain, mbuf, 1);
			const uint16_t nb_tx = rte_eth_tx_burst(port, id, mbuf, BURST_SIZE);
			sent_packets += nb_tx;
			if (unlikely(nb_tx < BURST_SIZE)) {
				for (unsigned buf = nb_tx; buf < BURST_SIZE; buf++) rte_pktmbuf_free(mbuf[buf]);
			}
			//if(nb_tx != BURST_SIZE) printf("nb_tx %u at packet id %u\n", nb_tx, packet_id);
			//assert(nb_tx == BURST_SIZE);
		}
		{
			struct rte_mbuf *mbuf[BURST_SIZE];
			const uint16_t nb_rx = rte_eth_rx_burst(port, id, mbuf, BURST_SIZE);
			if (unlikely(nb_rx == 0)) continue;
			int64_t end = rte_rdtsc();

			// nb_rx;
			for (unsigned buf = 0; buf < nb_rx; buf++) {
				struct write_header* w = rte_pktmbuf_mtod_offset(mbuf[buf], struct write_header *,
									sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr));
				//assert(w->kind & 0x80);
				if(w->kind & 0x80) {
					assert(w->id[1] <= sent_packets);
					//printf("%08"PRIx8"\n", w->kind);
					//int64_t* ts = rte_pktmbuf_mtod_offset(mbuf[buf], int64_t *,
					//	sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + sizeof(struct write_header));
					int64_t d = take_time(map, w->loc[1], end);
					received_packets += 1;
					delta += d;
					if(d != 0) { //TODO why dupes??
						valid_packets += 1;
					}
				}
				rte_pktmbuf_free(mbuf[buf]);
			}
		}
	}


	sends[id] = sent_packets;
	recvs[id] = received_packets;
	delta /= valid_packets;
	deltas[id] = delta;
	printf("done on chain %d, sent %"PRIi64" packets.\n", id, sent_packets);
	printf("done on  recv %d, recv %"PRIi64" packets, valid %"PRIi64" avg time %"PRIi64"ns.\n", id, received_packets, valid_packets, (int64_t)(delta / 2.5));
	return 0;
}

int chain_num[] = {1, 2, 3, 4, 5, 6, 7, 8};

static void
print_packets(void)
{
	uint64_t sent_packets = 0;
	uint64_t recv_packets = 0;
	for(int i = 0; i < 50; i++) {
		sent_packets += sends[i];
		sends[i] = 0;
	}
	for(int i = 0; i < 50; i++) {
		recv_packets += recvs[i];
		recvs[i] = 0;
	}
	printf("sent %12"PRIi64" packets.\n", sent_packets);
	printf("recv %12"PRIi64" packets.\n", recv_packets);

	printf("sent %12"PRIi64" bytes.\n", sent_packets * (DATA_SIZE + HEADERS_SIZE));
	printf("recv %12"PRIi64" bytes.\n", recv_packets * (DATA_SIZE + HEADERS_SIZE));

	printf("recv %12"PRIi64" packets/s.\n", recv_packets / NUM_SECS);
	printf("recv %12"PRIi64" bytes/s.\n", (recv_packets * (DATA_SIZE + HEADERS_SIZE)) / NUM_SECS);
}

#define TEST_TRANSACTION (!0)

int
main(int argc, char **argv)
{
	int ret;

	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_panic("Cannot init EAL\n");

	ret = rte_eth_dev_count();
	if (ret == 0) rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
	assert(ret == 2);

	SEC = rte_get_tsc_hz();

	packet_pool = rte_pktmbuf_pool_create(_DELOS_MBUF_POOL, NUM_MBUFS,
			MBUF_CACHE_SIZE, 0, DELOS_MBUF_DATA_SIZE,
			rte_eth_dev_socket_id(port));
			//SOCKET_ID_ANY);
			//rte_socket_id());
	if (packet_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get memory pool for buffers due to %s\n", rte_strerror(rte_errno));
	}

	printf("%d lcores\n", rte_lcore_count());
	//assert(rte_lcore_count() == 17);
	//audit_pool = packet_pool;

	{
		struct rte_eth_dev_info info;
		int retval;
		//const uint16_t rx_rings = rte_lcore_count() - 1, tx_rings = rte_lcore_count() - 1;
		const uint16_t rx_rings = 8, tx_rings = 8;
		//const uint16_t rx_rings = 1, tx_rings = 1;
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
	if(TEST_TRANSACTION) {
		printf("Testing transactions.\n");
	}
	else {
		printf("Testing appends.\n");
	}
	{
		int i = 0;
		unsigned lcore_id = 0;
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			if(TEST_TRANSACTION) {
				if(i < 8) {
					rte_eal_remote_launch(lcore_rx_transaction, &chain_num[i], lcore_id);
				}
				else if (i < 16) {
					//else if (i < 2) {
					rte_eal_remote_launch(lcore_tx_transaction, &chain_num[i - 8], lcore_id);
					//rte_eal_remote_launch(lcore_tx, &chain_num[0], lcore_id);
				}
			}
			else {
				if(i < 8) {
				//if(i < 1) {
					rte_eal_remote_launch(lcore_rx, &chain_num[i], lcore_id);
					//rte_eal_remote_launch(lcore_tx, &chain_num[i], lcore_id);
				}

				else if (i < 16) {
				//else if (i < 2) {
					rte_eal_remote_launch(lcore_tx, &chain_num[i - 8], lcore_id);
					//rte_eal_remote_launch(lcore_tx, &chain_num[0], lcore_id);
				}
			}
			i++;
		}
		printf("i = %d.\n", i);
		assert(i == 16);
	}
	rte_atomic16_set(&start, !0);
	//TODO delay...
	//lcore_tx(&chain_num[i - 7]);
	//int zero = 0;
	//lcore_txrx(&zero);
	//lcore_rx(&zero);
	rte_eal_mp_wait_lcore();
	rte_atomic16_set(&start, 0);

	print_packets();
	rte_spinlock_lock(&max_tx_lock);
	for(int i = 0; i < 8; i++) {
		printf("%u: %u\n", i + 1, max_tx[i]);
	}
	rte_spinlock_unlock(&max_tx_lock);
	{
		int i = 0;
		unsigned lcore_id = 0;
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			if(i < 8) {
			//if(i < 1) {
				rte_eal_remote_launch(lcore_rx, &chain_num[i], lcore_id);
				//rte_eal_remote_launch(lcore_tx, &chain_num[i], lcore_id);
			}
			else if (i < 16) {
			//else if (i < 2) {
				rte_eal_remote_launch(lcore_tx2, &chain_num[i - 8], lcore_id);
				//rte_eal_remote_launch(lcore_tx, &chain_num[0], lcore_id);
			}
			i++;
		}
		printf("i = %d.\n", i);
		assert(i == 16);
		}
	rte_atomic16_set(&start, !0);
	//TODO delay...
	//lcore_tx(&chain_num[i - 7]);
	//int zero = 0;
	//lcore_txrx(&zero);
	//lcore_rx(&zero);
	rte_eal_mp_wait_lcore();
	print_packets();
	return 0;
}
