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
#include <rte_power.h>
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

#define DELOS_BENCHMARK 1

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

struct entry_pointer_and_index {
	void **entry;
	char *ptr;
	uint32_t index;
};

extern void *init_log(void);
struct entry_pointer_and_index get_next_loc(void *, size_t, uint32_t);
char *get_val(void *, uint32_t, uint32_t);
void * alloc_seg(void);
void * ualloc_seg(void);

//#define BURST_SIZE 10
//#define BURST_SIZE 15
#define BURST_SIZE 31
#define NUM_SAMPLES 100000

const uint8_t port = 1; //TODO
static const char *_DELOS_MBUF_POOL = "DELOS_MBUF_POOL";
static const unsigned NUM_MBUFS = 2047;
//static const unsigned NUM_MBUFS = 3000000;
static const unsigned NUM_ALLOC = 0x37FFFF;
//static const unsigned NUM_ALLOC2 = 2000000;
static const unsigned MBUF_CACHE_SIZE = 31;
static const uint16_t DELOS_MBUF_DATA_SIZE = 8192;
static uint32_t core_id[RTE_MAX_LCORE];
static struct rte_mempool *packet_pool;
static struct rte_mempool *alloc_pool;
static struct rte_mempool *alloc_pool2;

#include "trie.h"

//#include "trie.h"

struct inout {
	int64_t in;
	int64_t out;
	int64_t mean;
	int64_t per_packet;
	//double var;
	int64_t samples;
} __rte_cache_aligned;

static struct inout stats[128];

static struct rte_mempool *audit_pool;

static uint64_t send_to_copy_iters = 0;

static void
print_inout_stats(__attribute__((unused)) void *arg) {
#ifdef DELOS_BENCHMARK
	uint64_t total_in = 0, total_out = 0;
	printf("--------------------------------------------------------------------------------\n");
	{
		int64_t mean = stats[0].mean, samples = stats[0].samples, in = stats[0].in, out = stats[0].out,
				per_packet = stats[0].per_packet;
		if (in > 0) {
			printf("distribute\n\t  in: %12"PRIi64 ",    out: %12"PRIi64 ", per_packet: %12"PRIi64 "\n",
					in, out, (int64_t)((per_packet / in) / 2.5));
			if(samples > 0) {
				printf("\tmean: %12"PRIi64 "ns,  iters: %10"PRIi64", per_packet: %12"PRIi64 "\n",
						(int64_t)((mean / 2.5) / samples), samples,
						(int64_t)(((mean / 2.5) / samples) / (out / samples)));
			}
		}
		printf("\t stc: %12"PRIi64",    miss: %12"PRIi64 ".\n", send_to_copy_iters, send_to_copy_iters - out);
	}
	for(unsigned i = 1; i < rte_lcore_count(); i++) {
		total_in += stats[i].in; total_out += stats[i].out;
		int64_t mean = stats[i].mean, samples = stats[i].samples, in = stats[i].in, out = stats[i].out,
				per_packet = stats[i].per_packet;
		if (in > 0) {
			printf("score %3d\n\t  in: %12"PRIi64 ",    out: %12"PRIi64 ", per_packet: %12"PRIi64 "\n",
					i - 1, in, out, (int64_t)((per_packet / in) / 2.5));
			if(samples > 0) {
				printf("\tmean: %12"PRIi64 "ns,  iters: %10"PRIi64", per_packet: %12"PRIi64 "\n",
						(int64_t)((mean / 2.5) / samples), samples,
						(int64_t)(((mean / 2.5) / samples) / (out / samples)));
			}
		}
	}

	printf("totals\n\tin: %12"PRIu64 ",  out: %12"PRIu64 "\n", total_in, total_out);
	rte_eal_alarm_set(10 * US_PER_S, print_inout_stats, NULL);
#endif
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
lcore_stats(__attribute__((unused)) void *arg) {
	printf("Starting stats on lcore %u.\n", rte_lcore_id());
	rte_eal_alarm_set(15 * US_PER_S, print_inout_stats, NULL);
	while(1) rte_pause();
}

static struct rte_ring *to_copy_rings[RTE_MAX_LCORE];
static struct rte_ring *ack_ring;


#if defined(DELOS_DEBUG_ACK) || defined(DELOS_DEBUG_ALL)
#define debug_ack(fmt, ...) printf("ACK: " fmt, ## __VA_ARGS__)
#else
#define debug_ack(...) do { } while (0)
#endif

static __attribute__((hot, noreturn)) int
lcore_ack(__attribute__((unused)) void *arg) {
	const unsigned lcore_id = rte_lcore_id();
	const uint32_t score_id = core_id[lcore_id];
#ifdef DELOS_BENCHMARK
	int64_t mean = 0, samples = 0, per_packet = 0;
	int64_t in = 0, out = 0;
#endif

	struct rte_ring *restrict const ring = ack_ring;
	printf("Starting ack on lcore %u, id %u.\n", lcore_id, score_id);
	while(1) {
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		struct rte_mbuf *bufs[BURST_SIZE];
		unsigned nb_rx = rte_ring_sc_dequeue_burst(ring, (void **)bufs, BURST_SIZE);
		if(unlikely(nb_rx == 0)) continue;

#ifdef DELOS_BENCHMARK
		in += nb_rx;
#endif

		for(unsigned i = 0; i < nb_rx; i++) update_packet_addresses(bufs[i]);

		debug_ack("sending %u.\n", nb_rx);
		//printf("acking %u\n", to_tx);
		uint16_t nb_tx = rte_eth_tx_burst(port, score_id, bufs, nb_rx);
		while (unlikely(nb_tx < nb_rx)) {
			//for (unsigned buf = nb_tx; buf < nb_rx; buf++) rte_pktmbuf_free(bufs[buf]);
			nb_tx += rte_eth_tx_burst(port, score_id, &bufs[nb_tx], nb_rx - nb_tx);
		}

		for(unsigned i = 0; i < nb_rx; i++) assert(0 && "TODO decref"); //TODO decref
#ifdef DELOS_BENCHMARK
		out += nb_tx;
#endif
#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		mean += duration;
		stats[score_id + 1].in = in;
		stats[score_id + 1].out = out;
		stats[score_id + 1].mean = mean;
		stats[score_id + 1].samples = samples;
		stats[score_id + 1].per_packet = per_packet; //TODO
#endif
	}
}

static inline const union delos_header *
data_ptr_const(const struct rte_mbuf *restrict mbuf)
{
	return rte_pktmbuf_mtod_offset(mbuf, const delos_header *restrict,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)
			+ sizeof(struct udp_hdr));
}

static inline union delos_header *
data_ptr(struct rte_mbuf *restrict mbuf)
{
	return rte_pktmbuf_mtod_offset(mbuf, delos_header *restrict,
			sizeof(struct ether_hdr) + sizeof(struct ipv4_hdr)
			+ sizeof(struct udp_hdr));
}

static inline int
mbuf_is_not_multiput(const struct rte_mbuf *restrict mbuf)
{
	return data_ptr_const(mbuf)->write.kind & DATA_KIND;
}

static inline int
is_not_multiput(const delos_header *restrict header)
{
	return (header->write.kind & 0x3) != MULTIAPPEND_KIND;
}

static inline int
is_read(const struct rte_mbuf *restrict mbuf)
{
	return data_ptr_const(mbuf)->write.kind & (DATA_KIND | MULTIAPPEND_KIND);
}

static inline size_t
delos_data_size(const delos_header *restrict header)
{
	if(is_not_multiput(header)) {
		return sizeof(struct write_header) + header->write.data_bytes + header->write.dep_bytes;
	}
	else {
		return sizeof(struct multi_header) + header->multi.data_bytes
				+ header->multi.dep_bytes
				+ header->multi.cols * sizeof(struct OrderIndex);
	}
}

static inline size_t
data_size(const char *restrict data)
{
	return delos_data_size((const delos_header *) data);
}

static inline size_t
mbuf_data_size(const struct rte_mbuf *restrict mbuf)
{
	return delos_data_size(data_ptr_const(mbuf));
}

#if defined(DELOS_DEBUG_COPY) || defined(DELOS_DEBUG_ALL)
#define debug_copy(fmt, ...) printf("%3u: " fmt, score_id, ## __VA_ARGS__)
#else
#define debug_copy(...) do { } while (0)
#endif

#define NOT_MULT_OF_3(n) (n != 3 && n != 6 && n != 9 && n != 12 && n != 15)

static __attribute__((hot, noreturn)) int
lcore_copy(__attribute__((unused)) void *arg) {
	const unsigned lcore_id = rte_lcore_id();
	const uint32_t score_id = core_id[lcore_id];
#ifdef DELOS_BENCHMARK
	int64_t mean = 0, samples = 0, per_packet = 0;
	int64_t in = 0, out = 0;
#endif

	struct rte_ring *restrict const input_ring = to_copy_rings[score_id];
	printf("Starting copy on lcore %u, id %u.\n", lcore_id, score_id);
	while(1) {
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		void *ptrs[15];
		struct rte_mbuf *buf_to_tx[5];
		//int err = rte_ring_sc_dequeue_bulk(input_ring, ptrs, 3); //TODO burst?
		//if(unlikely(err != 0)) continue;
		uint16_t nb_rx = rte_ring_sc_dequeue_burst(input_ring, (void **)ptrs, 15); //TODO burst?
		if(unlikely(nb_rx == 0)) continue;

		rte_prefetch0(ptrs[0]);
		rte_prefetch0(ptrs[1]);

		if(unlikely(nb_rx < 3)) {
			while(nb_rx < 3) {
				int err = rte_ring_sc_dequeue(input_ring, &ptrs[nb_rx]);
				if(err != 0) nb_rx += 1;
			}
		}
		else if(unlikely(NOT_MULT_OF_3(nb_rx))) {
			while(nb_rx < 3) {
				int err = rte_ring_sc_dequeue(input_ring, &ptrs[nb_rx]);
				if(err != 0) nb_rx += 1;
			}
		}
		debug_copy("got %u items.\n", nb_rx);
		nb_rx /= 3;
#ifdef DELOS_BENCHMARK
		uint64_t packet_start_tsc = rte_rdtsc();
#endif
		//struct rte_mbuf *restrict mbuf = ptrs[0];
		//char *const restrict write_loc = ptrs[1];
		//char **const restrict table_loc = ptrs[2];
		//rte_prefetch0(write_loc); //TODO?
#ifdef DELOS_BENCHMARK
		in += nb_rx;
#endif
		for(unsigned i = 0; i < nb_rx; i++) {
			struct rte_mbuf *restrict mbuf = ptrs[0 + 3 * i];
			buf_to_tx[i] = mbuf;
			char *const restrict write_loc = ptrs[1 + 3 * i];
			char **const restrict table_loc = ptrs[2 + 3 * i];
			if(is_not_multiput(data_ptr_const(mbuf))) {
				debug_copy("got single-chain.\n");
				update_packet_addresses(mbuf); //TODO no-send
			} else debug_copy("got multiput.\n");

			if(likely(write_loc != NULL)) {
				if(is_read(mbuf)) {
					debug_copy("got read.\n");
					rte_memcpy(data_ptr(mbuf), write_loc, data_size(write_loc));
				}
				else {
					debug_copy("got append.\n");
					rte_memcpy(write_loc, data_ptr_const(mbuf), mbuf_data_size(mbuf));
					//*write_loc |= 0x80; TODO MULI?
					rte_smp_wmb(); //TODO correct membarrier?
					// looking through the dpdk code, this is valid on x86_64
					// (see lib/librte_eal/common/include/arch/x86/rte_atomic_64.h:81
					//  where rte_atomic64_set is defined as `v->cnt = new_value`)
					// unfortunately this cannot be made architecture independent without
					// `ifdef`s on the size of pointers, and slightly specious casts
					*table_loc = write_loc;
				}
			} else debug_copy("got empty read.\n");
			#ifdef DELOS_BENCHMARK
					per_packet += rte_rdtsc() - packet_start_tsc;
			#endif
		}

		//TODO burst? no-send multiputs, only 1 ack core?
		{
			debug_copy("sending %u.\n", nb_rx);
			uint16_t nb_tx = rte_eth_tx_burst(port, score_id, buf_to_tx, nb_rx);
			while (unlikely(nb_tx < nb_rx)) {
				debug_copy("missed send %u.\n", nb_rx - nb_tx);
				nb_tx += rte_eth_tx_burst(port, score_id, &buf_to_tx[nb_tx], nb_rx - nb_tx);

			}
#ifdef DELOS_BENCHMARK
			out += nb_tx;
#endif
		}
#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		mean += duration;
		stats[score_id + 1].in = in;
		stats[score_id + 1].out = out;
		stats[score_id + 1].mean = mean;
		stats[score_id + 1].samples = samples;
		stats[score_id + 1].per_packet = per_packet; //TODO
#endif
	}
}

static struct rte_ring *from_rx_rings[2];

static __attribute__((hot, noreturn)) int
lcore_rx(__attribute__((unused)) void *arg) {
#ifdef DELOS_BENCHMARK
	int64_t mean = 0, samples = 0;
	int64_t in = 0, out = 0;
#endif
	const unsigned lcore_id = rte_lcore_id();
	const uint32_t score_id = core_id[lcore_id];
	assert((score_id == 0 || score_id == 1) && "rx_queue_id == score_id");
	const uint16_t rx_queue_id = score_id; //TODO do not tie to score_id
	struct rte_ring *restrict const output_ring = from_rx_rings[rx_queue_id];
	assert(output_ring != NULL);
	printf("Starting rx on lcore %u, id %u.\n", lcore_id, score_id);
	while(1) {
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		struct rte_mbuf *bufs[BURST_SIZE];
		const uint16_t nb_rx = rte_eth_rx_burst(port, rx_queue_id, bufs, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;
#ifdef DELOS_BENCHMARK
		in += nb_rx;
#endif
		unsigned nb_tx = rte_ring_sp_enqueue_burst(output_ring, (void * const*)bufs, nb_rx);
		if(unlikely(nb_tx < nb_rx)) {
			for(int i = nb_tx; i < nb_rx; i++) rte_pktmbuf_free(bufs[i]);
		}

#ifdef DELOS_BENCHMARK
		out += nb_tx;
		samples += 1;
		uint64_t duration = rte_rdtsc() - start_tsc;
		samples += 1;
		mean += duration;
		stats[score_id + 1].in = in;
		stats[score_id + 1].out = out;
		stats[score_id + 1].mean = mean;
		stats[score_id + 1].samples = samples;
#endif
	}
}

#if defined(DELOS_DEBUG_DISTRIBUTE) || defined(DELOS_DEBUG_ALL)
#define debug_dist(fmt, ...) printf("CLS: " fmt, ## __VA_ARGS__)
#else
#define debug_dist(...) do { } while (0)
#endif

void *
alloc_seg(void) {
	//printf("alloc start.\n");
	void *obj = NULL;
	int ret = rte_mempool_sc_get(alloc_pool, &obj);
	//assert(buf != NULL && "out of mem");
	if(ret != 0) {
		int ret = rte_mempool_sc_get(alloc_pool2, &obj);
		if(ret != 0) {
			assert(0 && "out of mem");
			return NULL;
		}
	}
	_Static_assert(DELOS_MBUF_DATA_SIZE == 8192, "bad alloc size");
	memset(obj, 0, DELOS_MBUF_DATA_SIZE);
	//printf("alloc done.\n");
	return obj;
}

void *
ualloc_seg(void) {
	//printf("alloc start.\n");
	void *obj = NULL;
	int ret = rte_mempool_sc_get(alloc_pool, &obj);
	//assert(buf != NULL && "out of mem");
	if(ret != 0) {
		int ret = rte_mempool_sc_get(alloc_pool2, &obj);
		if(ret != 0) {
			assert(0 && "out of mem");
			return NULL;
		}
	}
	_Static_assert(DELOS_MBUF_DATA_SIZE == 8192, "bad alloc size");
	//printf("alloc done.\n");
	//rte_prefetch2(obj);
	return obj;
}

inline static uint32_t
send_to_copy_core(void * const* ptrs, unsigned num_ptrs, uint32_t next_copy_ring, uint32_t num_copy_cores)
{
	int err;
	do {
		err = rte_ring_sp_enqueue_bulk(to_copy_rings[next_copy_ring], ptrs, num_ptrs);
		next_copy_ring += 1;
		if(next_copy_ring >= num_copy_cores) {
			next_copy_ring = 2; //TODO
		}
		send_to_copy_iters++;
	} while(err != 0); //TODO drop on timeout?
	return next_copy_ring;
}

inline static uint32_t
prep_copy_core_for_multi(char *write_loc, void **table_loc,
		uint32_t next_copy_ring, uint32_t num_copy_cores)
{
	int err;
	do {
		err = 1;
		assert(0 && write_loc && table_loc && "unimplented");
	} while(err != 0); //TODO drop on timeout?
	next_copy_ring += 1; //TODO non-stalling mechanism
	if(next_copy_ring >= num_copy_cores) {
		next_copy_ring = 2; //TODO
	}
	return next_copy_ring;
}

inline static uint32_t
send_multi_to_copy(struct rte_mbuf *mbuf, uint32_t next_copy_ring, uint32_t num_copy_cores)
{
	int err;
	do {
		err = 1;
		assert(0 && mbuf && "unimplented");
	} while(err != 0);
	next_copy_ring += 1;
	if(next_copy_ring >= num_copy_cores) {
		next_copy_ring = 2; //TODO
	}
	return next_copy_ring;
}

static __attribute__((hot, noreturn)) void
classify(uint32_t num_copy_cores) {
#ifdef DELOS_BENCHMARK
	uint64_t in = 0, out = 0, mean = 0, samples = 0, per_packet = 0;
#endif
	const unsigned lcore = rte_lcore_id();
	uint32_t next_copy_ring = 2;//TODO

	//int err = sched_setscheduler(0, SCHED_FIFO, &schedp);
	//assert(err != -1);
	DelosTrieRootTable *restrict const log = c_log_init();
	uint8_t rx_ring_num = 1; //TODO handle more than just 2 rx rings
	struct rte_ring *restrict const rx_rings[2] = { from_rx_rings[0], from_rx_rings[1]}; //TODO handle more than just 2 rx rings
	assert(rx_rings[0] != NULL && rx_rings[1] != NULL);
	//void ** temp_storage = alloc_seg();
	printf("Starting classifier on lcore %u.\n", lcore);
	while(1) {
		rx_ring_num = (~rx_ring_num) & 1; //TODO handle more than just 2 rx rings
		assert(rx_ring_num == 0 || rx_ring_num == 1);
#ifdef DELOS_BENCHMARK
		uint64_t start_tsc = rte_rdtsc();
#endif
		struct rte_mbuf *bufs[BURST_SIZE];
		const uint16_t nb_rx = rte_ring_sc_dequeue_burst(rx_rings[rx_ring_num], (void **)bufs, BURST_SIZE);
		if (unlikely(nb_rx == 0)) continue;
#ifdef DELOS_BENCHMARK
		in += nb_rx;
#endif
		//rte_prefetch0(bufs[0]);
		//rte_prefetch0(bufs[1]);
		//rte_prefetch0(bufs[2]);
		//rte_prefetch0(bufs[0] + 1);
		//rte_prefetch0(bufs[1] + 1);
		//rte_prefetch0(bufs[2] + 1);

		//for(int i = 0; i < nb_rx; i++) rte_prefetch2(data_ptr(bufs[i]));

		for(int i = 0; i < nb_rx; i++) {
			struct rte_mbuf *restrict mbuf = bufs[i];
			rte_prefetch0(data_ptr(bufs[i]));

#ifdef DELOS_BENCHMARK
			const uint64_t packet_start_tsc = rte_rdtsc();
#endif
			//TODO prefetch?
			delos_header *restrict header = data_ptr(mbuf);

			if(unlikely(header->write.kind) == MULTIAPPEND_KIND) {
				uint32_t next_multi_copy_ring = next_copy_ring; //TODO
				rte_mbuf_refcnt_set(mbuf, 2);//TODO
				struct multi_header *restrict mheader = &header->multi;
				mheader->kind |= 0x80; //TODO
				uint16_t chains = mheader->cols;
				if (unlikely(chains > MAX_MULTIAPPEND_CHAINS)) {
					//TODO skip
					assert(0 && "too much append");
				}
				//printf("it's valid\n");
				struct OrderIndex *restrict locations = (struct OrderIndex *restrict)(char *)(&mheader->cols + 1);
				//TODO incref
				for(uint16_t j = 0; j < chains; j++) {
					debug_dist("(%u, %u) ", locations[j].order, locations[j].index);
				}
				debug_dist(".\n");
				size_t data_size = mbuf_data_size(mbuf);
				for(uint16_t j = 0; j < chains; j++) {
					uint32_t chain = locations[j].order;
					assert(locations[j].order != 0);
					struct entry_pointer_and_index pi = get_next_loc(log, data_size, chain);
					locations[j].index = pi.index; //TODO get pointer? store spillover
					next_multi_copy_ring = prep_copy_core_for_multi(pi.ptr, pi.entry, next_multi_copy_ring, num_copy_cores);
				}
				for(uint16_t j = 0; j < chains; j++) {
					//TODO where does ptr go? (private data? next in queue? (other queue?)
					next_copy_ring = send_multi_to_copy(mbuf, next_copy_ring, num_copy_cores);
				}
				assert(next_copy_ring == next_multi_copy_ring);
				rte_ring_sp_enqueue(ack_ring, mbuf);
			}
			else if(is_read(mbuf)) {
				struct write_header *restrict wheader = &header->write;
				char *val = c_get_val(log, wheader->loc[0], wheader->loc[1]);
				debug_dist("send read (%u, %u) -> %p to %u.\n", wheader->loc[0], wheader->loc[1], val, next_copy_ring);
				void * ptrs[3] = {mbuf, val, NULL }; //TODO batch
				next_copy_ring = send_to_copy_core(ptrs, 3, next_copy_ring, num_copy_cores);
			}
			else { //TODO batch?
				struct write_header *restrict wheader = &header->write;
				wheader->kind |= 0x80; //TODO
				uint32_t chain = wheader->loc[0];
				struct entry_pointer_and_index pi = c_get_next_loc(log, mbuf_data_size(mbuf), chain);
				wheader->loc[1] = pi.index;
				debug_dist("send append (%u, %u) to %u.\n", wheader->loc[0], wheader->loc[1], next_copy_ring);
				//rte_prefetch0(bufs[i + 1]); ?
				//TODO batch
				void * ptrs[3] = {mbuf, pi.ptr, pi.entry }; //TODO batch
				next_copy_ring = send_to_copy_core(ptrs, 3, next_copy_ring, num_copy_cores);
			}
#ifdef DELOS_BENCHMARK
			per_packet += rte_rdtsc() - packet_start_tsc;
#endif
		}
#ifdef DELOS_BENCHMARK
		out += nb_rx; //TODO
#endif
#ifdef DELOS_BENCHMARK
		uint64_t duration = rte_rdtsc() - start_tsc;
		mean += duration;
		samples += 1;
		stats[0].in = in;
		stats[0].out = out;
		stats[0].mean = mean;
		stats[0].samples = samples;
		stats[0].per_packet = per_packet;
#endif
	}
}

int
main(int argc, char **argv)
{
	int ret;
	unsigned lcore_id, ack_core_id = UINT_MAX, stats_core_id = UINT_MAX, rx_core1_id = UINT_MAX, rx_core2_id = UINT_MAX;

	ret = rte_eal_init(argc, argv);
	if (ret < 0) rte_panic("Cannot init EAL\n");

	ret = rte_eth_dev_count();
	if (ret == 0) rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
	assert(ret == 2);

	alloc_pool = rte_mempool_create("DELOS_ALLOC_POOL", NUM_ALLOC, 8192, 256, 0, NULL, NULL, NULL, NULL,
			SOCKET_ID_ANY, 0);
	if (alloc_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get alloc pool due to %s\n", rte_strerror(rte_errno));
	}

	alloc_pool2 = rte_mempool_create("DELOS_ALLOC_POOL2", NUM_ALLOC, 8192, 256, 0, NULL, NULL, NULL, NULL,
			SOCKET_ID_ANY, 0);
	if (alloc_pool == NULL) {
		rte_exit(EXIT_FAILURE, "Cannot get alloc pool due to %s\n", rte_strerror(rte_errno));
	}

	unsigned number_copy_cores = rte_lcore_count() - 5; //TODO
	printf("Using %u copy cores.\n", number_copy_cores);
	audit_pool = packet_pool;
	{

		uint32_t score_id = 0, copy_cores = 0;
		char ring_name[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			core_id[lcore_id] = score_id;
			//rte_power_init(lcore_id);
			if(score_id < 1) {
				snprintf(ring_name, 10, "f%u", score_id);
				from_rx_rings[score_id] = rte_ring_create(ring_name, 512,
						rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO size
				if(from_rx_rings[score_id] == NULL) {
					rte_exit(EXIT_FAILURE, "Cannot init rx ring %d memory "
							"pool due to:\n\t%s\n", score_id, rte_strerror(rte_errno));
				}
				rx_core1_id = lcore_id;
			}
			else if(score_id < 2) {
				snprintf(ring_name, 10, "f%u", score_id);
				from_rx_rings[score_id] = rte_ring_create(ring_name, 512,
						rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO size
				if(from_rx_rings[score_id] == NULL) {
					rte_exit(EXIT_FAILURE, "Cannot init rx ring %d memory "
							"pool due to:\n\t%s\n", score_id, rte_strerror(rte_errno));
				}
				rx_core2_id = lcore_id;
			}
			else if(copy_cores < number_copy_cores) {
				printf("chain core id %u.\n", lcore_id);
				//rte_power_freq_max(lcore_id);

				snprintf(ring_name, 10, "d%u", score_id);
				to_copy_rings[score_id] = rte_ring_create(ring_name, 4,
						rte_socket_id(), RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO size, smaller is faster!?

				if(to_copy_rings[score_id] == NULL) {
					rte_exit(EXIT_FAILURE, "Cannot init distributor ring %d memory "
						"pool due to:\n\t%s\n", score_id, rte_strerror(rte_errno));
				}
				copy_cores += 1;
			}
			else if (copy_cores == number_copy_cores && ack_core_id == UINT_MAX) {
				//last core acks multiappend packets
				//rte_power_freq_max(lcore_id);
				printf("ack core id %u.\n", lcore_id);
				ack_core_id = lcore_id;
				//to_multiappend_ack
				ack_ring = rte_ring_create("to-ack", 256, rte_socket_id(),
						RING_F_SP_ENQ | RING_F_SC_DEQ); //TODO size
				if(ack_ring == NULL) {
					rte_exit(EXIT_FAILURE, "Cannot init to-ack ring memory "
						"pool due to:\n\t%s\n", rte_strerror(rte_errno));
				}
			}
			else { //TODO based on socket
				//rte_power_freq_min(lcore_id);
				printf("stats core id %u.\n", lcore_id);
				stats_core_id = lcore_id;
			}
			score_id += 1;
		}
		//assert(score_id == rte_lcore_count() - 1);
		assert(ack_core_id != UINT_MAX);
		assert(stats_core_id != UINT_MAX);
	}
	printf("num ccores %u\n", number_copy_cores);

	packet_pool = rte_pktmbuf_pool_create(_DELOS_MBUF_POOL, NUM_MBUFS,
				MBUF_CACHE_SIZE,
				0, //private data size
				DELOS_MBUF_DATA_SIZE,
				rte_eth_dev_socket_id(port));
				//SOCKET_ID_ANY);
				//rte_socket_id());
		if (packet_pool == NULL) {
			rte_exit(EXIT_FAILURE, "Cannot get memory pool for buffers due to %s\n", rte_strerror(rte_errno));
		}

	{
		struct rte_eth_dev_info info;
		int retval;
		struct ether_addr addr;
		const uint16_t rx_rings = 2, tx_rings = number_copy_cores + 1; //TODO
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

	{
		assert(ack_core_id > 0);
		RTE_LCORE_FOREACH_SLAVE(lcore_id) {
			if(lcore_id != ack_core_id && lcore_id != stats_core_id && lcore_id != rx_core1_id && lcore_id != rx_core2_id) {
				printf("starting copy core.\n");
				rte_eal_remote_launch(lcore_copy, NULL, lcore_id);
			}
			else if(lcore_id == rx_core1_id) {
				printf("starting rx core 1 core.\n");
				rte_eal_remote_launch(lcore_rx, NULL, lcore_id);
			}
			else if(lcore_id == rx_core2_id) {
				printf("starting rx core 2 core.\n");
				rte_eal_remote_launch(lcore_rx, NULL, lcore_id);
			}
			else if(lcore_id == ack_core_id)  {
				printf("starting ack core.\n");
				rte_eal_remote_launch(lcore_ack, NULL, lcore_id);
			}
			else if(lcore_id == stats_core_id) {
				printf("starting stats core.\n");
				rte_eal_remote_launch(lcore_stats, NULL, lcore_id);
			}
			else {
				printf("unused lcore %u.\n", lcore_id);
				assert(0 && "unused lcore");
			}
		}
	}
	classify(number_copy_cores);
	return 0;
}
