
#ifndef DELOS_TRIE_HEADER
#define DELOS_TRIE_HEADER

#include <assert.h>
#include <stdint.h>

#define DELOS_TRIE_ROOT_SHIFT 30
#define DELOS_TRIE_MASK ((8192 / 8) - 1)

typedef void *ValEdge;
typedef ValEdge *L3Edge;
typedef L3Edge *L2Edge;
typedef L2Edge *L1Edge;
typedef L1Edge *RootEdge;

typedef struct {
	ValEdge *l3;
	L3Edge *l2;
	L2Edge *l1;
	L1Edge array[3];
	uint32_t next_entry;
	char *alloc;
	ptrdiff_t alloc_rem;
} DelosTrieRootTable;

_Static_assert(sizeof(ptrdiff_t) == sizeof(uint64_t), "bad ptrdiff size");

struct entry_pointer_and_index;

typedef struct {
	uint32_t loc;
	void *val;
} DelosTrieAppendRet;

static inline void *
ialloc_seg(void) {
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
	memset(obj, 0, DELOS_MBUF_DATA_SIZE);
	//printf("alloc done.\n");
	return obj;
}

static inline void *
iualloc_seg(void) {
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
	//printf("alloc done.\n");
	return obj;
}

static inline struct entry_pointer_and_index
c_next_entry(DelosTrieRootTable *restrict const root_ptr, ptrdiff_t size)
{
	//assert(8192 >= size);
	//assert(size > 0);
	DelosTrieRootTable *restrict const  root = root_ptr;
	//printf("chain @ %p\n", root);
	//printf("1: %li\n", size);
	char *val_loc;
	if(root->alloc_rem >= size) {
		//printf("1.1: %li\n", size);
		val_loc = root->alloc;
		root->alloc += size;
		root->alloc_rem -= size;
	} else {
		//printf("1.2: %li\n", size);
		val_loc = iualloc_seg();
		root->alloc = val_loc + size;
		root->alloc_rem = 8192 - size;
	}

	//rte_memcpy(val_loc, val, size);
	uint32_t next_entry = root->next_entry;
	//printf("2: %u\n", next_entry);
	if ((next_entry & 0x3FFFFFFF) == 0) {
		//printf("2.1: %u\n", next_entry);
		root->l1 = ialloc_seg();
		uint32_t index = (next_entry >> DELOS_TRIE_ROOT_SHIFT) & DELOS_TRIE_MASK;
		assert(index < 4);
		root->array[index] = root->l1;
	}
	if ((next_entry & 0xfffff) == 0) {
		//printf("2.2: %u\n", next_entry);
		root->l2 = ialloc_seg();
		*root->l1 = root->l2;
		root->l1 += 1;
	}
	if ((next_entry & 0x3ff) == 0) {
		//printf("2.3: %u\n", next_entry);
		root->l3 = ialloc_seg();
		*root->l2 = root->l3;
		root->l2 += 1;
	}
	ValEdge *entry = root->l3;
	//printf("entry %p\n", entry);
	root->l3 += 1;
	root->next_entry += 1;
	struct entry_pointer_and_index ret = {.entry = entry, .ptr = val_loc, .index = next_entry};
	return ret;
}

static inline DelosTrieRootTable *
c_log_init(void)
{
	return ialloc_seg();
}

#define ARRAY_SIZE (8192 / 8)
#define MASK (ARRAY_SIZE - 1)
#define SHIFT_LEN 10
#define ROOT_SHIFT 30

#define INDEX(chain, level) (((chain) >> (ROOT_SHIFT - (SHIFT_LEN * level))) & MASK)


static inline struct entry_pointer_and_index
c_get_next_loc(DelosTrieRootTable *restrict const root_ptr, ptrdiff_t size, uint32_t chain)
{
	DelosTrieRootTable *restrict const root = root_ptr;
	const uint32_t root_index = INDEX(chain & 0xffffffff, 0);
	const uint32_t l1_index = INDEX(chain, 1);
	const uint32_t l2_index = INDEX(chain, 2);
	const uint32_t l3_index = INDEX(chain, 3);
	L1Edge *const restrict l1 = &root->array[root_index];
	if(*l1 == NULL) {
		*l1 = ialloc_seg();
		//goto fillin_1;
	}
	L2Edge *const restrict l2 = &(*l1)[l1_index];
	if(unlikely(*l2 == NULL)) {
		*l2 = ialloc_seg();
		//goto fillin_2;
	}
	L3Edge *const restrict l3 = &(*l2)[l2_index];
	if(unlikely(*l3 == NULL)) {
		*l3 = ialloc_seg();
		//goto fillin_3;
	}
	ValEdge *const restrict val = &(*l3)[l3_index];
	if(unlikely(*val == NULL)) {
		*val = ialloc_seg();
		c_next_entry(*val, 0);
		assert(((DelosTrieRootTable *)*val)->array[0] != NULL);
		//printf("new chain %u\n", chain);
	}
	return c_next_entry(*val, size);
}

static inline void *
trie_get_entry(DelosTrieRootTable *const restrict root_ptr, uint32_t index)
{
	const uint32_t root_index = INDEX(index & 0xffffffff, 0);
	const uint32_t l1_index = INDEX(index, 1);
	const uint32_t l2_index = INDEX(index, 2);
	const uint32_t l3_index = INDEX(index, 3);
	L1Edge *const restrict l1 = &root_ptr->array[root_index];
	if(*l1 == NULL) {
		//printf("a %u, r %u, 1 %u, 2 %u, 3 %u\n", index, root_index, l1_index, l2_index, l3_index);
		return NULL;
	}
	L2Edge *const restrict l2 = &(*l1)[l1_index];
	if(unlikely(*l2 == NULL)) {
		//printf("b\n");
		return NULL;
	}
	L3Edge *const restrict l3 = &(*l2)[l2_index];
	if(unlikely(*l3 == NULL)) {
		//printf("c\n");
		return NULL;
	}
	ValEdge *const restrict val = &(*l3)[l3_index];
	if(unlikely(*val == NULL)) {
		//printf("d %p\n", val);
		return NULL;
	}
	return *val;
}

static inline void *
c_get_val(DelosTrieRootTable *restrict root_ptr, uint32_t chain, uint32_t index)
{
	DelosTrieRootTable *const restrict chain_trie = trie_get_entry(root_ptr, chain);
	if(chain_trie == NULL) return NULL;
	//printf("get chain @ %p\n", chain_trie);
	return trie_get_entry(chain_trie, index);
}

/*
DelosTrieAppendRet
c_append_size(DelosTrieRootTable **, const void *, ptrdiff_t);

DelosTrieAppendRet
cappend_size(DelosTrieRootTable **root_ptr, const void *val, ptrdiff_t size) {
	assert(8192 >= size);
	assert(size > 0);
	if((*root_ptr) == NULL) {
		*root_ptr = ialloc_seg();
	}
	DelosTrieRootTable *root = *root_ptr;
	uint8_t *val_loc;
	if(root->alloc_rem >= size) {
		val_loc = root->alloc;
		root->alloc += size;
		root->alloc_rem -= size;
	} else {
		val_loc = ialloc_seg();
		root->alloc = val_loc + size;
		root->alloc_rem = 8192 - size;
	}

	rte_memcpy(val_loc, val, size);
	uint32_t next_entry = root->next_entry;
	if ((next_entry & 0x3FFFFFFF) == 0) {
		root->l1 = ialloc_seg();
		uint32_t index = (next_entry >> DELOS_TRIE_ROOT_SHIFT) & DELOS_TRIE_MASK;
		assert(index < 4);
		root->array[index] = root->l1;
	}
	if ((next_entry & 0xfffff) == 0) {
		root->l2 = ialloc_seg();
		*root->l1 = root->l2;
		root->l1 += 1;
	}
	if ((next_entry & 0x3ff) == 0) {
		root->l3 = ialloc_seg();
		*root->l2 = root->l3;
		root->l2 += 1;
	}
	*root->l3 = val_loc;
	root->l3 += 1;
	root->next_entry += 1;
	DelosTrieAppendRet ret = {.loc = next_entry, .val = val_loc};
	return ret;
}
*/
#endif
