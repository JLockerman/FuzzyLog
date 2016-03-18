
#ifndef DELOS_TRIE_HEADER
#define DELOS_TRIE_HEADER

#include <assert.h>
#include <stdint.h>

#define DELOS_TRIE_ROOT_SHIFT 30
#define DELOS_TRIE_MASK ((8192 / 8) - 1)

typedef uint8_t *ValEdge;
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
	uint8_t *alloc;
	ptrdiff_t alloc_rem;
} DelosTrieRootTable;

_Static_assert(sizeof(ptrdiff_t) == sizeof(uint64_t), "bad ptrdiff size");

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
	assert(DELOS_MBUF_DATA_SIZE == 8192);
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
	assert(DELOS_MBUF_DATA_SIZE == 8192);
	//printf("alloc done.\n");
	rte_prefetch0(obj);
	return obj;
}

DelosTrieAppendRet
cappend_size(DelosTrieRootTable **, const void *, ptrdiff_t);

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

#endif
