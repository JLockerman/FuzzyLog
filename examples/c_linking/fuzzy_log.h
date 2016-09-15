#ifndef FUZZY_LOG_HEADER
#define FUZZY_LOG_HEADER 1

#include <stdint.h>

struct FuzzyLog;

typedef struct ChainAndEntry {
	uint32_t chain;
	uint32_t entry;
} ChainAndEntry;

typedef uint8_t (*fuzzy_log_callback)(const uint8_t *, uint16_t);

struct FuzzyLog *fuzzy_log_new(const char * server_addr,
		const uint32_t *relevent_chains, uint16_t num_relevent_chains, fuzzy_log_callback callback);

ChainAndEntry fuzzy_log_append(struct FuzzyLog *log, uint32_t chain,
		const uint8_t *val, uint16_t len,
		const ChainAndEntry* deps, uint16_t num_deps);

void fuzzy_log_multiappend(struct FuzzyLog *log, uint32_t *chain, uint16_t num_chains,
		const uint8_t *val, uint16_t len,
		const ChainAndEntry* deps, uint16_t num_deps);

ChainAndEntry fuzzy_log_play_forward(struct FuzzyLog *log, uint32_t *chain);

#endif
