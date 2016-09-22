#ifndef FUZZY_LOG_HEADER
#define FUZZY_LOG_HEADER 1

#include <stdint.h>


//// Colour API sketch
//TODO where does the best effort flag go?
#define DELOS_MAX_DATA_SIZE 8000
typedef uint32_t ColorID;

struct colors
{
	size_t numcolors;
	ColorID *mycolors;
};

typedef struct DAGHandle DAGHandle;

DAGHandle *new_dag_handle(size_t num_ips, const char * const* server_ips,
	struct colors *interesting_colors);

//NOTE currently can only use 31bits of return value
uint32_t append(DAGHandle *handle, char *data, size_t data_size,
	struct colors* inhabits, struct colors* depends_on);

//NOTE we need either a way to specify data size, or to pass out a pointer
// this version simple assumes that no data+metadat passed in or out will be
// greater than DELOS_MAX_DATA_SIZE
uint32_t get_next(DAGHandle *handle, char *data_out, size_t *data_read, struct colors* inhabits_out);

//NOTE also frees the *handle
void close_dag_handle(DAGHandle *handle);

////////////////////////////////////
//    Old fuzzy log C bindings    //
////////////////////////////////////

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
