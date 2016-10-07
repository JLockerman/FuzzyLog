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
// sets data_read to 0 and colors->numcolors to 0 when out of data for the
// current snapshot
uint32_t get_next(DAGHandle *handle, char *data_out, size_t *data_read, struct colors* inhabits_out);

// If there is no unread updates attempts to take a snapshot of the interesting
// colors returning 0 if after checking all interesting colors it cannot find
// one which has unread data
// TODO is it better to just force the client to check get next after each
// snapshot for now?
void snapshot(DAGHandle *handle);

//NOTE also frees the *handle
void close_dag_handle(DAGHandle *handle);

////////////////////////////////////
//         Server Bindings        //
////////////////////////////////////

//Starts a fuzzy log server in the _current_ thread.
//NOTE this function _never_ returns
void start_fuzzy_log_server(const char * server_ip);

//Starts a fuzzy log server in a new thread.
void start_fuzzy_log_server_thread(const char * server_ip);

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
