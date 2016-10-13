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

//! Starts a fuzzy log server which listens at server_ip in the _current_ thread.
//!
//! @param server_ip
//!     a string of the form "<ip address>:<port>" at which the server should
//!     listen
//!
//! NOTE this function _never_ returns, as it immediately starts running the
//!   server
void start_fuzzy_log_server(const char * server_ip);

//! Just like start_fuzzy_log_server except it starts the server in a _new_ thread.
void start_fuzzy_log_server_thread(const char * server_ip);

//! Starts a fuzzy log server which is part of a group of N servers which
//! listens at server_ip in the _current_ thread.
//!
//! @param server_ip
//!     a string of the form "<ip address>:<port>" at which the server should
//!     listen
//!
//! @param server_number
//!     which server in [0, total_servers_in_group) this server is in it's group.
//!
//! @param total_servers_in_group
//!     The number of servers in the group that this server is a part of.
//!     Servers within the same group share the same chain address space and are
//!     managed by the same lock-server
//!
//! NOTE this function _never_ returns, as it immediately starts running the
//!   server
void start_fuzzy_log_server_for_group(const char * server_ip,
	 uint32_t server_number, uint32_t total_servers_in_group);

//! Just like start_fuzzy_log_server_for_group except it starts the server in a
//! _new_ thread.
void start_fuzzy_log_server_thread_from_group(const char * server_ip,
		uint32_t server_number, uint32_t total_servers_in_group);

static inline void start_fuzzy_log_servers(uint32_t num_servers,
	const char * server_ips[num_servers])
{
	for(uint32_t i = 0; i < num_servers; i++)
		start_fuzzy_log_server_thread_from_group(server_ips[i], i, num_servers);
}


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
