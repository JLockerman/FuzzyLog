#ifndef FUZZY_LOG_HEADER
#define FUZZY_LOG_HEADER 1

#include <stdint.h>


//// Colour API sketch
//TODO where does the best effort flag go?
#define DELOS_MAX_DATA_SIZE 8000
typedef uint32_t ColorID;

struct colors
{
	//! number of entries in mycolors
	size_t numcolors;
	//! pointer to ColorID[numcolors]
	ColorID *mycolors;
};

typedef struct DAGHandle DAGHandle;

//! Creates a new DAGHandle for a server group.
//!
//! @param lock_server_ip
//!     The IP address of the lock server used by the server group. Should be
//!     `NULL` if the group consists of only one server.
//!     NOTE The lock server may _not_ be used as a chain server.
//!
//! @param num_chain_servers
//!     The number of chain servers in the server group.
//!
//! @param chain_server_ips
//!     The IP address of every chain server in the server group.
//!     NOTE Currently the ordering of the IP addresses must be the same as the
//!     ordering of the servers.
//!
//! @param interesting_colors
//!     The colors this DAGHandle is interested in reading.
//!
DAGHandle *new_dag_handle(const char * lock_server_ip,
	size_t num_chain_servers, const char * const* chain_server_ips,
	struct colors *interesting_colors);

static inline DAGHandle *new_dag_handle_for_single_server(const char *chain_server_ip,
	struct colors *interesting_colors)
{
	const char *chain_server_ips[] = { chain_server_ip };
	return new_dag_handle(NULL, 1, chain_server_ips, interesting_colors);
}


//! Appends a new node to the dag.
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
//! @param data
//!     The data contained within the node.
//!
//! @param data_size
//!     The size, in bytes, of data.
//!
//! @param inhabits
//!     The colors which the new node shall be colored with. Must be non-empty.
//!
//! @param depends_on
//!     The colors which the new node should happen-after. May be empty.
//!
//NOTE currently can only use 31bits of return value
uint32_t append(DAGHandle *handle, char *data, size_t data_size,
	struct colors* inhabits, struct colors* depends_on);

//! Reads a valid next node from the new nodes discovered with the latests
//! snapshot. If there are no such nodes (i.e. all new nodes have been read)
//! data_read and inhabits_out->numcolors will be set to 0.
//!
//! @param handle
//!     The DAGHandle being worked through,
//!
//! @param[out] data_out
//!     The size in bytes of the data that was read.
//!
//! @param[out] data_read
//!     The data that was read.
//!
//! @param[out] inhabits_out
//!     A pointer to a `struct colors` which will be populated with the colors
//!     inhabited by the node just read.
//!     If a node was read inhabits_out->numcolors will contain the number of
//!     colors said node inhabits and inhabits_out->mycolors will be a pointer
//!     to a _new_ array containing said colors ColorID. This array should be
//!     freed with `free`
//!     If no new nodes remain, inhabits_out-numcolors will be 0 and
//!     inhabits_out->mycolors will be in an undefined state and
//!     _must not be read_.
//!
//NOTE we need either a way to specify data size, or to pass out a pointer
// this version simple assumes that no data+metadat passed in or out will be
// greater than DELOS_MAX_DATA_SIZE
uint32_t get_next(DAGHandle *handle, char *data_out, size_t *data_read, struct colors* inhabits_out);


//! If there is no unread updates attempts to take a snapshot of the interesting
//! colors
//!
// NOTE currently a nop if there are buffered nodes waiting to be read,
//  eventually this will change to start prefetching even if there are still
//  unread data
void snapshot(DAGHandle *handle);


//! Closes and frees a DAGHandle
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
//!
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
//!
void start_fuzzy_log_server_for_group(const char * server_ip,
	 uint32_t server_number, uint32_t total_servers_in_group);

//! Just like start_fuzzy_log_server_for_group except it starts the server in a
//! _new_ thread.
//!
void start_fuzzy_log_server_thread_from_group(const char * server_ip,
		uint32_t server_number, uint32_t total_servers_in_group);


static inline void start_fuzzy_log_servers(uint32_t num_servers,
	const char * const server_ips[num_servers])
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
