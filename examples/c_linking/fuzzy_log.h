
//! This file contains C bindings to the fuzzy log client and server.

#ifndef FUZZY_LOG_HEADER
#define FUZZY_LOG_HEADER 1

#include <stdint.h>

#define DELOS_MAX_DATA_SIZE 8000
typedef uint32_t ColorID;
typedef uint32_t LocationInColor;

struct colors
{
	//! number of entries in mycolors
	size_t numcolors;
	//! pointer to ColorID[numcolors]
	ColorID *mycolors;
};

//! The fuzzy log client
typedef struct DAGHandle DAGHandle;

typedef struct write_id {
	uint64_t p1;
	uint64_t p2;
} write_id;

typedef struct fuzzy_log_location {
	ColorID color;
	LocationInColor entry;
} fuzzy_log_location;

typedef struct write_locations {
	size_t num_locs;
	fuzzy_log_location *locs;
} write_locations;

typedef struct write_id_and_locs {
	write_id id;
	write_locations locations;
} write_id_and_locs;

static write_id WRITE_ID_NIL = {.p1 = 0, .p2 = 0};

//! @deprecated it is recommended that new_dag_handle_with_skeens()
//!             or new_dag_handle_with_replication() be used instead
//!
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

//! Creates a new DAGHandle for a server group
//! which uses the skeens based multiappend protocol.
//!
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
DAGHandle *new_dag_handle_with_skeens(
	size_t num_chain_servers, const char * const* chain_server_ips,
	struct colors *interesting_colors
);

//! Creates a new DAGHandle for a server group based on a config file.
DAGHandle *new_dag_handle_from_config(const char *config_filename,
	struct colors *interesting_colors);

//! Creates a new DAGHandle for a replicated server group
//! which uses the skeens based multiappend protocol.
//!
//! @param num_chain_servers
//!     The number of chain servers in the server group.
//!
//! @param chain_server_head_ips
//!     The IP address of the head of every replication-chain in the server group.
//!     NOTE Currently the ordering of the IP addresses must be the same as the
//!          ordering of the servers.
//!     NOTE the ordering of the replication-chains must be the same in both this
//!          and chain_server_tail_ips
//!
//! @param chain_server_tail_ips
//!     The IP address of the tail of every replication-chain in the server group.
//!     NOTE Currently the ordering of the IP addresses must be the same as the
//!          ordering of the servers.
//!     NOTE the ordering of the replication-chains must be the same in both this
//!          and chain_server_tail_ips
//!
//!
//! @param interesting_colors
//!     The colors this DAGHandle is interested in reading.
//!
DAGHandle *new_dag_handle_with_replication(
        size_t num_chain_servers,
        const char * const* chain_server_head_ips,
        const char * const* chain_server_tail_ips,
        struct colors *interesting_colors
);

write_id do_append(DAGHandle *handle, char *data, size_t data_size,
	struct colors* inhabits, struct colors* depends_on, uint8_t async);


//! Appends a new node to the dag, and waits for it to complete.
//!
//! @warning This function will flush some pending async appends.
//!          If you wish to wait for a specific write_id call wait_for_append()
//!          before this function.
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
static inline uint32_t append(DAGHandle *handle, char *data, size_t data_size,
	struct colors* inhabits, struct colors* depends_on) {
	do_append(handle, data, data_size, inhabits, depends_on, 0);
	return 0;
}

//! Asynchronously appends a new node to the dag.
//! This functions returns immediately with a write_id which uniquely identifies
//! the write. The id can be used with wait_for_append() to wait for this
//! specific write to be finished.
//! Also provided are wait_for_all_appends() which waits for all outstanding
//! appends to be finished and wait_for_an_append() which will wait until any
//! append finishes.
//!
//! Additionally there is flush_completed_appends().
//! In the event that a large number of concurrent appends are sent a large
//! amount of temporary allocations get performed.
//! If the exact point at which the various appends complete is irrelevant,
//! flush_completed_appends() can be used to remove these temporary allocations.
//!
//! \warning No guarantees are made on ordering of concurrent appends.
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
//! @return an id which uniquely identifies the write
//!
//NOTE currently can only use 31bits of return value
static inline write_id async_append(DAGHandle *handle, char *data, size_t data_size,
	struct colors* inhabits, struct colors* depends_on) {
	return do_append(handle, data, data_size, inhabits, depends_on, 1);
}

//! Atomically appends new node(s) to the dag.
//! This functions differs from append() in that,
//! if an append inhabits multiple colors,
//! if one of the entries is read the others need not be read.
//! In append() reading on part of a multi-color append forces the client to read
//! all parts of said append.
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
//! @param deps
//!     Specific entries in the log which this append must happen after.
//!
//! @return The location in the log the append was written to.
//!
fuzzy_log_location no_remote_append(DAGHandle *handle, char *data,
	size_t data_size, struct colors* inhabits, fuzzy_log_location *deps,
	size_t num_deps);

//! An asynchronous version of no_remote_append().
//! Is to no_remote_append() what async_append() is to append().
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
//! @param deps
//!     Specific entries in the log which this append must happen after.
//!
//! @return An id which uniquely identifies the write.
//!
write_id async_no_remote_append(DAGHandle *handle, char *data,
	size_t data_size, struct colors* inhabits, fuzzy_log_location *deps,
	size_t num_deps);


//! An async append which allows for happens-after edges.
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
//! @param happens_after
//!     Specific entries in the log which this append must happen after.
//!
//! @param num_happens_after
//!		The number of entries this happens after.
//!
//! @return An id which uniquely identifies the write.
//!
write_id async_causal_append(
	DAGHandle *handle,
	char *data, size_t data_size,
	struct colors* inhabits,
	struct colors* depends_on,
	fuzzy_log_location *happens_after, size_t num_happens_after);

//! An async append which allows for nodes which happens-after the seen entries
//! of a color
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
//! @param happens_after
//!     The colors which the new node should causally happen-after. May be empty.
//!
//! @return An id which uniquely identifies the write.
//!
write_id async_simple_causal_append(
	DAGHandle *handle,
	char *data, size_t data_size,
	struct colors* inhabits,
	struct colors* happens_after);

//! Reads a valid next node from the new nodes discovered with the latests
//! snapshot. If there are no such nodes (i.e. all new nodes have been read)
//! data_read and inhabits_out->numcolors will be set to 0.
//!
//! @warning this function assumes at most DELOS_MAX_DATA_SIZE bytes are returned
//!          if you might return more than DELOS_MAX_DATA_SIZE bytes use
//!          get_next2() instead
//!
//! @param handle
//!     The DAGHandle being worked through.
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
void get_next(DAGHandle *handle, char *data_out, size_t *data_read, struct colors* inhabits_out);

typedef struct get_next_val {
	const uint8_t *data;
	const fuzzy_log_location *locs;
} get_next_val;

//! Reads a valid next node from the new nodes discovered with the latests
//! snapshot. If there are no such nodes (i.e. all new nodes have been read)
//! locs_read will be set to 0.
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
//! @param[out] data_size
//!     The size in bytes of the data that was read.
//!
//! @param[out] locs_read
//!     The number of locations that the entry inhabited.
//!
//! @return
//!     Pointers to the data and locations that were read
//!     @warning do _not_ write to or free the data return by this function;
//!              they are maintained by the dag handle.
//!
get_next_val get_next2(DAGHandle *handle, size_t *data_size, size_t *locs_read);


//! Tries to read a valid next node from the new nodes discovered with the latests
//! snapshot.
//! If there are no nodes ready data_read and locs_read will be set to 0
//! If there are no such nodes remaining (i.e. all new nodes have been read)
//! additionally the pointers in get_next_val will be NULL
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
//! @param[out] data_size
//!     The size in bytes of the data that was read.
//!
//! @param[out] locs_read
//!     The number of locations that the entry inhabited.
//!
//! @return
//!     Pointers to the data and locations that were read,
//!     or null if there are no nodes remaining in the snapshot
//!     @warning do _not_ write to or free the data return by this function;
//!              they are maintained by the dag handle.
//!
get_next_val async_get_next2(DAGHandle *handle, size_t *data_size, size_t *locs_read);

//! Flush all appends that have already been completed.
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
void flush_completed_appends(DAGHandle *handle);

//! Waits for all outstanding appends to be ACK'd.
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
void wait_for_all_appends(DAGHandle *handle);

//! Waits for any append to be ACK'd, or returns immediately if there are none
//! in-flight
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
//! @return the write_id of the write that completed,
//!         WRITE_ID_NIL if there were no pending writes
//!
write_id wait_for_any_append(DAGHandle *handle);

//! Checks if there are any ACK'd appends and if so returns the first ones id
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
//! @return the write_id of the write that completed,
//!         WRITE_ID_NIL if there were no completed writes
//!
write_id try_wait_for_any_append(DAGHandle *handle);

//! Waits for an append with a specified id to be ACK'd,
//! or for no appends to be in-flight, whichever comes first.
//!
//! @param handle
//!     The DAGHandle being worked through.
//!
void wait_for_a_specific_append(DAGHandle *handle, write_id id);

//! Waits for an append with a specified id to be ACK'd,
//! returning the location(s) to which it was appended.
//!
//! @return the write_locations of the write that completed,
//!         note that field locs is malloc'd and should be free'd
//!
write_locations wait_for_a_specific_append_and_locations(DAGHandle *handle, write_id id);

//! Checks if there are any ACK'd appends,
//! and if so returns its write_id and the location(s) to which it was appended,
//! or .write_id == WRITE_ID_NIL if the append has not yet finished.
//!
//! @return the write_locations of the write that completed,
//!         or {.write_id = WRITE_ID_NIL, ...}
//!         note that iff write_id is non nil
//!         the field locs is malloc'd and should be free'd
//!         if there was a non-recoverable IO error
//!         {.write_id = WRITE_ID_NIL, .write_location = <server which had the error>}
//!
write_id_and_locs try_wait_for_any_append_and_location(DAGHandle *handle);

//! If there is no unread updates attempts to take a snapshot of the interesting
//! colors
//!
void snapshot(DAGHandle *handle);

//! Take a snapshot of the supplied colors
//!
//! @param colors the colors to be snapshotted
//!
void snapshot_colors(DAGHandle *handle, struct colors *colors);


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
	const char ** server_ips)
{
	for(uint32_t i = 0; i < num_servers; i++)
		start_fuzzy_log_server_thread_from_group(server_ips[i], i, num_servers);
}

//! Start a number of fuzzy log server threads based on a config file.
//! NOTE this function _does_ return after all servers start.
void start_servers_from_config(const char *config_filename);

////////////////////////////////////
//    Old fuzzy log C bindings    //
////////////////////////////////////

// These are all deprecated and should not be used

struct FuzzyLog;

typedef struct ChainAndEntry {
	uint32_t chain;
	uint32_t entry;
} ChainAndEntry;

typedef uint8_t (*fuzzy_log_callback)(const uint8_t *, uint16_t);

//! @deprecated
struct FuzzyLog *fuzzy_log_new(const char * server_addr,
		const uint32_t *relevent_chains, uint16_t num_relevent_chains, fuzzy_log_callback callback);

//! @deprecated
ChainAndEntry fuzzy_log_append(struct FuzzyLog *log, uint32_t chain,
		const uint8_t *val, uint16_t len,
		const ChainAndEntry* deps, uint16_t num_deps);

//! @deprecated
void fuzzy_log_multiappend(struct FuzzyLog *log, uint32_t *chain, uint16_t num_chains,
		const uint8_t *val, uint16_t len,
		const ChainAndEntry* deps, uint16_t num_deps);

//! @deprecated
ChainAndEntry fuzzy_log_play_forward(struct FuzzyLog *log, uint32_t *chain);

#endif
