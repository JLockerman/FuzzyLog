#include <stdio.h>
#include <stdlib.h>

/*
 * The FuzzyLog client library header provides an intuitive C interface to the
 * fuzzy log
 */
#include "fuzzylog.h"

static void print_event_callback(void *state, const char *data, uintptr_t data_size);

extern void start_fuzzy_log_server_thread(char *);

static char *local_server_ip = "127.0.0.1:13229";

int main(int argc, char *argv[argc])
{
	ServerSpec servers = { .num_ips = 1, servers.head_ips = &local_server_ip};
	if(argc == 1) {
		/*
		 * This isn't part of the FuzzyLog client, but we start a FuzzyLog server
		 * on another thread to make this code easy to run
		 */
		printf("Running against local server.\n");
		start_fuzzy_log_server_thread("0.0.0.0:13229");
	} else {
		servers.num_ips = argc - 1;
		servers.head_ips = &argv[1];
	}

	printf("First we start a FuzzyLog client\n");

	uint64_t chains[3] = {1ull, 2ull, 3ull};

	ColorSpec my_color = {.local_chain = 1, .remote_chains = chains, .num_remote_chains = 3};

	FLPtr log = new_fuzzylog_instance(servers, my_color, NULL);

	printf("FuzzyLog client started @ 0x%p.\n", log);
	printf("Let's send some data\n");

	{
		uint32_t data = 401;
		printf("\tsending %d to my_color\n", data);
		fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
	}
	{
		uint32_t data = 102;
		printf("\tsending %d to my_color\n", data);
		fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
	}
	{
		uint32_t data = 733;
		printf("\tsending %d to my_color\n", data);
		fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
	}

	printf("and now we sync\n");

	fuzzylog_sync(log, print_event_callback, NULL);

	fuzzylog_close(log);

	printf("and we're done.\n");

	return 0;
}

static void print_event_callback(void *state, const char *data, uintptr_t data_size) {
	if(data_size != 4) {
		printf("\tUh oh, unexpected data.\n");
		return;
	}

	printf("\tread %u from the log\n", *(uint32_t *)data);
}
