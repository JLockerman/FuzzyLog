#include <stdio.h>
#include <stdlib.h>

/*
 * The FuzzyLog client library header provides an intuitive C interface to the
 * fuzzy log
 */
#include "fuzzylog.h"

static void print_events(void *state, const char *data, uintptr_t data_size);
static void count_events(void *state, const char *data, uintptr_t data_size);

extern void start_fuzzy_log_server_thread(char *);

static char *local_server_ip = "127.0.0.1:13229";

int
main(int argc, char *argv[argc])
{
	ServerSpec servers = { .num_ips = 1, servers.head_ips = &local_server_ip};
	if(argc == 1) {
		/*
		 * This isn't part of the FuzzyLog client, but we start a FuzzyLog server
		 * on another thread to make this code easy to run
		 */
		printf("(Running against local server)\n\n");
		start_fuzzy_log_server_thread("0.0.0.0:13229");
	} else {
		servers.num_ips = argc - 1;
		servers.head_ips = &argv[1];
	}

	printf("First we start a FuzzyLog client... ");

	uint64_t chains[3] = {1ull, 3ull, 7ull};
	ColorSpec my_color = {.local_chain = 1, .remote_chains = chains, .num_remote_chains = 3};

	FLPtr log = new_fuzzylog_instance(servers, my_color, NULL);

	/* An example of basic FuzzyLog usage. */
	{
		printf("FuzzyLog client started @ 0x%p.\n\n", log);
		printf("Let's send some data\n");

		{
			uint32_t data = 11;
			printf("\tsending %d to my color\n", data);
			fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
		}
		{
			uint32_t data = 12;
			printf("\tsending %d to my color\n", data);
			fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
		}
		{
			uint32_t data = 13;
			printf("\tsending %d to my color\n", data);
			fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
		}

		printf("and now we sync\n");

		fuzzylog_sync(log, print_events, NULL);

		printf("the sync is done!\n");
		printf("When a single client writes to the log, everything is totally ordered.\n\n");
	}

	/*
	 * But just running on one log isn't that interesting.
	 * Let's add some dependencies. We'll need more clients for this.
	 */

	printf("Starting a second client, and syncing it with the log.\n");
	ColorSpec my_color2 = {.local_chain = 3, .remote_chains = chains, .num_remote_chains = 3};
	FLPtr second_log = new_fuzzylog_instance(servers, my_color2, NULL);

	printf("Second client started @ %p.\n\n", log);
	printf("Second client getting the first client's appends... ");
	uint32_t events_seen = 0;
	while(events_seen < 3) {
		fuzzylog_sync(second_log, count_events, &events_seen);
	}
	printf("done!\n\n");

	printf("Since the second client saw all of the first client's events\n");
	printf("all of these new events will be ordered after them.\n");
	{
		uint32_t data = 21;
		printf("\tsecond client sending %d to my color\n", data);
		fuzzylog_append(second_log, (char *)&data, sizeof(data), &my_color2, 1);
	}
	{
		uint32_t data = 22;
		printf("\tsending %d to my color\n", data);
		fuzzylog_append(second_log, (char *)&data, sizeof(data), &my_color2, 1);
	}
	{
		uint32_t data = 23;
		printf("\tsending %d to my color\n", data);
		fuzzylog_append(second_log, (char *)&data, sizeof(data), &my_color2, 1);
	}

	fuzzylog_close(second_log);

	printf("and clients will see them in that order.\n");
	{
		FLPtr reader = new_fuzzylog_instance(servers, my_color, NULL);
		for(events_seen = 0; events_seen < 6;) {
			fuzzylog_sync(reader, print_events, &events_seen);
		}
		fuzzylog_close(reader);
	}

	printf("However if the original client appends new events without syncing\n");
	{
		uint32_t data = 14;
		printf("\tfirst client sending %d to my color\n", data);
		fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
	}
	{
		uint32_t data = 15;
		printf("\tfirst client sending %d to my color\n", data);
		fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
	}
	{
		uint32_t data = 16;
		printf("\tfirst client sending %d to my color\n", data);
		fuzzylog_append(log, (char *)&data, sizeof(data), &my_color, 1);
	}

	printf("clients can see them in that order with respect to the second clients appends.\n");
	for(int i = 0; i < 1; i++) {
		FLPtr reader = new_fuzzylog_instance(servers, my_color, NULL);
		for(events_seen = 0; events_seen < 9;) {
			fuzzylog_sync(reader, print_events, &events_seen);
		}
		fuzzylog_close(reader);
	}

	fuzzylog_close(log);

	printf("\n");

	return 0;
}

static void
print_events(void *state, const char *data, uintptr_t data_size) {
	if(data_size != 4) {
		printf("\tUh oh, unexpected data.\n");
		exit(1);
	}

	if(state != NULL) *(uint32_t *)state += 1;

	printf("\tread %u from the log\n", *(uint32_t *)data);

}

static void
count_events(void *state, const char *data, uintptr_t data_size) {
	if(data_size != 4) {
		printf("\tUh oh, unexpected data.\n");
		exit(1);
	}

	*(uint32_t *)state += 1;
}

