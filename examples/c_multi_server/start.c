#include <stdio.h>
#include <time.h>
#include <stdlib.h>

#include "../c_linking/fuzzy_log.h"


static char *lock_server_ip = "127.0.0.1:13270";
static char *chain_server_ips[] = { "127.0.0.1:13271", "127.0.0.1:13272",
	"127.0.0.1:13273"};

static char out[DELOS_MAX_DATA_SIZE];

int main()
{
	start_fuzzy_log_server_thread(lock_server_ip);
	start_fuzzy_log_servers(3, (const char *const *)chain_server_ips);

	struct colors color = { .numcolors = 3, .mycolors = (ColorID[]){2, 3, 4}};
	struct DAGHandle* dag = new_dag_handle(lock_server_ip,
		3, (const char *const *)chain_server_ips, &color);
	printf("fuzzy log client at %p.\n", dag);

	uint32_t data;
	{
		data = 401;
		color.mycolors = (ColorID[]){4, 2};
		color.numcolors = 1;
		printf("sending %d to %d.\n", data, color.mycolors[0]);
		append(dag, (char *)&data, sizeof(data), &color, NULL);
	}
	{
		data = 102;
		color.mycolors = (ColorID[]){2, 3};
		color.numcolors = 1;
		printf("sending %d to %d.\n", data, color.mycolors[0]);
		append(dag, (char *)&data, sizeof(data), &color, NULL);
	}
	{
		data = 733;
		color.mycolors = (ColorID[]){3, 4};;
		color.numcolors = 1;
		printf("sending %d to %d.\n", data, color.mycolors[0]);
		append(dag, (char *)&data, sizeof(data), &color, NULL);
	}
	uint32_t end_key;
	{
		srand(time(NULL));
		end_key = rand();
		data = end_key;
		color.mycolors = (ColorID[]){2, 3, 4};
		color.numcolors = 3;
		printf("sending %d to all.\n", data);
		append(dag, (char *)&data, sizeof(data), &color, NULL);
	}

	snapshot(dag);

	while (color.numcolors != 0) {
		size_t size = 0;
		get_next(dag, out, &size, &color);
		if (color.numcolors != 0) {
			printf("read %zu bytes = %d from", size, *(uint32_t *)out);
			for(size_t i = 0; i < color.numcolors; i++) {
				printf(" %d", color.mycolors[i]);
			}
			printf(".\n");
		}
		else {
			printf("finished reading.\n");
		}
	}

	close_dag_handle(dag);
	return 0;
}
