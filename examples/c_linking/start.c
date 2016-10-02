#include <stdio.h>
#include <time.h>
#include <stdlib.h>

#include "fuzzy_log.h"

static char *server_ips[] = { "127.0.0.1:13229" };

static char out[DELOS_MAX_DATA_SIZE];

int main()
{
	start_fuzzy_log_server_thread("0.0.0.0:13229");

	struct colors color = { .numcolors = 3, .mycolors = (ColorID[]){2, 3, 4}};
	struct DAGHandle* dag = new_dag_handle(1, (const char *const *)server_ips, &color);
	printf("fuzzy log client at %p.\n", dag);

	uint32_t data;
	{
		data = 401;
		color.mycolors = (ColorID[]){4};
		color.numcolors = 1;
		printf("sending %d to %d.\n", data, color.mycolors[0]);
		append(dag, (char *)&data, sizeof(data), &color, NULL);
	}
	{
		data = 102;
		color.mycolors = (ColorID[]){2};
		color.numcolors = 1;
		printf("sending %d to %d.\n", data, color.mycolors[0]);
		append(dag, (char *)&data, sizeof(data), &color, NULL);
	}
	{
		data = 733;
		color.mycolors = (ColorID[]){3};;
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
