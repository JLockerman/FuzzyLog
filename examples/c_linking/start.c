#include <stdio.h>
#include <time.h>
#include <stdlib.h>

#include "fuzzy_log.h"

static ColorID interesting_colors[] = {2, 3, 4};
static ColorID interesting_colors2[] = {2};
static ColorID interesting_colors3[] = {3};
static ColorID interesting_colors4[] = {4};
static char *server_ips[] = { "127.0.0.1:13229" };

static char out[DELOS_MAX_DATA_SIZE];

int main()
{
	struct colors color = { .numcolors = 3, .mycolors = interesting_colors};
	struct DAGHandle* dag = new_dag_handle(1, (const char *const *)server_ips, &color);
	printf("fuzzy log client at %p\n", dag);

	uint32_t data = 1;
	color.mycolors = interesting_colors4;
	color.numcolors = 1;
	printf("sending %d to %d\n", data, color.mycolors[0]);
	append(dag, (char *)&data, sizeof(data), &color, NULL);
	data = 2;
	color.mycolors = interesting_colors2;
	color.numcolors = 1;
	printf("sending %d to %d\n", data, color.mycolors[0]);
	append(dag, (char *)&data, sizeof(data), &color, NULL);
	data = 3;
	color.mycolors = interesting_colors3;
	color.numcolors = 1;
	printf("sending %d to %d\n", data, color.mycolors[0]);
	append(dag, (char *)&data, sizeof(data), &color, NULL);

	srand(time(NULL));
	uint32_t end_key = rand();
	data = end_key;
	color.mycolors = interesting_colors;
	color.numcolors = 3;
	printf("sending %d\n", data);
	append(dag, (char *)&data, sizeof(data), &color, NULL);

	while (*(uint32_t *)out != end_key) {
		size_t size = 0;
		get_next(dag, out, &size, &color);
		printf("read %zu bytes = %d from", size, *(uint32_t *)out);
		for(size_t i = 0; i < color.numcolors; i++) {
			printf(" %d", color.mycolors[i]);
		}
		printf("\n");
	}
	return 0;
}
