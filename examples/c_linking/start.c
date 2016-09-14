#include "stdio.h"

#include "fuzzy_log.h"

uint8_t callback(const uint8_t * _arg1, uint16_t _arg2)
{
	return 0;
}

int main(int argc, char *argv[])
{
	struct FuzzyLog* log = fuzzy_log_new(0, 3666, 0, 0, callback);
	printf("fuzzy log client at %p\n", log);
	return 0;
}
