
# all source are stored in SRCS-y
C_SRC := start.c

LINK_FLAGS += -L./target/release -lc_link -pthread

CFLAGS += -O2 -std=gnu11
WFLAGS += -Wall -Wextra -Wpointer-arith -Wundef -Wcast-align -Wformat=2 -Wnested-externs

.PHONY: cargo_build

all: cargo_build
	$(CC) start.c -o start $(CFLAGS) $(WFLAGS) $(LINK_FLAGS)

cargo_build:
	cargo build --release
