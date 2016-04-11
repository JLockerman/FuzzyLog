
# all source are stored in SRCS-y
C_SRC := start.c

LINK_FLAGS += -L$(RTE_SRCDIR)/target/release -ldpdk_bench

CFLAGS += #TODO

PREBUILD += cargo_build

all:
	cargo_build
	$(CC) CFLAGS LINK_FLAGS #etc

cargo_build:
	cargo build --release
