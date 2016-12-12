#USAGE
#
# make [all]        builds and links example, placing it in ./out/start
#
# make rust_code    builds the delos-rust library in ./target/release/
#
# make mostlyclean  removes artifacts (object files and binaries) generated from C code
# make rust_clean   removes artifacts generated from rust code
# make clean        removes all generated artifacts
#
# make new_dir DIR_NAME=<dir name>
#        Creates a new copy of this makefile and necessary code in directory <dir name>
#        such that the above commands will work in said directory
#        this relies on the enviroment variable DELOS_RUST_LOC being set to the
#        location of a local copy of the delos-rust repository.

C_SRC := start.c

DELOS_RUST_LOC ?= ../..

LINK_FLAGS += -L$(DELOS_RUST_LOC)/examples/c_linking/target/release -lc_link -lm -ldl -lrt -pthread -Wl,--gc-sections

CFLAGS += -O2 -std=gnu11
WFLAGS += -Wall -Wextra -Wpointer-arith -Wundef -Wformat=2 -Wnested-externs

.PHONY: all rust_code clean mostlyclean rust_clean new_dir

all: out/start

out/start: $(DELOS_RUST_LOC)/examples/c_linking/target/release/libc_link.a $(C_SRC)
	@mkdir -p ./out
	$(CC) start.c -o ./out/start $(CFLAGS) $(WFLAGS) $(LINK_FLAGS)

rust_code: $(DELOS_RUST_LOC)/examples/c_linking/target/release/libc_link.a


$(DELOS_RUST_LOC)/examples/c_linking/target/release/libc_link.a: $(DELOS_RUST_LOC)/src/*
	@echo "Building fuzzy log @ $(DELOS_RUST_LOC)/examples/c_linking/"
	@{ cd $(DELOS_RUST_LOC)/examples/c_linking/ && cargo build --release; } || \
	{ echo "\033[0;31mHave you set DELOS_RUST_LOC?\033[0m" && exit 1; }

clean: rust_clean mostlyclean

mostlyclean:
	rm -rf ./out

rust_clean:
	@echo "Cleaning $(DELOS_RUST_LOC)/examples/c_linking/"
	@cd $(DELOS_RUST_LOC)/examples/c_linking/ && cargo clean

DONT_COPY = target out Cargo.lock Cargo.toml src

new_dir:
ifeq ($(strip $(DIR_NAME)),)
	@echo "DIR_NAME must be non-empty"
else
	@echo "Setting up in $(abspath $(DIR_NAME))"
	@mkdir -p $(abspath $(DIR_NAME))
	@cp -R $(filter-out $(DONT_COPY), $(wildcard *)) $(abspath $(DIR_NAME))
	@echo "\tfuzzylog @ $(abspath ../..)"
ifeq ($(DELOS_RUST_LOC), )
	@echo "run\n\texport DELOS_RUST_LOC=$(abspath ../..)\nto enable linking."
endif
ifeq ($(DELOS_RUST_LOC), ../..)
	@echo "run\n\t\033[0;37mexport DELOS_RUST_LOC=$(abspath ../..)\033[0m\nto enable linking."
endif
endif
