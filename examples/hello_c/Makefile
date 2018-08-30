#USAGE
#
# make [all]        builds and links example, placing it in ./out/hello
#
# make rust_code    builds the FuzzyLog library in $(FUZZYLOG_SRC_LOC)/target/debug/
#
# make mostlyclean  removes artifacts (object files and binaries) generated from C code
# make rust_clean   removes artifacts generated from rust code
# make clean        removes all generated artifacts
#
# make new_dir DIR_NAME=<dir name>
#        Creates a new copy of this makefile and necessary code in directory <dir name>
#        such that the above commands will work in said directory
#        this relies on the environment variable FUZZYLOG_SRC_LOC being set to the
#        location of a local copy of the FuzzyLog repository.

C_SRC := main.c

FUZZYLOG_SRC_LOC ?= $(realpath ../..)

OS_NAME := $(shell uname)

LINK_FLAGS += -L$(FUZZYLOG_SRC_LOC)/target/debug -lfuzzy_log

ifeq ($(OS_NAME), Linux)
	LINK_FLAGS += -lm -ldl -lrt -pthread -Wl,--gc-sections
else ifeq ($(UNAME_S),Darwin)
    LINK_FLAGS += -lsystem -lc -lm -pthread
endif

CFLAGS += -O2 -std=gnu11 -I$(FUZZYLOG_SRC_LOC)
WFLAGS += -Wall -Wextra -Wpointer-arith -Wundef -Wformat=2 -Wnested-externs -Wno-unused-parameter

.PHONY: all rust_code clean mostlyclean rust_clean new_dir

all: out/hello

out/hello: rust_code $(C_SRC)
	@mkdir -p ./out
	$(CC) main.c -o ./out/hello $(CFLAGS) $(WFLAGS) $(LINK_FLAGS)

rust_code: $(FUZZYLOG_SRC_LOC)/target/debug/libc_link.a


$(FUZZYLOG_SRC_LOC)/target/debug/libc_link.a: $(shell find $(FUZZYLOG_SRC_LOC)/src/ -type f)
	@echo "Building fuzzy log @ $(FUZZYLOG_SRC_LOC)"
	@{ cd $(FUZZYLOG_SRC_LOC) && cargo build; } || \
	{ echo "\033[0;31mHave you set FUZZYLOG_SRC_LOC?\033[0m" && exit 1; }

clean: rust_clean mostlyclean

mostlyclean:
	rm -rf ./out

rust_clean:
	@echo "Cleaning $(FUZZYLOG_SRC_LOC)"
	@cd $(FUZZYLOG_SRC_LOC) && cargo clean

DONT_COPY = target out Cargo.lock Cargo.toml src

new_dir:
ifeq ($(strip $(DIR_NAME)),)
	@echo "DIR_NAME must be non-empty"
else
	@echo "Setting up in $(abspath $(DIR_NAME))"
	@mkdir -p $(abspath $(DIR_NAME))
	@cp -R $(filter-out $(DONT_COPY), $(wildcard *)) $(abspath $(DIR_NAME))
	@echo "\tfuzzylog @ $(abspath ../..)"
ifeq ($(FUZZYLOG_SRC_LOC), )
	@echo "run\n\texport FUZZYLOG_SRC_LOC=$(abspath ../..)\nto enable linking."
endif
ifeq ($(FUZZYLOG_SRC_LOC), ../..)
	@echo "run\n\t\033[0;37mexport FUZZYLOG_SRC_LOC=$(abspath ../..)\033[0m\nto enable linking."
endif
endif
