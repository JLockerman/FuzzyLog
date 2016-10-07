
# all source are stored in SRCS-y
C_SRC := start.c

DELOS_RUST_LOC ?= ../..

LINK_FLAGS += -L$(DELOS_RUST_LOC)/examples/c_linking/target/release -lc_link -pthread

CFLAGS += -O2 -std=gnu11
WFLAGS += -Wall -Wextra -Wpointer-arith -Wundef -Wformat=2 -Wnested-externs

.PHONY: all rust_code clean mostlyclean rust_clean new_dir

all: $(DELOS_RUST_LOC)/examples/c_linking/target/release/libc_link.a
	@mkdir -p ./out
	$(CC) start.c -o ./out/start $(CFLAGS) $(WFLAGS) $(LINK_FLAGS)

rust_code: $(DELOS_RUST_LOC)/examples/c_linking/target/release/libc_link.a


$(DELOS_RUST_LOC)/examples/c_linking/target/release/libc_link.a:
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
