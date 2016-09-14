
# all source are stored in SRCS-y
C_SRC := start.c

LINK_FLAGS += -L./target/release -lc_link -pthread

CFLAGS += -O2 -std=gnu11
WFLAGS += -Wall -Wextra -Wpointer-arith -Wundef -Wcast-align -Wformat=2 -Wnested-externs

.PHONY: all rust_code clean mostlyclean rust_clean new_dir

all: rust_code
	@mkdir -p ./out
	$(CC) start.c -o ./out/start $(CFLAGS) $(WFLAGS) $(LINK_FLAGS)

rust_code:
	cargo build --release

clean: rust_clean mostlyclean

mostlyclean:
	rm -r ./out

rust_clean:
	cargo clean

DONT_COPY = target out Cargo.lock

new_dir:
ifeq ($(strip $(DIR_NAME)),)
	@echo "DIR_NAME must be non-empty"
else
	@echo "Setting up in $(abspath $(DIR_NAME))"
	@mkdir -p $(abspath $(DIR_NAME))
	@cp -R $(filter-out $(DONT_COPY), $(wildcard *)) $(abspath $(DIR_NAME))
	@echo "\tfuzzylog @ $(abspath ../..)"
	@sed -i .orig "s/^fuzzy_log.*$\/fuzzy_log={path=\"$(subst /,\/,$(abspath ../..))\"}/" $(DIR_NAME)/Cargo.toml
	@rm $(DIR_NAME)/Cargo.toml.orig
	@echo done
endif
