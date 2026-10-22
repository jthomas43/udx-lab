CFLAGS = -g -Wall -Werror --sanitize=address

LIB_SRC := $(wildcard *.c)

TEST_SRC := $(wildcard test/*.c)
TEST_BIN := $(patsubst test/%.c,build/test/%,$(TEST_SRC))


# optimized and without asan, so the numbers mean something
PERF_CFLAGS = -O2 -g -Wall -Werror

qdxperf: build/qdxperf

tags: $(LIB_SRC) $(TEST_SRC) *.h
	ctags -R

build/qdxperf: programs/qdxperf.c $(LIB_SRC) *.h
	@mkdir -p $(@D)
	gcc $(PERF_CFLAGS) $(LIB_SRC) $< -o $@

build/test/%: test/%.c $(LIB_SRC) *.h test/*.h
	@mkdir -p $(@D)
	gcc $(CFLAGS) $(LIB_SRC) $< -o $@

# run every test, even after a failure, and fail if any did
test: $(TEST_BIN)
	@failed=0; \
	for t in $(TEST_BIN); do \
		echo "== $$t"; \
		./$$t || { echo "FAILED: $$t"; failed=1; }; \
	done; \
	exit $$failed

clean:
	rm -rf build

.PHONY: test clean qdxperf
