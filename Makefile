CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -Iinclude -pthread

TESTS = test_spsc_queue test_spmc_queue test_mutex_queue

all: $(TESTS)

test_spsc_queue: tests/test_spsc_queue.cpp include/spsc_queue.h
	$(CXX) $(CXXFLAGS) -o $@ $<

test_spmc_queue: tests/test_spmc_queue.cpp include/spmc_queue.h
	$(CXX) $(CXXFLAGS) -o $@ $<

test_mutex_queue: tests/test_mutex_queue.cpp include/mutex_queue.h
	$(CXX) $(CXXFLAGS) -o $@ $<

run: $(TESTS)
	@echo "Running tests..." > output.log
	@./test_spsc_queue >> output.log 2>&1 || true
	@./test_spmc_queue >> output.log 2>&1 || true
	@./test_mutex_queue >> output.log 2>&1 || true
	@cat output.log
	@echo "All tests executed. Results in output.log"

clean:
	rm -f $(TESTS) output.log
