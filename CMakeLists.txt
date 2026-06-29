cmake_minimum_required(VERSION 3.14)
project(lockfree_queues LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler warnings
add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -Wshadow)

# Release-with-debug-info by default (good for benchmarking)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

# Header-only queue library
add_library(queues INTERFACE)
target_include_directories(queues INTERFACE ${CMAKE_SOURCE_DIR}/include)

# Threading
find_package(Threads REQUIRED)

# ─── Tests ───────────────────────────────────────────────────────────────────

add_executable(test_mutex_queue tests/test_mutex_queue.cpp)
target_link_libraries(test_mutex_queue PRIVATE queues Threads::Threads)

add_executable(test_spsc_queue tests/test_spsc_queue.cpp)
target_link_libraries(test_spsc_queue PRIVATE queues Threads::Threads)

add_executable(test_spmc_queue tests/test_spmc_queue.cpp)
target_link_libraries(test_spmc_queue PRIVATE queues Threads::Threads)

# ─── Benchmark ───────────────────────────────────────────────────────────────

add_executable(benchmark bench/benchmark.cpp)
target_link_libraries(benchmark PRIVATE queues Threads::Threads)
# Force -O2 for the benchmark regardless of the global build type
target_compile_options(benchmark PRIVATE -O2 -march=native)
