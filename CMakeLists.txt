cmake_minimum_required(VERSION 3.10)
project(redis-c)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

enable_testing()

include(FetchContent)
# Google Test
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
# Google Benchmark
FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.8.0
)
FetchContent_MakeAvailable(googlebenchmark)
# libev
include(FetchContent)
FetchContent_Declare(
    ev
    GIT_REPOSITORY  https://github.com/sjinks/libev-cmake
    GIT_TAG         v4.33
)
FetchContent_MakeAvailable(ev)

option(ENABLE_LOGGING "Enable Logging with logger" OFF)

# Bundle all files into a lib except for executable files.
add_library(common_lib STATIC
        src/hashtable.c
        src/ringbuf.c
        src/connection.c
        src/utils.c
        src/parse.c
        src/skiplist.c
        src/zset.c
        src/serialize.c
        src/heap.c
        src/kvstore.c
        src/cqueue.c
        src/thread_pool.c
        src/qsbr.c
        src/ebr.c
        src/cskiplist.c
)
target_link_libraries(common_lib PUBLIC ev::ev)
target_include_directories(common_lib PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
if (ENABLE_LOGGING)
    target_compile_definitions(common_lib PRIVATE LOGGING)
endif()

# Executables
## KV Server
add_executable(kv_server src/kv_server.c)
target_link_libraries(kv_server PRIVATE common_lib)
if(ENABLE_LOGGING)
    target_compile_definitions(kv_server PRIVATE LOGGING)
endif()

## KV Client
add_executable(kv_client tests/kv_client.cpp)

# Tests
## ring buffer test
add_executable(ringbuf_test tests/ringbuf_test.cpp)
target_link_libraries(ringbuf_test PRIVATE common_lib gtest_main)
add_test(NAME ringbuf_test COMMAND ringbuf_test)
## hashtable_test
add_executable(hashtable_test tests/hashtable_test.cpp)
target_link_libraries(hashtable_test PRIVATE common_lib gtest_main)
add_test(NAME hashtable_test COMMAND hashtable_test)
## parse_test
add_executable(parse_test tests/parse_test.cpp)
target_link_libraries(parse_test PRIVATE common_lib gtest_main)
add_test(NAME parse_test COMMAND parse_test)
## skiplist_test
add_executable(skiplist_test tests/skiplist_test.cpp)
target_link_libraries(skiplist_test PRIVATE common_lib gtest_main)
add_test(NAME skiplist_test COMMAND skiplist_test)
## zset_test
add_executable(zset_test tests/zset_test.cpp)
target_link_libraries(zset_test PRIVATE common_lib gtest_main)
add_test(NAME zset_test COMMAND zset_test)
## serialize_test
add_executable(serialize_test tests/serialize_test.cpp)
target_link_libraries(serialize_test PRIVATE common_lib gtest_main)
add_test(NAME serialize_test COMMAND serialize_test)
## heap_test
add_executable(heap_test tests/heap_test.cpp)
target_link_libraries(heap_test PRIVATE common_lib gtest_main)
add_test(NAME heap_test COMMAND heap_test)
## kvstore_test
add_executable(kvstore_test tests/kvstore_test.cpp)
target_link_libraries(kvstore_test PRIVATE common_lib gtest_main)
add_test(NAME kvstore_test COMMAND kvstore_test)
## cqueue_test
add_executable(cqueue_test tests/cqueue_test.cpp)
target_link_libraries(cqueue_test PRIVATE common_lib gtest_main pthread)
add_test(NAME cqueue_test COMMAND cqueue_test)
## thread_pool_test
add_executable(thread_pool_test tests/thread_pool_test.cpp)
target_link_libraries(thread_pool_test PRIVATE common_lib gtest_main pthread)
add_test(NAME thread_pool_test COMMAND thread_pool_test)
## chashmap_test
add_executable(chashmap_test tests/chashmap_test.cpp)
target_link_libraries(chashmap_test PRIVATE common_lib gtest_main pthread atomic)
# target_compile_options(chashmap_test PRIVATE -fsanitize=thread)
# target_link_options(chashmap_test PRIVATE -fsanitize=thread)
add_test(NAME chashmap_test COMMAND chashmap_test)
## ebr_test
add_executable(ebr_test tests/ebr_test.cpp)
target_link_libraries(ebr_test PRIVATE common_lib gtest_main pthread atomic)
add_test(NAME ebr_test COMMAND ebr_test)
## qsbr_test
add_executable(qsbr_test tests/qsbr_test.cpp)
target_link_libraries(qsbr_test PRIVATE common_lib gtest_main pthread)
add_test(NAME qsbr_test COMMAND qsbr_test)
## cskiplist_test
add_executable(cskiplist_test tests/cskiplist_test.cpp)
target_link_libraries(cskiplist_test PRIVATE common_lib gtest_main pthread atomic)
add_test(NAME cskiplist_test COMMAND cskiplist_test)

set_tests_properties(
        ringbuf_test
        hashtable_test
        parse_test
        skiplist_test
        zset_test
        serialize_test
        heap_test
        kvstore_test
        cqueue_test
        thread_pool_test
        chashmap_test
        ebr_test
        qsbr_test
        cskiplist_test
        PROPERTIES LABELS "Unit"
)

# Benchmarks
## chashmap_bench
add_executable(chashmap_bench bench/chashmap_bench.cpp)
target_link_libraries(chashmap_bench PRIVATE common_lib benchmark::benchmark pthread)

