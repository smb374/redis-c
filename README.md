# smb374/redis-c

A small Redis clone with additional concurrency support.

Inspired after reading
[Build Your Own Redis with C/C++](https://build-your-own.org/redis/#table-of-contents)

## Features

- Whole project is in pure C except for tests and benchmarks.
- `libev`-based event loop handling I/O events, signals, and timers for cross-platform support.
- A thread pool with Round-Robin job dispatch to run non-IO jobs on workers.
- Primary key-value store on a concurrent Hopscotch-Hashing hashmap with size
grow support, with a lock-free SkipList + timer for handling entry TTL expiration.
- Garbage collect for concurrent data structures through QSBR.
- `ZSet` support through serial Hopscotch-Hashing hashmap and SkipList dual index.
- Implemented commands
  - Primary key-value operations (`GET`, `SET`, `DEL`)
  - Ranged commands under a key entry (`ZADD`, `ZREM`, `ZSCORE`, `ZQUERY`)
  - TTL support with independent commands (`PTTL`, `PEXPIRE`)

## Dependencies

All of the dependencies should be automatically downloaded when running cmake.

- [`libev`](https://software.schmorp.de/pkg/libev.html) for event loop
- [`google/googletest`](https://github.com/google/googletest) for writing unit tests in C++.
- [`google/benchmark`](https://github.com/google/benchmark) for benchmarking the concurrent Hopscotch-Hashing hashmap.

## Future Work

- Currently every command is scheduled individually to the worker with Round-Robin,
  planning to make the same batch of commands from a client being processed on
  the same worker.
- Make more tests and updates to find and remove bugs from current code base.
- Add persistence support through log-replay.
- Maybe add raft support for supporting distributed server consistency.

## Credits

- [build-your-own.org](https://build-your-own.org/) for providing reference
  serial version of the small Redis.
- [Preshing's blog](https://preshing.com/) for resources on concurrent
  programming knowledges, especially for memory order knowledge, concurrent hashmap design,
  and QSBR memory reclamation design.
- [Practical lock-freedom by Keir Fraser](https://www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf):
  The lock-free SkipList came from here
- [Non-blocking hashtables with open addressing by Chris Purcell, Tim Harris](https://www.cl.cam.ac.uk/techreports/UCAM-CL-TR-639.pdf):
  Quadratic-probed lock-free fixed sized hashtable, initially wanted to add resize support on it
  but find it too hard for me to do it.
- [Hopscotch Hashing by Maurice Herlihy, Nir Shavit, Moran Tzafrir](https://dl.acm.org/doi/10.1007/978-3-540-87779-0_24):
  Hopscotch Hashing scheme and ideas about the fixed-sized concurrent version.
- [Crystalline: Fast and Memory Efficient Wait-Free Reclamation by Ruslan Nikolaev, Binoy Ravindran](https://arxiv.org/abs/2108.02763):
  Tried to use it instead of using QSBR, but couldn't figure out how to clean remaining nodes
  in the grid when threads exits.

## Benchmark

Here's the benchmark result for the concurrent Hopscotch-Hashing hashmap:

```
2025-10-16T18:17:46-04:00
Running ./build-release/chpmap_bench
Run on (12 X 5443.07 MHz CPU s)
CPU Caches:
  L1 Data 48 KiB (x6)
  L1 Instruction 32 KiB (x6)
  L2 Unified 1024 KiB (x6)
  L3 Unified 32768 KiB (x1)
Load Average: 0.89, 0.89, 0.63
--------------------------------------------------------------------------------------------------------------------
Benchmark                                                          Time             CPU   Iterations UserCounters...
--------------------------------------------------------------------------------------------------------------------
CHPMapFixture/BM_Insert/real_time/threads:1                      262 ns          262 ns      2772127 items_per_second=3.81164M/s
CHPMapFixture/BM_Insert/real_time/threads:2                      148 ns          294 ns      3930666 items_per_second=6.76506M/s
CHPMapFixture/BM_Insert/real_time/threads:4                      144 ns          566 ns      4197452 items_per_second=6.92236M/s
CHPMapFixture/BM_Insert/real_time/threads:8                      153 ns         1171 ns      6164536 items_per_second=6.55665M/s
CHPMapFixture/BM_LookupExisting/real_time/threads:1             35.7 ns         35.6 ns     21674035 items_per_second=28.0239M/s
CHPMapFixture/BM_LookupExisting/real_time/threads:2             16.8 ns         33.6 ns     43492640 items_per_second=59.3867M/s
CHPMapFixture/BM_LookupExisting/real_time/threads:4             7.56 ns         30.2 ns     95563964 items_per_second=132.307M/s
CHPMapFixture/BM_LookupExisting/real_time/threads:8             4.43 ns         35.3 ns    154462176 items_per_second=225.638M/s
CHPMapFixture/BM_LookupNonExisting/real_time/threads:1          25.1 ns         25.0 ns     25536823 items_per_second=39.8671M/s
CHPMapFixture/BM_LookupNonExisting/real_time/threads:2          10.2 ns         20.3 ns     60609068 items_per_second=98.1895M/s
CHPMapFixture/BM_LookupNonExisting/real_time/threads:4          4.81 ns         19.2 ns    142702620 items_per_second=207.847M/s
CHPMapFixture/BM_LookupNonExisting/real_time/threads:8          2.65 ns         21.2 ns    270343832 items_per_second=376.879M/s
CHPMapFixture/BM_LookupMixed/real_time/threads:1                42.9 ns         42.9 ns     16240250 items_per_second=23.2894M/s
CHPMapFixture/BM_LookupMixed/real_time/threads:2                19.9 ns         39.7 ns     34567410 items_per_second=50.3015M/s
CHPMapFixture/BM_LookupMixed/real_time/threads:4                9.41 ns         37.6 ns     75684284 items_per_second=106.322M/s
CHPMapFixture/BM_LookupMixed/real_time/threads:8                5.28 ns         42.1 ns    131191832 items_per_second=189.277M/s
CHPMapFixture/BM_Upsert/real_time/threads:1                      105 ns          105 ns      5285545 items_per_second=9.50939M/s
CHPMapFixture/BM_Upsert/real_time/threads:2                     54.7 ns          109 ns      9406212 items_per_second=18.2882M/s
CHPMapFixture/BM_Upsert/real_time/threads:4                     27.4 ns          109 ns     20191836 items_per_second=36.5047M/s
CHPMapFixture/BM_Upsert/real_time/threads:8                     17.5 ns          138 ns     29900984 items_per_second=57.0539M/s
CHPMapFixture/BM_Mixed_80Read_20Write/real_time/threads:1       91.1 ns         91.0 ns      6994964 items_per_second=10.9752M/s
CHPMapFixture/BM_Mixed_80Read_20Write/real_time/threads:2       64.1 ns          128 ns     12051438 items_per_second=15.593M/s
CHPMapFixture/BM_Mixed_80Read_20Write/real_time/threads:4       44.7 ns          176 ns     18342892 items_per_second=22.3936M/s
CHPMapFixture/BM_Mixed_80Read_20Write/real_time/threads:8       39.0 ns          299 ns     18808736 items_per_second=25.6126M/s
CHPMapFixture/BM_Mixed_CRUD/real_time/threads:1                 67.4 ns         67.3 ns     12191220 items_per_second=14.8464M/s
CHPMapFixture/BM_Mixed_CRUD/real_time/threads:2                 43.1 ns         85.9 ns     19341036 items_per_second=23.1868M/s
CHPMapFixture/BM_Mixed_CRUD/real_time/threads:4                 31.9 ns          126 ns     28043628 items_per_second=31.3819M/s
CHPMapFixture/BM_Mixed_CRUD/real_time/threads:8                 25.9 ns          203 ns     28033856 items_per_second=38.5809M/s
```

It has a very strong performance for pure lookups, either lookup existing,
non-existing, or both at the same time. The table is pre-filled with
500K items before benchmark starts.

Some detail about the benchmarks:

- Setup: pre-filled 500K elements before each benchmark starts.
- `BM_Insert`: pure insert with non-existing keys.
- `BM_LookupExisting`: pure lookup with existing keys.
- `BM_LookupNonExisting`: pure lookup with non-existing keys.
- `BM_LookupMixed`: pure lookup with both existing and non-existing keys.
- `BM_Upsert`: "find or insert" in single operation, with both existing and non-existing keys.
- `BM_Mixed_80Read_20Write`: 80% lookup 20% upsert load.
- `BM_Mixed_CRUD`: 80% lookup 10% upsert 10% remove load.
