# Build Optimization Log

## Final Result: 54m → 13m 32s (75% faster)

Build: `--multi-output --continents --multi-quality`
Output: 379 files, 93 GiB (planet + 8 continents × 3 modes × 7 quality levels)
Machine: 32-core / 174 GiB RAM
Validated: all output semantically identical to pre-optimization baseline

## Phase Breakdown

| Phase | Baseline (54m) | Osmium-optimized (16m) | Custom PBF (13m 32s) |
|---|---|---|---|
| Pass 1: relations | 46.6s | 47.6s | **25-38s** |
| Pass 2: nodes | 184.4s | 183.3s | **71-86s** |
| Pass 2b: ways | 132.3s | 126.5s | **94-100s** |
| Admin S2 drain | 134.4s | 133.2s | 129-132s |
| S2 street ways | 136.9s | 131.4s | 129-132s |
| S2 total | 251.5s | 241.9s | 206-247s |
| Rebuild cell maps | 224.2s | **0s** | 0s |
| Dedup | (in S2) | 53.3s | 38-55s |
| Continent processing | 2087.5s | 147.9s | 131-149s |
| **Total** | **54m 1.7s** | **16m 11s** | **13m 30s** |

## All Optimizations Applied

### PBF Reading (osmium → custom reader)
1. **Custom parallel PBF reader** — replaced osmium entirely
2. **Streaming node decode** — no PbfNode objects, single-pass protobuf, inline callback
3. **Streaming way decode** — no PbfWay objects, direct callback during decode
4. **Zero-copy varint decode** — iterate packed fields directly without intermediate vectors
5. **Mmap PBF file** — eliminates pread syscalls for parallel decode
6. **Reuse decomp buffer + z_stream** — eliminates malloc contention
7. **Release mmap pages between passes** — reduces memory pressure
8. **Unmap after Pass 2b** — frees 86 GiB before later phases
9. **Single-pass tag extraction** — one loop extracts all needed tags
10. **Early exit for irrelevant ways** — skips index.get() for ~80% of ways
11. **Flat way_refs array** — eliminates per-way vector allocation
12. **Block reuse** — PbfBlock vectors cleared but keep capacity
13. **Parallel blob classification** — classify 50K blobs in parallel not sequentially
14. **MADV_HUGEPAGE** on dense index — reduces TLB miss penalty

### Data Processing
15. **Parallel continents** (4 concurrent, bounded, largest-first)
16. **Eliminated planet cell map rebuild** (filter from sorted pairs)
17. **Parallel remap** in filter_by_bbox
18. **Parallel mode writes** (full/no-addresses/admin simultaneously)
19. **Parallel quality variants** (3 concurrent)
20. **Sorted pair filtering** (cache-friendly linear scan)
21. **Parallel chunked scanning** per continent
22. **Parallel per-cell dedup**
23. **Admin polygon sorting** (largest-first for work-stealing)
24. **Continent sorting** (largest-first by bbox area)
25. **Worker threads = N-1** (31 instead of 28)
26. **Deterministic interp resolution** (lexicographic tiebreaker)
27. **Sorted pair remap for continents** (avoids hash map, fixes street_entries bug)
28. **Per-phase CPU/memory profiling** (getrusage + /proc/self/statm)

## Bug Fixes
- **Continent street_entries.bin was empty** — fixed by producing sorted pairs for continent subsets
- **Deterministic interp resolution** — old code had nondeterministic last-wins behavior
- **PBF way callback used return instead of continue** — skipped 50% of ways

## Remaining Hardware-Limited Bottlenecks
- **Pass 2b (94s, 25% CPU)** — random reads across 111 GiB dense index are TLB-miss bound
- **S2 computation (206s, 96% CPU)** — compute-bound, optimal
- **Admin S2 drain (129s, 95% CPU)** — compute-bound, optimal
- **Dedup (38s, 4% CPU)** — hash map iteration is inherently sequential
