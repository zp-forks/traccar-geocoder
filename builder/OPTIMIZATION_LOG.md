# Build Optimization Log

## Current Best: 16m 11s (70% faster than 54m baseline)

Build: `--multi-output --continents --multi-quality`
Output: 379 files, 93 GiB (planet + 8 continents × 3 modes × 7 quality levels)
Machine: 32-core / 174 GiB RAM

### Phase Breakdown (with CPU profiling)
| Phase | Wall | Cores | Util% | RSS |
|---|---|---|---|---|
| Pass 1: relation scanning | 43s | 13/32 | 41% | 1.2 GiB |
| Pass 2: node processing | 189s | 16/32 | 50% | 113 GiB |
| Pass 2b: way processing | 129s | 10/32 | 31% | 142 GiB |
| Admin: ring assembly | 2.6s | 31/32 | 96% | 149 GiB |
| Admin: S2 covering drain | 133s | 30/32 | 94% | 139 GiB |
| S2: street ways | 131s | 31/32 | 96% | 145 GiB |
| S2 total | 250s | 17/32 | 53% | 151 GiB |
| Dedup | 56s | 1.4/32 | 4% | 40 GiB |
| Continent processing | 148s | 13/32 | 39% | 45-67 GiB |
| **Total** | **16m 11s** | | | |

## Critical Bug Found and Fixed

When the cell map rebuild was eliminated (commit 73a3005), continent `street_entries.bin`
files became empty (0 bytes). Fixed in commit 5926502 by producing sorted pairs for
continent subsets directly. Output size increased from 77 GiB to 93 GiB.

## All Optimizations Applied

1. **Parallel continents** (4 concurrent, bounded)
2. **Eliminated planet cell map rebuild** (filter from sorted pairs)
3. **Parallel remap** in filter_by_bbox (4 data types simultaneously)
4. **Parallel mode writes** (full/no-addresses/admin simultaneously)
5. **Parallel quality variants** (3 concurrent per region)
6. **Sorted pair filtering** (cache-friendly linear scan)
7. **Parallel chunked scanning** (N threads per sorted pair scan)
8. **Parallel per-cell dedup** (addr hash map conversion)
9. **Planet/continent write overlap**
10. **Admin polygon sorting** (largest-first for work-stealing)
11. **Continent sorting** (largest-first by bbox area)
12. **Worker threads = N-1** (31 instead of 28)
13. **Deterministic interp resolution** (lexicographic tiebreaker)
14. **Sorted pair remap for continents** (avoids hash map entirely)
15. **Per-phase CPU/memory profiling** (getrusage + /proc/self/statm)

## Remaining Bottlenecks
| Phase | Wall | Cores | Opportunity |
|---|---|---|---|
| S2 cell computation | 250s | 17/32 | Compute-bound, optimal |
| Pass 2: nodes | 189s | 16/32 | PBF I/O bound |
| Admin S2 drain | 133s | 30/32 | Compute-bound, optimal |
| Pass 2b: ways | 129s | 10/32 | PBF I/O bound |
| Dedup | 56s | 1.4/32 | Hash map iteration is sequential |
| Pass 1 | 43s | 13/32 | Single-threaded PBF scan |
