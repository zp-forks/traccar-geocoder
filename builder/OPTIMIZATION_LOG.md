# Build Optimization Log

## Final Summary

| Round | Total Time | vs Baseline | Key Change |
|---|---|---|---|
| Baseline | 54m 1.7s | — | Sequential everything |
| Round 1 | 28m 36.6s | -47% | Parallel continents (4 concurrent) + parallel cell map rebuild |
| Round 2 | 28m 19.6s | -48% | + Parallel remap + parallel mode writes |
| Round 3 | ~33m | REVERTED | Cell pre-tagging (hash map overhead worse) |
| **Round 4** | **17m 14.7s** | **-68%** | Sorted pair filtering + eliminate rebuild |
| Round 5 | ~16m 50s | -69% | + Parallel chunked sorted pair scanning |
| Round 6 | 17m 28.3s | -68% | + Parallel dedup + planet/continent overlap |

## Best Configuration: ~17 minutes (68-69% faster than baseline)

Build command:
```
build-index output/ planet.osm.pbf --multi-output --continents --multi-quality
```

Output: 379 files, 77 GiB (planet + 8 continents × 3 modes × 7 quality levels)

### Phase Breakdown
| Phase | Baseline | Current | Improvement |
|---|---|---|---|
| Pass 1: relation scanning | 46.6s | 46-76s | (disk cache variance) |
| Pass 2: node processing | 184.4s | 186.9s | same (28 threads) |
| Pass 2b: way processing | 132.3s | 128.8s | same (28 threads) |
| Admin assembly (all) | 141.7s | 142.0s | same |
| S2 cell computation | 251.5s | 246.0s | same (28 threads) |
| **Rebuild cell maps** | **224.2s** | **0s** | **eliminated** |
| Deduplication | (included above) | 56.3s | (now separate timing) |
| Planet + continent processing | **2129.5s** | **182.3s** | **-91%** |
| **Total** | **54m 1.7s** | **~17m** | **-68%** |

## Optimizations Applied

### ✅ Parallel continent processing (4 concurrent)
Each continent uses ~8 threads for filtering. 4 concurrent on 32-core machine.

### ✅ Parallel cell map rebuild → ELIMINATED
Originally parallelized rebuild, then eliminated entirely by making filter_by_bbox work with sorted pairs directly.

### ✅ Parallel remap in filter_by_bbox
4 data type remaps (ways/addrs/interps/admins) run in parallel, each building its own vectors.

### ✅ Parallel mode writes
3 mode writes (full/no-addresses/admin) run in parallel per region.

### ✅ Sorted pair filtering (cache-friendly linear scan)
filter_by_bbox uses sorted pair arrays instead of hash maps for ways/addrs/interps. Linear scan is 2-5x faster than hash map iteration.

### ✅ Parallel chunked sorted pair scanning
Each sorted pair scan is split into N chunks across threads for parallel cell testing.

### ✅ Planet write overlaps with continent start
Planet write (I/O bound) runs async while continent filtering (CPU bound) begins.

### ✅ Parallel per-cell dedup
Addr hash map per-cell sort+dedup parallelized across all cores.

### ❌ Cell pre-tagging (REVERTED)
Pre-computing continent masks for 300M+ cells took longer than the savings.

## Remaining Bottlenecks (all at or near optimal)
| Phase | Time | Status |
|---|---|---|
| S2 cell computation | 246.0s | 28 threads, compute-bound |
| Pass 2: nodes | 186.9s | 28 threads |
| Admin S2 drain | 135.6s | Thread pool |
| S2 street ways | 135.8s | 28 threads |
| Pass 2b: ways | 128.8s | 28 threads |
| Continent processing | 182.3s | 4 concurrent, parallel scanning |
| Dedup | 56.3s | Parallel per-cell |
| Pass 1 | 46-76s | Single-threaded (osmium PBF reader) |

Theoretical minimum: ~12-14 min (limited by sequential PBF I/O stages and compute-bound S2)
