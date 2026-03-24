# Build Optimization Log

## Final Summary

| Round | Total Time | vs Baseline | Key Change |
|---|---|---|---|
| Baseline | 54m 1.7s | — | Sequential everything |
| Round 1 | 28m 36.6s | -47% | Parallel continents + parallel cell map rebuild |
| Round 2 | 28m 19.6s | -48% | + Parallel remap + parallel mode writes |
| Round 3 | ~33m | REVERTED | Cell pre-tagging (hash map overhead worse) |
| Round 4 | 17m 14.7s | -68% | Sorted pair filtering + eliminate rebuild |
| Round 5 | ~16m 50s | -69% | + Parallel chunked sorted pair scanning |
| Round 6 | 17m 28.3s | -68% | + Parallel dedup + planet/continent overlap |
| **Round 7** | **15m 41.7s** | **-71%** | **Simplify before S2 covering** |

## Current Best: 15m 41.7s

### Phase Breakdown
| Phase | Baseline | Current | Improvement |
|---|---|---|---|
| Pass 1: relation scanning | 46.6s | 43.9s | -6% |
| Pass 2: node processing | 184.4s | 182.7s | same |
| Pass 2b: way processing | 132.3s | 125.8s | -5% |
| Admin: ring assembly | 2.7s | 2.6s | same |
| Admin: parallel simplify | 1.2s | 1.2s | same |
| Admin: append + submit S2 | 2.9s | 2.0s | -31% |
| **Admin: S2 covering drain** | **134.4s** | **73.3s** | **-45%** |
| S2: street ways | 136.9s | 135.2s | same |
| S2: sort + group | 8.8s | 8.9s | same |
| S2 cell computation total | 251.5s | 245.4s | -2% |
| **Rebuild cell maps** | **224.2s** | **0s** | **-100%** |
| Deduplication | (in S2) | 53.0s | (now separate) |
| **Planet+continent processing** | **2129.5s** | **181.3s** | **-91%** |
| **Total** | **54m 1.7s** | **15m 41.7s** | **-71%** |

## All Optimizations Applied

1. **Parallel continents** (4 concurrent, bounded by memory)
2. **Eliminated cell map rebuild** (filter from sorted pairs directly)
3. **Parallel remap** in filter_by_bbox (4 data types simultaneously)
4. **Parallel mode writes** (full/no-addresses/admin simultaneously)
5. **Sorted pair linear scanning** (cache-friendly vs hash map random access)
6. **Parallel chunked scanning** (N threads per sorted pair scan)
7. **Parallel per-cell dedup** (addr hash map conversion)
8. **Planet/continent overlap** (planet write async while continents start)
9. **Simplify before S2 covering** (500-vertex cap for S2Polygon construction)

## Remaining Bottlenecks (all at or near optimal)
| Phase | Time | Status |
|---|---|---|
| S2 cell computation | 245.4s | 28 threads, compute-bound |
| Pass 2: nodes | 182.7s | 28 threads |
| Continent processing | 181.3s | 4 concurrent, parallel scanning |
| S2 street ways | 135.2s | 28 threads |
| Pass 2b: ways | 125.8s | 28 threads |
| Admin S2 drain | 73.3s | 28 thread pool |
| Dedup | 53.0s | Parallel per-cell |
| Pass 1 | 43.9s | Single-threaded (osmium) |

Theoretical minimum: ~10-12 min (limited by sequential PBF I/O and compute-bound S2)
