# Build Optimization Log

## Summary

| Round | Total Time | vs Baseline | Key Change |
|---|---|---|---|
| Baseline | 54m 1.7s | — | Sequential everything |
| Round 1 | 28m 36.6s | -47% | Parallel continents + parallel cell map rebuild |
| Round 2 | 28m 19.6s | -48% | Parallel remap + parallel mode writes |
| Round 3 | ~33m | REVERTED | Cell pre-tagging (hash map overhead) |
| Round 4 | 17m 14.7s | -68% | Sorted pair filtering + eliminate rebuild |
| **Round 5** | **~16m 50s** | **-69%** | Parallel chunked sorted pair scanning |

## Current Best Phase Breakdown (~16m 50s)

| Phase | Time | Notes |
|---|---|---|
| Pass 1: relation scanning | 46.5s | Single-threaded PBF read |
| Pass 2: node processing | 187.5s | 28 threads |
| Pass 2b: way processing | 132.5s | 28 threads |
| Admin assembly (all) | 142.7s | Mixed parallel |
| S2 cell computation | 246.9s | 28 threads |
| Deduplication | 50.6s | 2 async tasks |
| Planet write + qualities | 33.7s | Parallel |
| Continent processing | 168.5s | 4 concurrent, parallel chunked scan |
| **Total** | **~16m 50s** | |

### Continent Filter Times (Round 5 — parallel chunked scan)
| Continent | Filter | Total |
|---|---|---|
| Africa | 30.5s | 3.7s |
| Asia | 60.3s | 9.8s |
| Europe | 137.7s | 22.0s |
| North America | 82.9s | 12.0s |
| South America | 29.8s | 5.6s |
| Oceania | 27.6s | 4.0s |
| Central America | 24.7s | 1.5s |
| Antarctica | 19.1s | 0.0s |

## Remaining Bottlenecks (all already heavily parallelized)
| Phase | Time | Status |
|---|---|---|
| S2 cell computation | 246.9s | 28 threads, compute-bound |
| Pass 2: nodes | 187.5s | 28 threads, I/O + compute |
| Admin S2 drain | 136.2s | Thread pool |
| S2 street ways | 136.5s | 28 threads |
| Continent processing | 168.5s | 4×8 threads, memory-limited concurrency |
| Pass 2b: ways | 132.5s | 28 threads |
| Deduplication | 50.6s | 2 async tasks |
| Pass 1 | 46.5s | Single-threaded (osmium) |
| Planet write | 33.7s | Parallel |

### Notes
- Most phases are already at or near optimal parallelism
- Further gains would require architectural changes (pipeline overlap between phases, reduce data copies)
- The theoretical minimum for this workload is ~10-12 min (limited by PBF I/O and S2 computation)
