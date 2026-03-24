# Build Optimization Log

## Summary

| Round | Total Time | Change vs Previous | Change vs Baseline |
|---|---|---|---|
| Baseline | 54m 1.7s | — | — |
| Round 1 | 28m 36.6s | -47% | -47% |
| Round 2 | 28m 19.6s | -1% | -48% |
| Round 3 (reverted) | ~33m | +16% | REVERTED |
| **Round 4** | **17m 14.7s** | **-39%** | **-68%** |

## Current Best: 17m 14.7s

### Phase Breakdown
| Phase | Baseline | Current | Change |
|---|---|---|---|
| Pass 1: relation scanning | 46.6s | 49.1s | (disk cache variance) |
| Pass 2: node processing | 184.4s | 176.3s | -4% |
| Pass 2b: way processing | 132.3s | 120.3s | -9% |
| Admin assembly | 141.7s | 143.2s | same |
| S2 cell computation | 251.5s | 245.8s | -2% |
| **Rebuild cell maps** | **224.2s** | **0s (eliminated)** | **-100%** |
| Planet write + qualities | 42.0s | 35.3s | -16% |
| **Continent processing** | **2087.5s** | **172.0s** | **-92%** |
| **Total** | **54m 1.7s** | **17m 14.7s** | **-68%** |

### Continent Filter Times (sorted pairs vs hash maps)
| Continent | Baseline | Round 1 (hash map) | Round 4 (sorted pairs) |
|---|---|---|---|
| Africa | 162.1s | 147.9s | **55.7s** |
| Asia | 234.4s | 264.8s | **79.5s** |
| Europe | 277.4s | 313.0s | **136.5s** |
| North America | 301.0s | 403.0s | (concurrent) |
| South America | 177.3s | 187.3s | **58.6s** |
| Oceania | 164.9s | — | **56.3s** |
| Central America | 159.5s | — | **53.2s** |
| Antarctica | 145.2s | — | **49.6s** |

---

## Changes Applied

### Round 1: Parallel continents + parallel cell map rebuild ✅
**Commit**: `d2901da`
- 4 concurrent continents on 32-core machine
- Parallel cell map rebuild: split sorted pairs into chunks

### Round 2: Parallel remap + parallel mode writes ✅
**Commits**: `5a7f9d7`, `49c4b81`
- 4 remap phases run in parallel in filter_by_bbox
- 3 mode writes (full/no-addresses/admin) run in parallel

### Round 3: Cell pre-tagging ❌ REVERTED
**Commit**: `f17eb6c` → reverted `996c374`
- Pre-computing continent masks for 300M cells took 434s — worse than savings

### Round 4: Sorted pair filtering + eliminate rebuild ✅
**Commit**: `73a3005`
- filter_by_bbox uses sorted pair arrays instead of hash maps for ways/addrs/interps
- Linear cache-friendly scan vs random hash map iteration = 2-5x faster per continent
- Eliminated 170s rebuild_cell_maps phase entirely
- Combined savings: 170s rebuild + ~70% faster continent filtering

---

## Remaining Bottlenecks

| Phase | Time | Feasibility |
|---|---|---|
| S2 cell computation | 245.8s | Already fully parallel (28 threads) |
| Pass 2: node processing | 176.3s | Already parallel (28 threads) |
| S2 covering drain | 137.0s | Already parallel thread pool |
| S2 street ways | 137.7s | Already parallel (28 threads) |
| Admin: S2 covering drain | 137.0s | Already parallel thread pool |
| Pass 2b: way processing | 120.3s | Already parallel (28 threads) |
| Continent processing | 172.0s | 4 concurrent, sorted pair scanning |
| Pass 1 | 49.1s | Single-threaded osmium PBF read (unavoidable) |
| Deduplication | 50.2s | 2 async tasks |
| Planet write | 35.3s | Mixed parallel |

Most remaining phases are already parallel. The theoretical minimum is ~250s (S2 computation, limited by CPU) + 176s (nodes) + 120s (ways) ≈ ~9 min if everything overlapped perfectly, but these are sequential pipeline stages.
