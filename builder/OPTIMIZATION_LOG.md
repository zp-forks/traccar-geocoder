# Build Optimization Log

## Baseline Timings (54m 1.7s total)
Build: `--multi-output --continents --multi-quality` (planet PBF → S3 layout)
- 379 files, 93 GiB output
- 32-core / 174 GiB RAM machine

### Phase Breakdown
| Phase | Time | Notes |
|---|---|---|
| Pass 1: relation scanning | 46.6s | Single-threaded PBF read |
| Pass 2: node processing | 184.4s | 28 threads |
| Pass 2b: way processing | 132.3s | 28 threads |
| Admin assembly (all) | 141.7s | Mixed parallel/sequential |
| S2 cell computation | 251.5s | 28 threads |
| Rebuild cell maps | 224.2s | 2 async tasks (~15 cores) |
| Planet write + qualities | 42.0s | Mixed |
| Continent filter (sum) | 1621.8s | Sequential, ~8 threads each |
| Continent write (sum) | 465.7s | Sequential |
| **Total** | **54m 1.7s** | |

---

## Round 1: Parallel continents + parallel cell map rebuild
**Commit**: `d2901da`

### Changes
1. **Bounded concurrent continents**: Run up to `hardware_concurrency / 8` continents simultaneously (4 on 32-core). Each uses 8 threads for filter_by_bbox.
2. **Parallel cell map rebuild**: Split sorted pairs at cell boundaries into N chunks, each thread builds its own sub-map, then merge with move semantics.

### Results (28m 36.6s total — 47% faster)
| Phase | Baseline | Round 1 | Change |
|---|---|---|---|
| Pass 1 | 46.6s | 91.6s | +45s (disk cache) |
| Pass 2 | 184.4s | 181.7s | -3s |
| Pass 2b | 132.3s | 124.9s | -7s |
| Admin assembly | 141.7s | 142.2s | same |
| S2 computation | 251.5s | 245.6s | -6s |
| Rebuild cell maps | **224.2s** | **169.8s** | **-24%** |
| Planet write + qualities | 42.0s | 51.6s | +10s |
| Continent processing | **2087.5s** | **621.7s** | **-70%** |
| **Total** | **54m 1.7s** | **28m 36.6s** | **-47%** |

### Continent Detail (concurrent, 4 at a time)
| Continent | Baseline filter | Baseline write | Round 1 total |
|---|---|---|---|
| Africa | 162.1s | 16.1s | 19.7s |
| Asia | 234.4s | 93.1s | 107.6s |
| Europe | 277.4s | 121.2s | 124.7s |
| North America | 301.0s | 172.8s | 191.2s |
| South America | 177.3s | 29.6s | 71.6s |
| Oceania | 164.9s | 22.1s | 48.4s |
| Central America | 159.5s | 10.8s | 15.3s |
| Antarctica | 145.2s | 0.0s | 0.3s |
| **Wall clock** | **2087.5s** | | **621.7s** |

---

## Round 2: [Investigating]
### Remaining bottlenecks
1. **Rebuild cell maps**: 169.8s — still the largest single phase. The hash map merge step is sequential.
2. **Pass 1**: 91.6s — inflated by disk cache; normally ~45s. Single-threaded, unavoidable.
3. **S2 covering drain**: 135.9s — fully parallel, hard to optimize.
4. **S2 street ways**: 136.0s — fully parallel, hard to optimize.
5. **Continent filter**: overlapped but each still ~150-400s. Could the filter itself be faster?
