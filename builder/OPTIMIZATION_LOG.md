# Build Optimization Log

## Baseline Timings (54m 1.7s total)
Build: `--multi-output --continents --multi-quality` (planet PBF → S3 layout)

### PBF Processing Phase (13m 42s)
| Phase | Time | CPU% | Notes |
|---|---|---|---|
| Pass 1: relation scanning | 46.6s | ~100% | Single-threaded PBF read |
| Pass 2: node processing | 184.4s | ~2000% | 28 threads, good utilization |
| Pass 2b: way processing | 132.3s | ~2000% | 28 threads, good utilization |
| Admin: ring assembly | 2.7s | ~2800% | Fully parallel |
| Admin: parallel simplify | 1.2s | ~2800% | Fully parallel (uncapped = minimal work) |
| Admin: append + submit S2 | 2.9s | ~100% | Sequential (append to vectors) |
| Admin: S2 covering drain | 134.4s | ~2800% | Thread pool processing |
| Admin assembly total | 0.5s | | |
| S2: street ways | 136.9s | ~2800% | Fully parallel |
| S2: interp ways | 0.0s | ~2800% | Tiny dataset |
| S2: sort + group | 8.8s | ~2800% | Parallel tree merge |
| S2 cell computation total | 251.5s | | |

### Dedup + Rebuild Phase (224.2s = 3m 44s)
| Phase | Time | CPU% | Notes |
|---|---|---|---|
| Rebuild cell maps | 224.2s | ~1500% | **2 async tasks (ways + interps), but only uses 2 cores for hash map insertion** |
| Deduplication | 0.0s | | Included in rebuild timing |

### Write Phase (2147.9s = 35m 48s)
| Phase | Time | CPU% | Notes |
|---|---|---|---|
| Planet index + qualities | 42.0s | mixed | 3 modes + 7 quality variants |

### Continent Processing (sequential, ~34 min)
| Continent | Filter | Write | Total |
|---|---|---|---|
| Africa | 162.1s | 16.1s | 178.2s |
| Asia | 234.4s | 93.1s | 327.5s |
| Europe | 277.4s | 121.2s | 398.6s |
| North America | 301.0s | 172.8s | 473.8s |
| South America | 177.3s | 29.6s | 206.9s |
| Oceania | 164.9s | 22.1s | 187.0s |
| Central America | 159.5s | 10.8s | 170.3s |
| Antarctica | 145.2s | 0.0s | 145.2s |
| **Total** | **1621.8s** | **465.7s** | **2087.5s** |

## Bottleneck Analysis

### 1. filter_by_bbox: 1621.8s (30% of total)
- Scans ALL planet cell maps for each continent (even Antarctica scans all 300M entries)
- Already has 4 parallel cell scans + 4 parallel remaps (8 threads)
- But 8 threads on a 32-core machine wastes 24 cores
- **Fix**: Run 2-3 continents concurrently to use more cores

### 2. Rebuild cell maps: 224.2s (7% of total)
- Two async tasks (ways + interps) inserting into hash maps
- Hash map insertion is inherently sequential per map
- Only uses ~2 cores effectively despite showing 1500% CPU
- **Fix**: Partition sorted pairs into N shards, build N smaller hash maps in parallel, merge

### 3. Continent write phase: 465.7s (8.6% of total)
- Each continent writes 3 modes + 7 quality variants
- Quality variant generation re-simplifies vertices for each scale
- Already parallelized internally (write_index uses threads)
- **Fix**: Limited room — already using threads within each write

### 4. S2 covering drain: 134.4s (2.5% of total)
- Thread pool processes uncapped polygons (365M vertices)
- With simplified polygons this was ~70s
- Already fully parallel

### 5. Pass 1: 46.6s (single-threaded PBF scan)
- Unavoidable — single PBF reader

---

## Round 1: Parallel continent processing
**Goal**: Run multiple continents concurrently (2-3 at a time) to better utilize cores during filter phase
**Expected savings**: ~50% of filter time (1621s → ~810s)
**Status**: In progress
