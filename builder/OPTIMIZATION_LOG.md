# Build Optimization Log

## Summary

| Round | Total Time | Change | Key Change |
|---|---|---|---|
| Baseline | 54m 1.7s | — | Sequential everything |
| Round 1 | 28m 36.6s | **-47%** | Parallel continents (4 concurrent) + parallel cell map rebuild |
| Round 2 | 28m 19.6s | -0.5% | Parallel remap in filter_by_bbox (minimal gain) |
| Round 3 | ~33m | **+16%** | Cell pre-tagging (REVERTED — hash map overhead too high) |
| Current best | **28m 19.6s** | **-48% from baseline** | |

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
| Rebuild cell maps | 224.2s | 2 async tasks |
| Planet write + qualities | 42.0s | Mixed |
| Continent processing (all) | 2087.5s | Sequential |
| **Total** | **54m 1.7s** | |

---

## Round 1: Parallel continents + parallel cell map rebuild ✅
**Commit**: `d2901da`

### Changes
1. Bounded concurrent continents: 4 at a time on 32-core machine
2. Parallel cell map rebuild: split sorted pairs at cell boundaries into N chunks

### Results: 28m 36.6s (47% faster)
| Phase | Baseline | Round 1 | Change |
|---|---|---|---|
| Rebuild cell maps | 224.2s | 169.8s | -24% |
| Continent processing | 2087.5s | 621.7s | **-70%** |
| **Total** | **54m 1.7s** | **28m 36.6s** | **-47%** |

---

## Round 2: Parallel remap in filter_by_bbox + parallel mode writes ✅
**Commits**: `5a7f9d7`, `49c4b81`

### Changes
1. 4 remap phases (ways, addrs, interps, admins) run in parallel
2. 3 mode writes (full, no-addresses, admin) run in parallel per region

### Results: 28m 19.6s (marginal improvement)
Same as Round 1 — the remap parallelism helps within each continent but doesn't reduce wall time significantly since continents are already bounded at 4 concurrent.

---

## Round 3: Cell pre-tagging — REVERTED ❌
**Commit**: `f17eb6c` (reverted in `996c374`)

Pre-computing continent membership for 300M+ cells into a hash map took 434s — worse than the savings from faster per-continent lookups. Hash map build + lookup overhead exceeded the S2 cell_in_bbox cost.

---

## Remaining Bottlenecks (in current best: 28m 19.6s)

### Cannot easily optimize
- **Pass 1** (46.6s): Single-threaded PBF relation scanning — osmium limitation
- **S2 covering drain** (134.4s): Already fully parallel thread pool
- **S2 street ways** (136.9s): Already fully parallel
- **Admin S2 append** (2.9s): Sequential vector append, trivial time

### Potential optimizations
1. **Rebuild cell maps** (169.8s): The merge phase after parallel build is sequential hash map insertion. Could use a different data structure.
2. **Continent filter** (~150s per continent): Dominated by hash map iteration + cell_in_bbox. Already parallelized internally (8 threads). Main cost is scanning ALL cells for each continent.
3. **Continent write** (~20-170s per continent): Quality variant simplification is already parallel.

### Architecture note
The continent filter phase is fundamentally O(total_cells × num_continents) because each continent must scan all cell maps. This is hard to avoid without changing the data structure (e.g., spatial index on cells).
