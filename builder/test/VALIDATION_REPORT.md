# Parallel Admin Assembly Validation Report

**Date**: 2026-03-23
**Commits**: 99 on main (unpushed, backed up to node 3)

## Planet (86GB PBF) — Final Results

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 47,826,371 | 47,826,371 | **100% PASS** |
| Address points | 160,091,817 | 160,091,817 | **100% PASS** |
| Interpolation ways | 72,817 | 72,817 | ~100% |
| Admin (unique name\|level) | 642,275 | 644,809 | **10 missing** (0.0016%), 2,544 extras |
| Build time | ~5 hours | **~13.7 minutes** | **22x faster** |

### Performance Breakdown (Planet)

| Phase | Time | % | Parallelism |
|---|---|---|---|
| Pass 1: relation scanning | 45s | 5% | single-threaded (PBF) |
| Pass 2: node processing | 177s | 22% | 22+ cores |
| Pass 2b: way processing | 128s | 16% | 28 cores |
| Admin assembly | 161s | 20% | 15-28 cores |
| S2 cell computation | 249s | 30% | 28 cores (compute) + 1 core (merge) |
| Dedup + interp resolve | 50s | 6% | 2 cores |
| Index writing | 11s | 1% | 32 cores |

### Optimization Summary

| Optimization | Impact |
|---|---|
| Parallel node/way processing | 28 cores for PBF processing |
| Combined Pass 2+3 (single PBF read) | Save ~30s re-reading way blocks |
| Parallel admin assembly | 28 cores for ring stitching |
| thread_local S2RegionCoverer | Avoid 335M constructor calls |
| cover_edge fast-path (same/adjacent cell) | Skip S2 library for ~80% of edges |
| Flat-array S2 cell collection | Replace hash map merge with sort |
| Parallel tree merge for S2 pairs | 13x faster sort+group (109s→8.4s) |
| Sweep-line self-intersection check | O(n log n) vs O(n²) for large rings |
| Parallel chunked merge-join for writes | 26x faster index writing (232s→9s) |
| Sort cell IDs only for addr dedup | O(M log M) vs O(N log N) |
| Parallel index file writes | 8+ files written concurrently |
| Buffered IO | Single write calls vs millions of small writes |

## Correctness

### 10 Missing Admin Boundaries

All level 8-10 small municipality subdivisions:
- Argentina (6), Peru (1), Brazil (2), Indonesia (1)
- Root cause: greedy ring stitcher makes different branch choices than osmium
- **Zero impact on reverse geocoding** — parent admin areas cover these locations
- Verified: server output identical for all 10 coordinates

### 2,544 Extra Admin Boundaries

Legitimate boundaries our improved stitcher recovers:
- Coordinate-matching handles split/replaced nodes
- Closed-way admin polygon support
- Inner-role way retry
- **Strictly improves** reverse geocoding — adds city/town names where baseline returns only country

## Bugs Found and Fixed

1. **String interleaving** — way names swapped with building addr strings (~62% of ways)
2. **Building address corruption** — `"\0"` null separator never embedded (~16M addresses)
3. **Ring stitcher** — coordinate matching, split-at-shared-nodes, greedy+backtracking
4. **Closed-way admin polygons** — standalone boundary ways not handled
5. **Inner-way retry** — osmium ignores roles, inner ways bridge outer gaps
6. **Border-line filter** — level 2 non-country relations
7. **Self-intersection detection** — invalid assembled rings discarded
8. **Duplicate coordinate filter** — prevents hole+outer boundary merging

## Code Changes

- Sequential osmium assembly path removed — always parallel
- BuildHandler class removed (-212 lines)
- MultipolygonManager no longer needed
- 3033 lines total (down from 3245)
- 12 unit tests + integration test + e2e test
- All backed up to node 3 at ~/traccar-geocoder-backup/
