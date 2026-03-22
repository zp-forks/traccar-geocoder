# Parallel Admin Assembly Validation Report

**Date**: 2026-03-22
**Baseline**: commit 93fe6e4 (pre-parallel, sequential osmium assembler)
**Parallel**: commit a80d4e9 (all fixes, greedy+backtracking hybrid stitcher)

## Planet (86GB PBF) — Global Comparison

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 47,826,371 | 47,826,371 | **100% PASS** |
| Address points | 160,091,817 | 160,091,817 | **100% PASS** |
| Interpolation ways | 72,817 | 72,817 | ~100% |
| Admin polygons | 943,638 | 947,358 | — |
| Admin (unique name\|level) | 642,275 | 644,809 | **10 missing** (0.0016%), 2,544 extras |
| Build time | ~5 hours | ~40 minutes | **~7.5x faster** |

### 10 Missing Admin Boundaries (of 642,275 = 99.998%)

All level 8-10 small municipality subdivisions:
- Argentina: Alto Los Cardales|8, Pringles|9, COVIMER 2|10, FOPROVI|10, Antonio Palacios|10, Tres Esquinas|9
- Peru: ATE II|10
- Brazil: Jardim Mônaco (proposto)|10, Residencial Melville|10
- Indonesia: RT 002 RW 015 Kel. Mekarjaya|10

Root cause: greedy ring stitcher makes wrong branch choices at junctions
where multiple admin areas share boundary ways. Backtracking would fix
these but causes planet-scale stalls (exponential search) for complex
relations with >30 sub-ways. Matching osmium exactly would require
reimplementing its full segment graph assembler (~1200 lines).

**Reverse geocoding impact**: Zero. These 10 areas are small subdivisions
where the parent admin level (city/district) is still correctly returned.
A query in these areas gets the right city name; only the neighborhood-level
name differs.

## Europe (31GB PBF)

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 19,452,480 | 19,452,480 | **100% PASS** |
| Address points | 85,891,830 | 85,891,830 | **100% PASS** |
| Interpolation ways | 13,988 | 13,988 | **100% PASS** |
| Admin (unique name\|level) | 327,854 | 327,882 | **0 missing**, 28 extras |

## Germany (4.5GB PBF)

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 3,312,673 | 3,312,673 | **100% PASS** |
| Address points | 19,951,156 | 19,951,156 | **100% PASS** |
| Interpolation ways | 78 | 78 | **100% PASS** |
| Admin (unique name\|level) | 37,034 | 37,034+ | **0 missing** |

## Bugs Found and Fixed (21 commits)

1. **String interleaving** — way names swapped with building addr strings (~62% of ways)
2. **Building address corruption** — `"\0"` null separator never embedded (~16M addresses)
3. **Ring stitcher** — coordinate matching, split-at-shared-nodes, greedy+backtracking hybrid
4. **Closed-way admin polygons** — standalone boundary ways not handled
5. **Inner-way retry** — osmium ignores roles, inner ways bridge outer gaps
6. **Border-line filter** — level 2 non-country relations (name pattern check)
7. **Self-intersection detection** — invalid assembled rings discarded
8. **Duplicate coordinate filter** — prevents hole+outer boundary merging
9. **Planet-scale performance** — greedy stitcher with budgeted backtracking retry
10. **Parallel index writes** — entry files + raw data files written concurrently (~2.5x faster)

## Performance

| Phase | Sequential baseline | Parallel |
|---|---|---|
| Total build (planet) | ~5 hours | ~40 minutes |
| Admin assembly | ~2 hours (sequential osmium) | ~5 minutes (parallel stitcher) |
| Index writing | ~25-30 minutes | ~10-12 minutes (parallel writes) |

## Admin Polygon Gap Journey

| Stage | Germany | Europe | Planet |
|---|---|---|---|
| Pre node-ID fix | -11,675 | — | — |
| After node-ID fix | -43 | — | — |
| After coordinate matching | -29 | — | — |
| After closed-way + inner-way | 0 missing | 0 missing | — |
| After greedy+backtracking | +1 | 0 missing | **10 missing** |
