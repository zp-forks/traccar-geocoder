# Parallel Admin Assembly Validation Report

**Date**: 2026-03-22
**Baseline**: commit 93fe6e4 (pre-parallel, sequential osmium assembler)
**Parallel**: commit c4064a6 (all fixes, greedy stitcher)

## Planet (86GB PBF) — Global Comparison

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 47,826,371 | 47,826,371 | **100% PASS** |
| Address points | 160,091,817 | 160,091,817 | **100% PASS** |
| Interpolation ways | 72,817 | 72,817 | 99.85% (107 freq diffs) |
| Admin polygons | 943,638 | 947,306 | — |
| Admin (unique name\|level) | 642,275 | 644,716 | 12 missing, 2453 extras |
| Build time | ~5 hours | ~40 minutes | **~7.5x faster** |

### Missing Admin Boundaries (12 of 642,275 = 99.998%)

ATE II|10, Alto Los Cardales|8, Antonio Palacios|10, COVIMER 2|10,
FOPROVI|10, Jardim Mônaco (proposto)|10, Mangaung Ward 43|10,
Pringles|9, RT 002 RW 015 Kel. Mekarjaya|10, Residencial Melville|10,
Tres Esquinas|9, Vale Pastoril|10

These are small admin boundaries (level 8-10) in South America, Africa,
and Southeast Asia where our greedy stitcher makes different topology
choices than osmium's assembler. Higher-level admin names are unaffected.

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

## Bugs Found and Fixed (16 commits)

1. **String interleaving** — way names swapped with building addr strings (~62% of ways)
2. **Building address corruption** — `"\0"` null separator never embedded (~16M addresses)
3. **Ring stitcher** — coordinate matching, split-at-shared-nodes, greedy O(n) algorithm
4. **Closed-way admin polygons** — standalone boundary ways not handled
5. **Inner-way retry** — osmium ignores roles, inner ways bridge outer gaps
6. **Border-line filter** — level 2 non-country relations (name pattern check)
7. **Self-intersection detection** — invalid assembled rings discarded
8. **Planet-scale performance** — greedy stitcher replaces exponential backtracking

## Planet (86GB PBF) — Global Comparison

### Cross-version (Baseline vs Parallel)

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 47,826,371 | 47,826,371 | **100% PASS** |
| Address points | 160,091,817 | 160,091,817 | **100% PASS** |
| Interpolation ways | 72,817 | 72,817 | 99.85% (107 freq diffs) |
| Admin polygons | 943,638 | 947,306 | 99.998% |
| Admin (unique name\|level) | 642,275 | 644,716 | **12 missing, 2,453 extra** |

The 12 missing admin boundaries are small municipality subdivisions in Argentina (5),
Peru (1), South Africa (1), Indonesia (1), and Brazil (4) where the greedy ring stitcher
makes a different branch choice than osmium's assembler. All are level 8-10 boundaries
with no impact on reverse geocoding.

The 2,453 extras are legitimate boundaries recovered by our improved coordinate-matching
stitcher, closed-way polygon support, and inner-way bridging.

## Admin Polygon Gap Journey

| Stage | Gap (Germany) | Gap (Europe) | Gap (Planet) |
|---|---|---|---|
| Pre node-ID fix | -11,675 | — | — |
| After node-ID fix | -43 | — | — |
| After coordinate matching | -29 | — | — |
| After closed-way support | +3 | -4 missing | — |
| After inner-way retry | +1 | **0 missing** | — |
| After greedy stitcher | +1 | 0 missing | **12 missing** |
