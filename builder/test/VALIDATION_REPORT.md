# Parallel Admin Assembly Validation Report

**Date**: 2026-03-21
**PBF**: germany-latest.osm.pbf (4.5GB)
**Baseline**: commit 93fe6e4 (pre-parallel, sequential osmium assembler)
**Parallel**: commit e1c0b75 (all fixes applied)

## Results: Parallel vs Sequential (same code, same PBF)

| Component | Parallel | Sequential | Status |
|---|---|---|---|
| Street ways | 3,312,673 | 3,312,673 | **100% PASS** |
| Address points | 19,951,156 | 19,951,156 | **100% PASS** |
| Interpolation ways | 78 | 78 | **100% PASS** |
| Admin polygons | 43,222 | 43,221 | 99.998% (3 freq diffs, net +1) |

## Results: Cross-version (Baseline vs Parallel, same PBF)

| Component | Baseline | Parallel | Status |
|---|---|---|---|
| Street ways | 3,312,673 | 3,312,673 | **100% PASS** |
| Address points | 19,951,156 | 19,951,156 | **100% PASS** |
| Interpolation ways | 78 | 78 | **100% PASS** |
| Admin (unique name\|level) | 37,034 | 37,034+ | **0 missing** (12 extras from cross-border) |

## Remaining 3 Admin Polygon Differences

### 1. Krefeld-Ost / Krefeld-Uerdingen (level 9) — ring name swap, net 0

A 7-vertex polygon (~1.5 sq meters) at the exact junction of two district boundaries.
Our build labels it "Krefeld-Ost"; osmium labels it "Krefeld-Uerdingen". Both are equally
valid — the point is on the boundary line itself. GPS accuracy (~3-5m) means no real query
would reliably land in this polygon.

### 2. Neuhaus (level 10) — extra micro-polygon, net +1

A 12-vertex polygon (~2.7 sq meters) at the edge of a "Neuhaus" boundary near Wolfsburg.
Our build returns "Neuhaus" for this area; osmium's build has no level-10 result here.
The level-8/9 results are identical. Arguably more correct since the location IS in Neuhaus.

### Practical Impact

All 3 differences affect a combined ~4 square meters of Earth's surface at boundary
junction points. No real-world GPS coordinate would reliably land in these areas.
**Zero practical impact on reverse geocoding.**

## Bugs Found and Fixed

### 1. String interleaving (commit 3984d21)
Parallel way processing pushed way names, building address strings, and interpolation
strings into a single `interned_strings` vector in interleaved processing order. The merge
code read them assuming grouped-by-type order, causing ~62% of way names to be swapped
with building address strings in any block containing mixed types.

### 2. Building address string corruption (commit d0b3540)
Building addresses stored housenumber+street with an embedded null separator:
`std::string(hn) + "\0" + std::string(street)`. But `std::string("x") + "\0"` produces
`std::string("x") + std::string("")` — the null terminator makes it an empty string.
Both housenumber and street fields ended up containing the full concatenated string,
corrupting ~16M building addresses in Germany.

### 3. Ring stitcher improvements (commits a5817ac, 1f24a5a, 028662a, e22af43)
- Backtracking at branch points (greedy algorithm picked wrong branch)
- Coordinate-based matching (osmium matches by coordinates, not node IDs)
- Split ways at shared internal nodes (segment-level assembly)

### 4. Closed-way admin polygons (commit cd9e361)
Osmium creates areas from both multipolygon relations AND standalone closed ways tagged
with boundary=administrative. Our parallel assembler only handled relations, missing ~87
closed-way admin boundaries in Germany.

### 5. Border-line and geometry filters (commit e1c0b75)
- Skip level-2 boundary-line relations (no ISO code) — these are lines between countries
- Self-intersection detection for assembled rings

## Journey: Admin Polygon Gap

| Stage | Parallel | Sequential | Gap |
|---|---|---|---|
| Pre node-ID fix (planet) | 931,963 | 943,638 | -11,675 |
| After node-ID fix | 43,178 | 43,221 | -43 |
| After backtracking | 43,179 | 43,221 | -42 |
| After coordinate matching | 43,192 | 43,221 | -29 |
| After split-at-shared-nodes | 43,193 | 43,221 | -28 |
| After closed-way support | 43,224 | 43,221 | +3 |
| After border-line filter | 43,222 | 43,221 | +1 |
