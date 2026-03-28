# Geocoder Incremental Patch System

## Goal

Produce small patch files nightly so users can update their geocoder index without
re-downloading the full dataset. A user starting from any build can apply patches
in sequence and arrive at output **byte-identical** to a fresh build.

## Current Status

### What Works
- **Deterministic builds**: Same PBF always produces byte-identical output
- **Single-patch application**: Verified on both Europe and Planet — all 14 files byte-identical
- **Sequential patching**: Verified on planet with 3 distinct weekly snapshots (Mar 9 → Mar 16 → Mar 23) — **PASS**
- **Custom patch format**: Merge sequences, cell corrections, flag corrections — no external tools for diff/apply logic (only zstd for transport compression)

### What Needs Work
- **Sequential planet test**: Running, results pending
- **Oversized merge sequences**: `street_nodes` (33%) and `admin_vertices` (210%) produce merge data larger than the files themselves at planet scale — the sequential merge algorithm's 200-record lookahead fails for large coordinate shifts
- **Patch size optimization**: Planet 7-day patches are 852 MiB; target is much smaller
- **Build determinism audit**: Need to verify there are no remaining non-deterministic paths (struct padding was one; there may be others)
- **Code cleanup**: Prototype files, dead code blocks, inconsistent patterns

## Tested Patch Sizes

### Europe (7 GiB dataset)

| Gap | Patch Size | Per Day | All Match? |
|-----|-----------|---------|------------|
| 2 days (Mar 25→27) | 78 MiB | ~39 MiB | YES (14/14) — old approach |
| 6 days (Mar 21→27) | 79 MiB | ~13 MiB | YES (14/14) — with parent merges + string diff |

### Planet (17 GiB dataset)

| Gap | Patch Size | Per Day | All Match? |
|-----|-----------|---------|------------|
| 7 days (Mar 9→16) | ~850 MiB (est) | ~121 MiB | PENDING |
| 7 days (Mar 9→16) | 852 MiB | ~122 MiB | YES — old approach, sequential step 1 |
| 7 days (Mar 16→23) | 229 MiB | ~33 MiB | YES — old approach, sequential step 2 |
| **7 days (Mar 16→23)** | **186 MiB** | **~27 MiB** | **YES — with parent merges + string diff** |
| Sequential (Mar 9→16→23) | N/A | N/A | PASS — both steps byte-identical |

### Per-File Breakdown (Planet 7-day gap, Mar 16→23)

| File | Size | Merge Seq | % | Issue |
|------|------|-----------|---|-------|
| strings.bin | 205 MiB | full replacement | — | Could merge at string level |
| addr_points.bin | 2.4 GiB | 7.9 MiB | 0.31% | Excellent |
| street_ways.bin | 547 MiB | 5.2 MiB | 0.90% | Excellent |
| **street_nodes.bin** | **3.8 GiB** | **1.3 GiB** | **33.36%** | **BAD — sequential merge fails at scale** |
| interp_ways.bin | 1.7 MiB | 2.6 KB | 0.15% | Excellent |
| interp_nodes.bin | 3.3 MiB | 3.5 KB | 0.10% | Excellent |
| admin_polygons.bin | 22.7 MiB | 205 KB | 0.90% | Good |
| **admin_vertices.bin** | **716 MiB** | **1.5 GiB** | **209.84%** | **BAD — merge larger than file** |
| Cell corrections | — | ~47 MiB | — | Working |
| String remap | — | 95 MiB | — | Fixed overhead |
| Fixup tables | — | ~49 MiB | — | Way/admin offset fixes |

**Key observation**: `street_nodes` and `admin_vertices` are coordinate files (8 bytes: lat+lng). The merge algorithm uses a sequential 200-record lookahead which fails when record insertions/deletions shift large blocks of coordinates. These two files account for ~80% of the planet patch size.

### Per-File Breakdown (Europe 6-day gap, Mar 21→27)

| File | Size | Merge Seq | % | Notes |
|------|------|-----------|---|-------|
| strings.bin | 83 MiB | full | — | |
| addr_points.bin | 1.3 GiB | 4.4 MiB | 0.32% | Excellent |
| street_ways.bin | 223 MiB | 2.7 MiB | 1.16% | Excellent |
| street_nodes.bin | 1.4 GiB | 406 MiB | 28.11% | Large |
| admin_polygons.bin | 11 MiB | 95 KB | 0.88% | Good |
| admin_vertices.bin | 278 MiB | 83 MiB | 28.48% | Large |

## Architecture

```
build-index (deterministic) → geocoder-diff old/ new/ → patch.gcpatch
geocoder-patch old/ patch.gcpatch → new/  (must be byte-identical to fresh build)
```

### Tools
- **build-index** (`src/build_index.cpp`): Builder with deterministic ordering
- **geocoder-diff** (`tools/geocoder_diff.cpp`): Compares two builds, produces `.gcpatch`
- **geocoder-patch** (`tools/geocoder_patch.cpp`): Applies patch, produces new build
- **patch_format.h** (`tools/patch_format.h`): Shared format definitions + rebuild functions

### How Patching Works

**Diff tool:**
1. Build string remap (old string pool offsets → new string pool offsets)
2. For each data file: apply string remap to old, match records by content hash (for ways/admin: also fix node_offset/vertex_offset fields), build merge sequence (MATCH/INSERT/DELETE at record stride)
3. Derive ID remaps from merge sequences (MATCH ops define old→new record correspondence)
4. Rebuild entry files from derived ID remaps + cell changes
5. Compare derived entries with new entries cell by cell — include differing cells as corrections
6. Compare cell flags (has_street/has_addr/has_interp) between old and new — include differences
7. Package everything, compress whole patch with zstd for transport

**Patch tool:**
1. Decompress zstd, read string remap
2. For each data file: apply string remap + offset fixups to old, replay merge sequence → output file, track ID mapping from MATCH ops
3. Read cell changes (added/removed cell IDs) and flag corrections
4. Rebuild entry files from tracked ID mappings + cell changes
5. Apply cell-level entry corrections (replace specific cells' data)
6. Rebuild geo_cells/admin_cells from corrected entries + flag corrections

## Known Issues

### 1. Coordinate file merge inefficiency (BIGGEST ISSUE)

**Files affected**: `street_nodes.bin`, `admin_vertices.bin`, `interp_nodes.bin`

**Problem**: These files contain 8-byte coordinate records (float lat, float lng). The merge algorithm uses sequential comparison with a 200-record lookahead. When records are inserted/deleted, all subsequent records shift. With planet-scale data (500M+ nodes), the lookahead fails to find matches beyond the shift distance, causing DELETE+INSERT for huge ranges.

**Why it happens**: Coordinate records are grouped by their parent record (ways/polygons). When the parent sort order changes (due to new/deleted parents), entire blocks of coordinates shift by thousands of positions. The sequential scan can't bridge these gaps.

**Impact**: street_nodes at 33% = 1.3 GiB of merge data for planet. admin_vertices at 210% = 1.5 GiB (more data than the file itself, due to INSERT overhead).

**Potential fixes**:
- **Parent-aware merging**: Match coordinate blocks by their parent record instead of individual coordinates. If a way is matched, copy its entire node block. Only INSERT nodes for unmatched (new) ways.
- **Increased lookahead with hash verification**: Use content hashing for coordinate files, but with collision detection (same coordinates can appear at many positions).
- **File format change**: Store node data inline with way records instead of in a separate flat array. This would eliminate the cascade problem but requires server format changes.

### 2. String pool as full replacement

**Impact**: 95-215 MiB per patch (depending on dataset size).

**Problem**: The string pool is alphabetically sorted. Any new/removed string shifts all subsequent offsets. Including the full pool is simpler than trying to merge it.

**Potential fix**: String-level merge (variable stride, compare by string content). Since both pools are sorted alphabetically, most strings are at similar positions. A string-aware merge would be very efficient.

### 3. Large cell correction sets

**Impact**: 689K-1.9M cell corrections for 6-7 day gaps.

**Problem**: The merge-derived ID remap doesn't capture all record matches (173K unmatched ways on planet). Unmatched records cause cells to have wrong entry IDs, requiring correction.

**Root cause**: The merge matches records by byte equality. If even one byte differs (e.g., node_offset not matched by fixup), the record is unmatched. The hash-based fixup matches ~99.6% of records, but the 0.4% unmatched cascade to many cells.

**Potential fix**: Improve the fixup matching to cover more records, or accept the corrections as a cost of the approach.

### 4. Struct padding non-determinism (FIXED)

**Status**: Fixed by adding explicit padding fields to `AdminPolygon` and `InterpWay` structs in `types.h`, plus zeroing padding in diff/patch tools for old builds.

**Root cause**: Compiler-inserted padding between struct fields contained uninitialized memory values that differed between builds, causing 0% match rate for admin polygons on planet.

## Patch Format (.gcpatch, version 2)

Whole file is zstd-compressed for transport. Internal structure:

```
Header: "GCPATCH\0" (8) + version=2 (u32) + flags=0 (u32)

String Remap: marker 0xFFFFFFFE (u32) + count (u32) + [(old_off, new_off)] × count

Per-file merge: file_id (u32) + stride (u32) + old_size (u64) + new_size (u64)
  + n_fixups (u32) + [(record_idx, new_offset_value)] × n_fixups
  + seq_size (u64) + merge_ops
  Ops: MATCH(count:u32) | INSERT(count:u32, data) | DELETE(count:u32)
  stride=0: full replacement (no fixups, data follows directly)

Cell Changes: marker 0xFFFFFFFB/0xFFFFFFFA (u32) + n_added (u32) + n_removed (u32)
  + [cell_id:u64] × n_added + [cell_id:u64] × n_removed

Cell Flag Corrections: marker 0xFFFFFFF9 (u32) + count (u32)
  + [(cell_id:u64, flags:u8)] × count
  flags: bit0=has_street, bit1=has_addr, bit2=has_interp

Entry Corrections: marker 0xFFFFFFF8 (u32) + file_id (u32) + count (u32)
  + [(cell_id:u64, entry_count:u16, [id:u32] × entry_count)] × count

End marker: 0xFFFFFFFF (u32)
```

## Performance

| Operation | Europe | Planet |
|-----------|--------|--------|
| Build (deterministic) | ~12 min | ~14 min |
| Diff generation | ~5 min | ~42 min |
| Patch application | ~3 min | ~20 min |
| Verification | ~1 min | ~5 min |

## TODO (Priority Order)

### Done
1. ~~Validate sequential planet test~~ — **PASS** (3 distinct weekly snapshots, Mar 9→16→23)
3. ~~Parent-aware coordinate merge~~ — **DONE** (street_nodes 33%→7.5%, admin_vertices 28%→2.4%)
4. ~~String-level merge~~ — **DONE** (83 MiB → ~100 KB per patch)

### Done (continued)
5. ~~Run planet test with latest optimizations~~ — **DONE** (852 MiB → 186 MiB, all 14 MATCH)
6. ~~Build determinism (Germany)~~ — **PASS** (two runs byte-identical)

### Must Do
2. **Verify build determinism on planet** — Germany passes; need to confirm planet
7. **Sequential test with optimized diff** — The sequential test used old diff; need to re-run with parent merges + string diff

### Nice to Have (Performance)
5. **Parallel merge building** — Merge sequences are built sequentially; could parallelize across files
6. **Streaming patch application** — Currently loads entire patch into memory after decompression
7. **Reduce fixup table size** — 49 MiB of way offset fixups could be delta-encoded

### Future Consideration (Format Changes)
8. **Inline node storage** — Store way nodes inline with way records instead of in a separate flat array. Eliminates coordinate cascade problem entirely. Requires server format change.
9. **Content-addressed record IDs** — Use hash-based IDs instead of sequential array indices. Makes entry files stable across builds. Requires server format change (array index → hash lookup).
10. **Fixed-size cell slots** — Pre-allocate entry space per cell to prevent offset cascading. Analysis showed this wastes 2-6x space due to skewed distribution (74% of cells have 1 street entry, but max is 886 addr entries).

## File Inventory

### Core files (committed)
- `src/build_index.cpp` — Deterministic ordering (sort strings, records, nodes, vertices)
- `src/types.h` — Explicit struct padding for AdminPolygon and InterpWay
- `src/s2_helpers.cpp` — Zero padding in AdminPolygon creation
- `tools/geocoder_diff.cpp` — Diff tool (~800 lines)
- `tools/geocoder_patch.cpp` — Patch tool (~600 lines)
- `tools/patch_format.h` — Shared format definitions + rebuild functions (~450 lines)
- `tools/geocoder_canonicalize.cpp` — Standalone canonicalize (testing tool)
- `CMakeLists.txt` — Build targets

### Test data on Node 3 (/home/michtest/)
- `planet-old/` — Built from planet-260309 (Mar 9), 17 GiB
- `planet-B/` — Built from planet-260316 (Mar 16), 17 GiB (building)
- `planet-new/` — Built from planet-260323 (Mar 23/27), 17 GiB
- `det-A/` — Europe built from Mar 21 PBF, 7 GiB
- `det-today/` — Europe built from Mar 27 PBF, 7 GiB
