# Geocoder Incremental Patch System

## Goal

Produce small patch files nightly so users can update their geocoder index without
re-downloading the full dataset. A user starting from any build can apply patches
in sequence and arrive at output **byte-identical** to a fresh build.

## Current Status

### What Works
- **Deterministic builds**: Same PBF always produces byte-identical output (verified on Germany and Europe)
- **Single-patch application**: Verified on both Europe and Planet — all 14 files byte-identical
- **Sequential patching**: Verified on planet with 3 distinct weekly snapshots (Mar 9 → Mar 16 → Mar 23) — **PASS**
- **Custom patch format**: Merge sequences, string-level diffs, parent-aware coordinate merges, cell corrections — fully custom diff/apply logic (only zstd for transport compression)

### What Needs Work
- **Build determinism on planet**: Germany and Europe pass; need to confirm planet-scale
- **Sequential test with optimized diff**: The sequential test used the old diff tool; need to re-run with parent merges + string diff
- **Code cleanup**: Prototype files (`full_remap_test.cpp`, `remap_test.cpp`), dead code blocks, inconsistent patterns

## Tested Patch Sizes

### Europe (7 GiB dataset)

| Gap | Patch Size | Per Day | All Match? |
|-----|-----------|---------|------------|
| 6 days (Mar 21→27) | **68 MiB** | **~11 MiB** | YES — latest approach |

### Planet (17 GiB dataset)

| Gap | Patch Size | Per Day | All Match? |
|-----|-----------|---------|------------|
| 7 days (Mar 9→16) | **160 MiB** | **~23 MiB** | YES — optimized diff + dedup, sequential step 1 |
| 7 days (Mar 16→23) | **159 MiB** | **~23 MiB** | YES — optimized diff + dedup, sequential step 2 |
| Sequential (Mar 9→16→23) | N/A | N/A | **PASS** — optimized diff + dedup, both steps byte-identical |

### Per-File Breakdown (Planet 7-day gap, Mar 16→23, latest approach)

| Component | Raw Size | % of Patch | Notes |
|-----------|---------|-----------|-------|
| **Way fixups (47.7M pairs)** | **364 MiB** | **46.3%** | **Biggest item — node_offset fixes** |
| **street_nodes merge** | **252 MiB** | **32.0%** | **Parent-aware, 6.65% of file** |
| String remap (explicit)* | 91 MiB | 11.5% | *Eliminated in latest code — will be 0* |
| street_entries corrections (2M cells) | 31 MiB | 4.0% | |
| admin_vertices merge | 16 MiB | 2.0% | Parent-aware, 2.34% of file |
| admin_polygons fixups (945K pairs) | 7.2 MiB | 0.9% | vertex_offset fixes |
| admin_entries corrections (193K cells) | 7.1 MiB | 0.9% | |
| addr_points merge | 5.9 MiB | 0.7% | 0.24% of file — excellent |
| street_ways merge | 5.1 MiB | 0.6% | 0.92% of file — excellent |
| addr_entries corrections (116K cells) | 2.9 MiB | 0.4% | |
| Flag corrections (224K cells) | 1.9 MiB | 0.2% | |
| Cell changes (184K added, 46K removed) | 1.8 MiB | 0.2% | |
| String diff (~11K added, ~4K deleted) | 0.2 MiB | <0.1% | |
| interp + other | 1.1 MiB | 0.1% | |
| **Total uncompressed** | **786 MiB** | | |
| **Compressed (zstd transport)** | **186 MiB** | | *~150 MiB estimated without explicit remap* |

\* The 91 MiB explicit string remap was eliminated in the latest code (remap is now derived from the string-level diff). This test was run before that change. Estimated savings: ~36 MiB compressed.

### Per-File Breakdown (Europe 6-day gap, Mar 21→27, latest approach)

| File | Size | Merge/Diff | % | Notes |
|------|------|-----------|---|-------|
| strings.bin | 83 MiB | ~60 KB (string diff) | <0.01% | String-level merge |
| addr_points.bin | 1.3 GiB | 4.4 MiB | 0.32% | Excellent |
| street_ways.bin | 223 MiB | 2.7 MiB | 1.16% | Excellent |
| street_nodes.bin | 1.4 GiB | 108 MiB | 7.49% | Parent-aware merge |
| admin_polygons.bin | 11 MiB | 95 KB | 0.88% | Good |
| admin_vertices.bin | 278 MiB | 7.1 MiB | 2.44% | Parent-aware merge |
| Way fixups | — | 19 MiB | — | |
| Cell corrections | — | 15 MiB | — | |
| **Compressed (zstd)** | | **68 MiB** | | |

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
1. Build string-level diff (walk both sorted pools, emit added/deleted strings)
2. Build string remap (old string pool offsets → new, derived from string diff)
3. For each data file: apply string remap to old, match records by content hash (for ways/admin: also fix node_offset/vertex_offset fields), build merge sequence (MATCH/INSERT/DELETE at record stride)
4. For coordinate files (nodes/vertices): derive merge from parent way/polygon merge — verify node blocks match before COPY, INSERT if nodes changed
5. Derive ID remaps from merge sequences (MATCH ops define old→new record correspondence)
6. Rebuild entry files from derived ID remaps + cell changes
7. Compare derived entries with new entries cell by cell — include differing cells as corrections
8. Compare cell flags (has_street/has_addr/has_interp) between old and new — include differences
9. Package everything, compress whole patch with zstd for transport

**Patch tool:**
1. Decompress zstd
2. Read string-level diff → reconstruct new string pool from old + additions/deletions, derive remap
3. For each data file: apply string remap + offset fixups to old, replay merge sequence → output file, track ID mapping from MATCH ops
4. Read cell changes (added/removed cell IDs) and flag corrections
5. Rebuild entry files from tracked ID mappings + cell changes
6. Apply cell-level entry corrections (replace specific cells' data)
7. Rebuild geo_cells/admin_cells from corrected entries + flag corrections

## Known Issues

### 1. Cell correction overhead

**Impact**: 15-43 MiB of cell correction data per patch.

**Problem**: The merge-derived ID remap doesn't capture all record matches (~80K-173K unmatched ways). Unmatched records cause cells to have wrong entry IDs, requiring cell-level corrections.

**Root cause**: The merge matches records by byte equality. If even one byte differs (e.g., node_offset not matched by fixup), the record is unmatched. The hash-based fixup matches ~99.6% of records, but the 0.4% unmatched cascade to many cells.

**Potential fix**: Improve fixup matching coverage, or accept corrections as a cost of the approach.

### 2. Way fixup table size

**Impact**: 19-48 MiB of fixup data per patch.

**Problem**: Every matched way needs a `(record_index, new_node_offset)` fixup because the canonical sort produces different node layouts between builds. With 19-48M matched ways, this is significant.

**Potential fix**: Delta-encode the fixup values (record indices are sequential, offsets are monotonically increasing). Expected 3-5x compression.

### 3. street_nodes.bin still 6-7%

**Impact**: 108-264 MiB of merge data.

**Problem**: Parent-aware merge reduced this from 33% to 7%, but it's still the largest single component. The remaining 7% is from ways whose header matches (byte-identical) but whose actual node coordinates changed (geometry edits in OSM).

**Potential fix**: None needed — this represents actual data changes, not algorithm inefficiency.

### 4. Struct padding non-determinism (FIXED)

**Status**: Fixed by adding explicit padding fields to `AdminPolygon` and `InterpWay` structs.

## Patch Format (.gcpatch, version 2)

Whole file is zstd-compressed for transport. Internal structure:

```
Header: "GCPATCH\0" (8) + version=2 (u32) + flags=0 (u32)

String Remap: marker 0xFFFFFFFE (u32) + count=0 (u32)
  (remap derived from string diff, explicit table no longer needed)

Per-file merge: file_id (u32) + stride (u32) + old_size (u64) + new_size (u64)
  + n_fixups (u32) + [(record_idx, new_offset_value)] × n_fixups
  + seq_size (u64) + merge_ops
  Ops: MATCH(count:u32) | INSERT(count:u32, data) | DELETE(count:u32)
  stride=0: full replacement (no fixups, data follows directly)

String Diff: marker 0xFFFFFFF7 (u32) + n_added (u32) + n_deleted (u32)
  + [string_data\0] × n_added + [deleted_index:u32] × n_deleted

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
| Diff generation | ~8 min | ~38 min |
| Patch application | ~5 min | ~20 min |

## TODO (Priority Order)

### Completed
1. ~~Sequential planet test~~ — **PASS** (3 distinct weekly snapshots, old diff tool)
2. ~~Parent-aware coordinate merge~~ — **DONE** (street_nodes 33%→7%, admin_vertices 210%→2.4%)
3. ~~String-level merge~~ — **DONE** (83 MiB → ~100 KB)
4. ~~Planet test with optimizations~~ — **DONE** (852 MiB → 186 MiB)
5. ~~Build determinism~~ — **PASS** (Germany, Europe, Planet — all verified)
6. ~~Eliminate explicit string remap~~ — **DONE** (derived from string diff)
7. ~~Struct padding fix~~ — **DONE** (explicit padding in AdminPolygon/InterpWay)
8. ~~Addr_points dedup~~ — **DONE** (4.4M duplicates on planet, fixed non-determinism)

### Must Do
- ~~Sequential test with optimized diff on planet~~ — **PASS** (160 MiB + 159 MiB, both steps byte-identical)
- **Verify patched planet still serves correct data** — Spot-check geocoding results against fresh build

### Should Do (Patch Size)
- **Delta-encode fixup tables** — 364 MiB raw (46% of planet patch). Record indices are sequential, offsets monotonically increasing — delta encoding could reduce to ~10 MiB
- **Reduce cell correction count** — 2M street cell corrections (31 MiB). Improve fixup matching to reduce unmatched records

### Nice to Have (Performance)
- **Parallel merge building** — Currently sequential across files
- **Streaming patch application** — Currently loads entire decompressed patch into memory
- **Code cleanup** — Remove prototype files, dead code, inconsistent patterns

### Future Consideration (Format Changes)
- **Inline node storage** — Store way nodes inline instead of separate flat array. Eliminates coordinate cascade entirely. Requires server format change.
- **Content-addressed record IDs** — Hash-based IDs instead of sequential indices. Makes entry files stable across builds. Requires server format change.
- **Fixed-size cell slots** — Pre-allocate per cell. Analysis showed 2-6x space waste due to skewed distribution.

## File Inventory

### Core files (committed)
- `src/build_index.cpp` — Deterministic ordering
- `src/types.h` — Explicit struct padding
- `src/s2_helpers.cpp` — Zero padding in AdminPolygon creation
- `tools/geocoder_diff.cpp` — Diff tool
- `tools/geocoder_patch.cpp` — Patch tool
- `tools/patch_format.h` — Shared format definitions + rebuild functions
- `tools/geocoder_canonicalize.cpp` — Standalone canonicalize (testing tool)
- `CMakeLists.txt` — Build targets

### Test data on Node 3 (/home/michtest/)
- `planet-old/` — Built from planet-260309 (Mar 9), 17 GiB
- `planet-B/` — Built from planet-260316 (Mar 16), 17 GiB
- `planet-C/` — Built from planet-260323 (Mar 23), 17 GiB
- `det-A/` — Europe built from Mar 21 PBF, 7 GiB
- `det-today/` — Europe built from Mar 27 PBF, 7 GiB
