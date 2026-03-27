# Geocoder Incremental Patch System

## Overview

A system for producing small daily patch files so users can update their geocoder
index (~7 GiB for Europe) without re-downloading the full dataset. Users apply
patches in sequence: day1 + patch_1→2 → day2, day2 + patch_2→3 → day3, etc.

## Architecture

```
build-index (deterministic) → geocoder-diff old/ new/ → patch.gcpatch
geocoder-patch old/ patch.gcpatch → new/  (byte-identical to fresh build)
```

### Tools
- **build-index**: Modified to produce deterministic output (canonical sort order)
- **geocoder-diff**: Compares two deterministic builds, produces .gcpatch file
- **geocoder-patch**: Applies .gcpatch to old build, produces new build

### Key Design Decisions
1. **Deterministic builds**: Same PBF → byte-identical output (verified)
2. **Custom patch format**: Merge sequences at record level, not raw binary diff
3. **String remapping**: Old string offsets mapped to new (alphabetical sort differs between builds)
4. **zstd transport compression**: Whole patch compressed for download

## Current State (2026-03-27)

### What Works
- Deterministic builds: verified byte-identical across runs
- Data files: all 8 data files produce byte-identical output after patching
- Entry files: corrected via zstd deltas against derived rebuild
- admin_cells: corrected via zstd delta
- **13 of 14 files match** — only geo_cells.bin remains

### Patch Size: 2-Day Europe Gap (Mar 25 → Mar 27)

| Component | Uncompressed | In Patch |
|-----------|-------------|----------|
| String remap (5M entries) | 40 MiB | ~12 MiB |
| Way offset fixups (19.4M) | 155 MiB | ~43 MiB |
| Admin offset fixups (449K) | 3.6 MiB | ~1 MiB |
| strings.bin (full replacement) | 83 MiB | ~15 MiB |
| addr_points merge (0.04%) | 517 KB | <1 MiB |
| street_ways merge (0.15%) | 356 KB | <1 MiB |
| street_nodes merge (2.24%) | 32 MiB | ~3 MiB |
| admin_vertices merge (0.65%) | 1.9 MiB | <1 MiB |
| admin_polygons merge (0.17%) | 18 KB | <1 MiB |
| interp files | <1 KB | <1 MiB |
| Cell changes (9K added, 2K removed) | 85 KB | <1 MiB |
| Entry corrections (4 files) | 1.2 MiB | <1 MiB |
| **geo_cells.bin correction** | **396 MiB** | **~370 MiB** |
| admin_cells correction | 500 KB | <1 MiB |
| **Total** | | **~452 MiB** |

Without geo_cells: **~78 MiB**. Estimated 1-day gap: **~40 MiB**.

## Patch Format (.gcpatch, version 2)

Whole file is zstd-compressed for transport. Uncompressed structure:

```
Header: "GCPATCH\0" (8) + version (u32) + flags (u32)

Section: String Remap
  marker: 0xFFFFFFFE (u32)
  count: u32
  entries: [(old_offset: u32, new_offset: u32)] × count

Section: Per-file merge sequences
  file_id: u32 (PatchFileId enum, 0-13)
  stride: u32 (record size, 0=full replacement, 0xFE=correction)
  old_size: u64, new_size: u64
  n_fixups: u32
  fixups: [(record_index: u32, new_offset_value: u32)] × n_fixups
  seq_size: u64
  ops: MATCH_RUN(count) | INSERT_RUN(count, data) | DELETE_RUN(count)

Section: Cell Changes
  marker: 0xFFFFFFFB/0xFFFFFFFA (geo/admin)
  n_added: u32, n_removed: u32
  added_cells: [cell_id: u64] × n_added
  removed_cells: [cell_id: u64] × n_removed

Section: Correction deltas (stride=0xFE)
  file_id: u32, stride: 0xFE
  derived_size: u64, new_size: u64
  n_fixups: u32 (always 0)
  delta_size: u64
  delta: zstd frame (derived → new via --patch-from)

End marker: 0xFFFFFFFF (u32)
```

## How Patching Works

### Diff Tool (geocoder-diff)
1. Build string remap (old string pool → new string pool offsets)
2. For each data file:
   a. Apply string remap to old file
   b. For ways/interps/admin: hash-match records by content, fix node_offset/vertex_offset
   c. Build merge sequence (MATCH/INSERT/DELETE at record stride)
   d. Record offset fixups for matched records
3. Derive ID remaps by replaying merge sequences (same as patch tool)
4. Rebuild entries from derived ID remaps + cell changes
5. Compute entry corrections: zstd(derived_entries → new_entries)
6. Compute cell index corrections: zstd(derived_cells → new_cells)
7. Package everything, compress with zstd

### Patch Tool (geocoder-patch)
1. Decompress zstd transport layer
2. Read string remap, detect strides
3. For each merge section:
   a. Apply string remap to old file
   b. Apply offset fixups (node_offset/vertex_offset)
   c. Replay merge sequence → output file
   d. Track old→new ID mapping from MATCH ops
4. Read cell changes (added/removed cells)
5. Rebuild entry files from derived ID remaps + cell changes
6. Rebuild admin files similarly
7. Apply entry corrections (zstd deltas)
8. Rebuild geo_cells from corrected entries
9. Apply cell index corrections (zstd deltas)

## Approaches Tried

### 1. Raw zstd --patch-from (baseline)
**Result**: 1.9 GiB patches (27% of 7 GiB). String offsets and record IDs differ
between independent builds, causing cascading byte differences in every file.

### 2. Canonical ordering in builder
Added deterministic sorting (string pool alphabetical, records by content).
**Result**: Same PBF → byte-identical output. But patches between different PBFs
still 1.7 GiB because string offsets cascade.

### 3. String remap + zstd --patch-from
Apply string offset remapping before binary diff.
**Result**: Data files drop to 0.2-0.7%. But entry/cell files still 24-79%.
Total: ~941 MiB.

### 4. Record ID remap + entry rebuild
Match records by content, remap IDs in entry files.
**Result**: Entry files drop to 0.02-0.07%. Total with compressed tables: ~626 MiB.
But remap tables are 240 MiB compressed (187 MiB for 86M addr pairs).

### 5. Custom merge sequences (current approach)
Walk sorted records in parallel, emit MATCH/INSERT/DELETE ops at record level.
Hash-indexed matching for data files. Sequential scan for coordinate files.
**Result**: Data files at 0.04-2.24%. Merge sequences implicitly encode ID remaps.
Eliminates explicit ID remap tables. Total: ~78 MiB without entry/cell files.

### 6. Entry corrections
Include zstd deltas: derived_entries → new_entries. Tiny corrections (~1 MiB).
**Result**: All entry files match. geo_cells.bin remains at 396 MiB because derived
geo_cells has cascading offset differences.

## Remaining Work

### geo_cells.bin (the last bottleneck)
- **Problem**: 1.6 GiB file with 84M × 20-byte records. Each record has 12 bytes of
  entry offsets that cascade when any cell's entries change.
- **Current**: 396 MiB correction delta (derived offsets → correct offsets)
- **Root cause**: Derived geo_cells is built from pre-correction entries. After corrections,
  entries are correct, but we need to recompute offsets. The rebuild algorithm in diff
  and patch tools must produce byte-identical output.
- **Potential fix**: Share exact rebuild algorithm, or include only the ~9K cells whose
  has/hasn't flags differ (~100 KB of correction data).

### Sequential patching test
- Have 3 Europe builds: det-A (Mar 21), det-new (Mar 25), det-today (Mar 27)
- Need: patch(det-A, p12) → result2, patch(result2, p23) → result3, verify result3 == det-today
- Blocked by: geo_cells.bin mismatch

### Code cleanup
- Remove prototype files (full_remap_test.cpp, remap_test.cpp)
- Remove dead code blocks (if (false) sections)
- Remove geocoder_canonicalize.cpp (superseded by build_index determinism)
- Clean up the diff/patch tools (currently ~700 lines each, could be ~400)

## File Inventory

### Modified
- `src/build_index.cpp` — deterministic ordering (sort strings, records, nodes, vertices)
- `CMakeLists.txt` — new build targets

### New (committed)
- `tools/geocoder_diff.cpp` — diff tool (~700 lines)
- `tools/geocoder_patch.cpp` — patch tool (~500 lines)
- `tools/patch_format.h` — shared format definitions + rebuild functions (~400 lines)
- `tools/geocoder_canonicalize.cpp` — standalone canonicalize (testing tool)

### Prototypes (uncommitted, can be removed)
- `tools/full_remap_test.cpp` — proved remap approach
- `tools/remap_test.cpp` — string-only remap test

## Performance

### Build Time Overhead
- Deterministic sorting: ~100s (string sort 5s, addr sort 55s, way sort 16s)
- Total build: ~12 min + 100s = ~14 min (+13%)

### Diff Time
- 2-day Europe gap: ~5 min (dominated by merge building + zstd compression)

### Patch Application Time
- ~2 min (dominated by merge replay + entry reconstruction + zstd decompression)

## Three Europe Builds Available for Testing
- `det-A`: March 21 PBF (europe.osm.pbf, 34.0 GiB)
- `det-new`: March 25 PBF (europe-new.osm.pbf, 34.1 GiB)
- `det-today`: March 27 PBF (europe-today.osm.pbf, 34.1 GiB)

Node 3: 32 cores, 174 GiB RAM, files at /home/michtest/
