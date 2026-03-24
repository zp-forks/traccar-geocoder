# Build Optimization Log

## Current Investigation: Single-core continent filter

### Problem
The pipeline continent processing shows only 1 core active in htop despite spawning 8 threads per scan.
Debug confirms threads are spawned but they get no CPU time.

### Status: Investigating
- Threads ARE spawned (debug output confirms 8 threads × 3 scans)
- Output is correct (file sizes match)
- But only 1 core active on machine during entire continent phase

### Hypothesis
The `remap_cells` phase at line 267-271 in continent_filter.cpp iterates `cell_to_addrs` hash map (48.9M entries) single-threaded with `cell_in_bbox` per entry. This is likely where most time is spent, not in the parallel scan.

### Working on
Adding per-sub-phase timing within filter_by_bbox to identify exactly which stage is slow.
