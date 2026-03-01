# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CLI tool for exporting HEC-HMS (Hydrologic Modeling System) results to GeoParquet format, with DuckDB querying, PMTiles generation, and PostGIS sync. Built on top of `hms-commander` for HMS model parsing.

## Commands

```bash
# Install in dev mode with all optional dependencies
pip install -e ".[all]"

# Run all tests
pytest tests/ -v

# Run a single test
pytest tests/test_geometry.py::test_get_subbasins_points -v

# Build distribution
python -m build
```

## Architecture

The package (`hmscmdr_parquet/`) follows a pipeline architecture:

```
HMS model files (.basin, .results XML)
  → geometry.py / results.py    (parse via hms-commander → GeoDataFrame)
    → GeoParquet                (intermediate storage format)
      → duckdb_session.py      (SQL queries on parquet)
      → postgis_sync.py        (upload to PostGIS)
      → pmtiles.py             (vector tiles for web viz)
```

**cli.py** — Typer CLI app exposing five commands: `geometry`, `results`, `query`, `pmtiles`, `sync`. Uses Rich console with `emoji=False` for Windows SSH compatibility (cp1252 encoding).

**geometry.py** — Extracts subbasins (points), reaches (linestrings), junctions (points), and watershed boundaries (polygons) from `.basin` files using `HmsBasin` and `HmsGeo` from hms-commander. Auto-detects CRS from the HMS project, defaults to WGS84/EPSG:4326.

**results.py** — Parses `RUN_*.results` XML files for summary statistics (peak, min, mean, time of peak). Does NOT read DSS files. Merges statistics with geometry from the basin file. Maps CLI variable names to HMS XML prefixes (e.g., "Flow Out" → "Outflow").

**duckdb_session.py** — Thin wrapper: `DuckSession` class manages connections with spatial extension pre-loaded. `query_parquet()` and `spatial_join()` are convenience functions.

**pmtiles.py** — Requires external CLI tools (`tippecanoe` and `pmtiles`). Pipeline: GeoParquet → GeoJSON (temp) → MBTiles → PMTiles. Temp files cleaned up via `tempfile.TemporaryDirectory()`.

**postgis_sync.py** — SQLAlchemy-based, prefers psycopg v3. Creates GIST spatial indices automatically. Supports "replace" or "append" modes.

**__init__.py** — Re-exports all public functions for programmatic use.

## Key Patterns

- **File discovery**: Functions accept both file paths and directory paths; they walk up the directory tree to find HMS project files (.basin, .map, .results).
- **CRS handling**: Auto-detected from HMS project via `init_hms_project()`. If detection fails, no reprojection occurs (rather than erroring).
- **Optional dependencies**: duckdb, postgis, and pmtiles extras are guarded by import checks. Core functionality (geometry + results export) works with base dependencies only.
- **Variable normalization**: User-facing names like "Flow Out" or "Stage" are mapped to HMS XML element prefixes in `results.py`.

## Testing

Tests create minimal `.basin` and `.results` XML data inline (no fixture files needed). Tests exercise full export pipelines and validate geometry types/values. Currently 4 tests across 2 modules.
