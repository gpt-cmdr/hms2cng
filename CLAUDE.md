# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hms2cng** (HMS to Cloud Native GIS) — CLI tool for exporting HEC-HMS (Hydrologic Modeling System) results to GeoParquet format, with DuckDB querying, PMTiles generation, and PostGIS sync. Built on top of `hms-commander` for HMS model parsing.

## Commands

```bash
# Install in dev mode with all optional dependencies (UV - preferred)
uv pip install -e ".[all]"

# Run all tests
uv run pytest tests/ -v

# Run a single test
uv run pytest tests/test_geometry.py::test_get_subbasins_points -v

# Run the CLI
uv run hms2cng --help

# Update the lock file after dependency changes
uv lock

# Build distribution
uv run python -m build

# Run a marimo notebook interactively
uv run marimo edit examples/01_export_geometry.py

# Build and preview docs locally
uv run mkdocs serve

# Build docs static site
uv run mkdocs build
```

## Package Management

- Use `uv pip install -e ".[all]"` for local dev (10-100x faster than pip)
- `uv.lock` is checked into git — commit it when dependencies change (`uv lock`)
- `hms-commander` resolves to `../hms-commander` (local editable) via `[tool.uv.sources]`
- For publishing: remove `[tool.uv.sources]` block so PyPI version is used

## Architecture

The package (`hms2cng/`) follows a pipeline architecture:

```
HMS model files (.basin, .results XML)
  → geometry.py / results.py    (parse via hms-commander → GeoDataFrame)
    → GeoParquet                (intermediate storage format)
      → duckdb_session.py      (SQL queries on parquet)
      → postgis_sync.py        (upload to PostGIS)
      → pmtiles.py             (vector tiles for web viz)
```

### Consolidated Parquet Archive Format

`export_full_project()` produces a **single GeoParquet** with a `layer` discriminator column:

```
output_dir/
├── {project_slug}.parquet    # ALL geometry + results
└── manifest.json             # JSON catalog (schema v2.0)
```

The `layer` column discriminates geometry layers (`subbasins`, `reaches`, `junctions`, `watershed`, ...) from result variables (`outflow`, `stage`, `inflow`, `depth`). Query patterns:

```sql
SELECT * FROM 'project.parquet' WHERE layer = 'subbasins'
SELECT * FROM 'project.parquet' WHERE layer = 'outflow' AND run_name = 'Run 1'
SELECT layer, COUNT(*) FROM 'project.parquet' GROUP BY layer
```

- **Compression**: ZSTD everywhere (replaced snappy)
- **Per-row bbox columns**: `bbox_xmin`, `bbox_ymin`, `bbox_xmax`, `bbox_ymax` for spatial filtering
- **Hilbert sorting**: Rows within each layer sorted by Hilbert curve of centroid (via DuckDB) for spatial locality. Disable with `sort=False` or `--no-sort`.
- **manifest.json**: Schema v2.0 catalog with project metadata, layer inventory, and error log

Individual CLI commands (`geometry`, `results`, `query`, etc.) are unchanged and still produce per-file output.

### Modules

**cli.py** — Typer CLI app exposing commands: `geometry`, `results`, `query`, `pmtiles`, `sync`, `manifest`, `project`. Uses Rich console with `emoji=False` for Windows SSH compatibility (cp1252 encoding).

**geometry.py** — Extracts subbasins (points), reaches (linestrings), junctions (points), and watershed boundaries (polygons) from `.basin` files using `HmsBasin` and `HmsGeo` from hms-commander. Auto-detects CRS from the HMS project, defaults to WGS84/EPSG:4326. `merge_all_layers()` consolidates all geometry layers into a single GeoDataFrame with a `layer` column. `_hilbert_sort()` applies DuckDB-based Hilbert curve sorting.

**results.py** — Parses `RUN_*.results` XML files for summary statistics (peak, min, mean, time of peak). Does NOT read DSS files. Merges statistics with geometry from the basin file. Maps CLI variable names to HMS XML prefixes (e.g., "Flow Out" → "Outflow"). `merge_all_variables()` consolidates all result variables for a run into a single GeoDataFrame.

**catalog.py** — `Manifest` and `ManifestLayer` dataclasses for the manifest.json catalog. Supports create/serialize/load roundtrip.

**duckdb_session.py** — Thin wrapper: `DuckSession` class manages connections with spatial extension pre-loaded. `query_parquet()` and `spatial_join()` are convenience functions.

**pmtiles.py** — Requires external CLI tools (`tippecanoe` and `pmtiles`). Pipeline: GeoParquet → GeoJSON (temp) → MBTiles → PMTiles. Temp files cleaned up via `tempfile.TemporaryDirectory()`.

**postgis_sync.py** — SQLAlchemy-based, prefers psycopg v3. Creates GIST spatial indices automatically. Supports "replace" or "append" modes.

**__init__.py** — Re-exports all public functions for programmatic use.

## Key Patterns

- **File discovery**: Functions accept both file paths and directory paths; they walk up the directory tree to find HMS project files (.basin, .map, .results).
- **CRS handling**: Auto-detected from HMS project via `init_hms_project()`. If detection fails, no reprojection occurs (rather than erroring).
- **Optional dependencies**: duckdb, postgis, and pmtiles extras are guarded by import checks. Core functionality (geometry + results export) works with base dependencies only.
- **Variable normalization**: User-facing names like "Flow Out" or "Stage" are mapped to HMS XML element prefixes in `results.py`.

## Examples

`examples/` contains marimo reactive notebooks (`.py` format). Run interactively with `marimo edit` or as scripts with `uv run python`.

- `00_using_hms_examples.py` — HmsExamples API intro (list versions, extract projects)
- `01_export_geometry.py` — Export all geometry layers from Tifton example
- `02_export_results.py` — Export results + spatial join
- `03_duckdb_query.py` — SQL analytics on GeoParquet with DuckDB
- `04_generate_pmtiles.py` — Generate PMTiles (requires tippecanoe + pmtiles on PATH)
- `05_multi_run_comparison.py` — Compare results across tifton and castro projects

## Testing

Tests create minimal `.basin` and `.results` XML data inline (no fixture files needed). Tests exercise full export pipelines and validate geometry types/values. Integration tests (marked `@pytest.mark.integration`) use `HmsExamples.extract_project("river_bend")` for real HMS data. Run with `uv run python -m pytest tests/test_project.py tests/test_geometry.py tests/test_results_xml.py -v`.
