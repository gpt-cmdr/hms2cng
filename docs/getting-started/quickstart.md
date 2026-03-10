# Quick Start

## Full Project Export

Export an entire HMS project to a single consolidated GeoParquet:

```bash
# Show what's in the project (runs, basin models, met models)
hms2cng manifest MyProject.hms

# Export everything: all basin geometries + all run results in one file
hms2cng project MyProject.hms out/
```

This creates a **single consolidated GeoParquet** with a `layer` discriminator column:
```
out/
  my_project.parquet     # ALL geometry + results (layer column discriminates)
  manifest.json          # JSON catalog (schema v2.0)
```

Query specific layers with DuckDB:

```bash
# All subbasins
hms2cng query "out/my_project.parquet" \
  "SELECT * FROM _ WHERE layer = 'subbasins'"

# Peak outflow across all runs
hms2cng query "out/my_project.parquet" \
  "SELECT run_name, name, max_value FROM _ WHERE layer = 'outflow' ORDER BY max_value DESC LIMIT 20"

# Layer inventory
hms2cng query "out/my_project.parquet" \
  "SELECT layer, COUNT(*) as n FROM _ GROUP BY layer ORDER BY n DESC"
```

!!! tip "Separate registry parquets"
    Use `hms2cng manifest MyProject.hms -o out/` to also export `manifest.parquet`, `run_registry.parquet`, and `basin_inventory.parquet` for cross-project DuckDB analytics.

---

## Single-Layer Pipeline

### Step 1 — Export geometry

```bash
hms2cng geometry project.basin subbasins.parquet --layer subbasins
```

Available layers: `subbasins`, `junctions`, `reaches`, `diversions`, `reservoirs`, `sources`, `sinks`, `watershed`, `subbasin_polygons`, `longest_flowpaths`, `centroidal_flowpaths`, `teneightyfive_flowpaths`, `subbasin_statistics`

### Step 2 — Export results

```bash
hms2cng results project/results results.parquet --type subbasin --var Outflow
```

Parses `RUN_*.results` XML files (NOT DSS). Merges summary statistics (max, min, mean, time of peak) with basin geometry.

### Step 3 — Query with DuckDB

```bash
hms2cng query results.parquet "SELECT name, max_value FROM _ ORDER BY max_value DESC"
```

Use `_` as the table alias. Results print to console or save to file with `--output result.csv`.

## All CLI Commands

```bash
hms2cng --help

# Full project export (consolidated single parquet + manifest.json)
hms2cng project PROJECT.hms OUTPUT_DIR/ [--layers L1,L2] [--vars V1,V2] [--out-crs EPSG:4326] [--no-sort]

# Show project manifest (runs, basin models, met models)
hms2cng manifest PROJECT.hms [-o OUTPUT_DIR]

# Export geometry layer
hms2cng geometry BASIN_FILE OUTPUT.parquet [--layer LAYER] [--crs EPSG:XXXX] [--out-crs EPSG:4326]

# Export results
hms2cng results RESULTS_DIR OUTPUT.parquet [--type TYPE] [--var VAR]

# Query parquet
hms2cng query INPUT.parquet "SQL" [--output result.csv]

# Generate PMTiles (requires tippecanoe + pmtiles on PATH)
hms2cng pmtiles INPUT.parquet OUTPUT.pmtiles [--layer NAME] [--min-zoom Z] [--max-zoom Z]

# Sync to PostGIS
hms2cng sync INPUT.parquet postgresql://user:pass@host/db TABLE_NAME [--schema public]
```

## Python API

```python
from hms2cng import export_full_project, export_basin_geometry, export_hms_results, DuckSession
from hms2cng.duckdb_session import query_parquet
from hms2cng.geometry import get_basin_layer_gdf

# Full project export (single consolidated parquet + manifest.json)
summary = export_full_project("MyProject.hms", "out/")
print(summary)  # {'parquet_file': ..., 'manifest_json': ..., 'geometry_rows': ..., ...}

# Get GeoDataFrame directly
gdf = get_basin_layer_gdf("project.basin", layer="subbasins")

# Export individual layers
export_basin_geometry("project.basin", "subbasins.parquet", layer="subbasins")
export_hms_results("project/results", "results.parquet", element_type="subbasin", variable="Outflow")

# Query with DuckDB
df = query_parquet("out/my_project.parquet", "SELECT * FROM _ WHERE layer = 'outflow' ORDER BY max_value DESC")
print(df.head(10))
```

## CRS Handling

hms2cng auto-detects the coordinate reference system from the HMS project file. Override with:

```bash
hms2cng geometry project.basin out.parquet --crs EPSG:2278 --out-crs EPSG:4326
```

If CRS cannot be detected, no reprojection is performed (no error thrown).
