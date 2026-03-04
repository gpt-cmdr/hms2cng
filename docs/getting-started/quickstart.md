# Quick Start

## 3-Step Pipeline

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
from hms2cng.geometry import export_basin_geometry, get_basin_layer_gdf
from hms2cng.results import export_hms_results
from hms2cng.duckdb_session import query_parquet, DuckSession
from hms2cng.pmtiles import generate_pmtiles_from_input
from hms2cng.postgis_sync import sync_to_postgres

# Get GeoDataFrame directly
gdf = get_basin_layer_gdf("project.basin", layer="subbasins")

# Export all geometry layers
for layer in ["subbasins", "junctions", "reaches", "watershed"]:
    try:
        export_basin_geometry("project.basin", f"out/{layer}.parquet", layer=layer)
    except (FileNotFoundError, ValueError) as e:
        print(f"Skipping {layer}: {e}")

# Export results
export_hms_results("project/results", "results.parquet", element_type="subbasin", variable="Outflow")

# Query with DuckDB
df = query_parquet("results.parquet", "SELECT name, max_value FROM _ ORDER BY max_value DESC")
print(df.head(10))
```

## CRS Handling

hms2cng auto-detects the coordinate reference system from the HMS project file. Override with:

```bash
hms2cng geometry project.basin out.parquet --crs EPSG:2278 --out-crs EPSG:4326
```

If CRS cannot be detected, no reprojection is performed (no error thrown).
