<p align="center">
  <img src="assets/hms2cng_logo.svg" alt="hms2cng logo" width="420"/>
</p>

# hms2cng — HMS to Cloud Native GIS

**hms2cng** exports HEC-HMS hydrologic model geometry and simulation results to cloud-native geospatial formats, eliminating the traditional geospatial tax through zero-copy Arrow memory structures, serverless tile delivery, and columnar analytics.

## What is "Cloud Native GIS"?

The cloud-native geospatial stack replaces legacy GIS workflows with open formats designed for the web and analytical engines:

| Legacy | Cloud Native | Benefit |
|--------|-------------|---------|
| Shapefile | **GeoParquet** | Columnar, Arrow-native, compressed |
| WMS/WFS tiles | **PMTiles** | Serverless HTTP range requests, no tile server |
| PostGIS queries | **DuckDB** | In-process spatial SQL, no server needed |
| SDE layers | **PostGIS** | Open standard, cloud-ready |

## hms2cng Stack

```
HMS model files (.basin, .results XML)
  → geometry.py / results.py    (parse via hms-commander → GeoDataFrame)
    → GeoParquet                (intermediate, columnar, Arrow-native)
      → DuckDB                 (serverless SQL analytics)
      → PMTiles                (serverless vector tiles via HTTP range)
      → PostGIS                (enterprise spatial database)
```

## Quick Start

```bash
pip install hms2cng

# Export the entire project (all basin models + all runs)
hms2cng project MyProject.hms out/

# Or export individual layers
hms2cng geometry project.basin subbasins.parquet --layer subbasins
hms2cng results project/results results.parquet --type subbasin --var Outflow

# Query with DuckDB
hms2cng query results.parquet "SELECT name, max_value FROM _ ORDER BY max_value DESC"
```

```python
from hms2cng.geometry import export_basin_geometry, get_basin_layer_gdf
from hms2cng.results import export_hms_results

# Get GeoDataFrame directly (in-memory)
gdf = get_basin_layer_gdf("project.basin", layer="subbasins")

# Export to file
export_basin_geometry("project.basin", "subbasins.parquet", layer="subbasins")
export_hms_results("project/results", "results.parquet", element_type="subbasin", variable="Outflow")
```

## Key Features

- **Full project archival** — `hms2cng project` exports all basin models and all runs into a single consolidated GeoParquet with a `layer` discriminator column
- **GeoParquet export** — All HMS basin layers (subbasins, reaches, junctions, watershed boundary, SQLite grid layers)
- **Results export** — Summary statistics (peak, min, mean, time of peak) spatially joined with geometry
- **Run registry** — `hms2cng manifest` exports `manifest.parquet`, `run_registry.parquet`, `basin_inventory.parquet` for cross-project analytics
- **DuckDB queries** — SQL analytics directly on GeoParquet files, no database server needed
- **PMTiles generation** — Vector tile pipeline via tippecanoe, serverless HTTP delivery
- **PostGIS sync** — Upload to enterprise spatial databases with automatic GIST indices
- **Auto CRS detection** — Reads coordinate system from HMS project files

## Installation

```bash
# Core (geometry + results export)
pip install hms2cng

# All optional dependencies (DuckDB, PostGIS, PMTiles)
pip install "hms2cng[all]"
```

See [Installation](getting-started/installation.md) for full details.
