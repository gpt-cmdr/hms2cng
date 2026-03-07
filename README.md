# hms2cng — HMS to Cloud Native GIS

<p align="center">
  <img src="docs/assets/hms2cng_logo.svg" alt="hms2cng logo" width="450">
</p>

<p align="center">
  <a href="https://pypi.org/project/hms2cng/"><img src="https://img.shields.io/pypi/v/hms2cng.svg" alt="PyPI version"></a>
  <a href="https://hms2cng.readthedocs.io/"><img src="https://readthedocs.org/projects/hms2cng/badge/?version=latest" alt="ReadTheDocs"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-teal.svg" alt="MIT License"></a>
</p>

**Full project access and archival for HEC-HMS.** Exports geometry, simulation results, and project metadata to cloud-native GeoParquet — enabling cross-project DuckDB analytics, PMTiles web visualization, and PostGIS integration.

**Documentation:** [hms2cng.readthedocs.io](https://hms2cng.readthedocs.io/)

Built on top of [`hms-commander`](https://github.com/gpt-cmdr/hms-commander) by [CLB Engineering Corporation](https://clbengineering.com/).

---

## Installation

```bash
# Base installation (GeoParquet export only)
pip install hms2cng

# All features (DuckDB, PostGIS, PMTiles)
pip install "hms2cng[all]"
```

### PMTiles Generation

PMTiles generation requires external CLI tools:
- **tippecanoe** — `conda install -c conda-forge tippecanoe`
- **pmtiles** — `go install github.com/protomaps/go-pmtiles/pmtiles@latest`

---

## Quick Start

### Full Project Export

Export an entire HMS project — all basin models, all runs, all layers:

```bash
# Preview the project structure (runs, basin models, met models)
hms2cng manifest MyProject.hms

# Export everything to a hierarchical GeoParquet archive
hms2cng project MyProject.hms out/my_archive/
```

Output structure:
```
out/my_archive/
  manifest.parquet          # project-level metadata (1 row)
  run_registry.parquet      # basin + met + control lineage per run
  basin_inventory.parquet   # element counts per basin model
  geometry/
    {basin_slug}/
      subbasins.parquet
      reaches.parquet
      junctions.parquet
      ...
  results/
    {run_slug}/
      outflow.parquet       # peak, min, mean, time of peak
```

Query across all runs with DuckDB:

```python
import duckdb
df = duckdb.sql("""
    SELECT project_name, run_name, name, max_value, units
    FROM read_parquet('out/my_archive/results/*/*.parquet', union_by_name=true)
    ORDER BY max_value DESC
    LIMIT 20
""").df()
```

### Single-Layer Export

```bash
# Export a specific geometry layer
hms2cng geometry model.basin subbasins.parquet --layer subbasins

# Export simulation results
hms2cng results project/results results.parquet --type subbasin --var Outflow

# Query with DuckDB
hms2cng query results.parquet "SELECT name, max_value FROM _ ORDER BY max_value DESC"

# Generate PMTiles (requires tippecanoe + pmtiles)
hms2cng pmtiles subbasins.parquet subbasins.pmtiles --layer subbasins

# Sync to PostGIS
hms2cng sync subbasins.parquet "postgresql://user:pass@host:5432/db" hms_subbasins
```

### Python API

```python
from hms2cng import (
    export_full_project,          # export entire HMS project
    get_project_manifest,         # read project structure as dict
    export_all_basin_geometry,    # batch geometry export
    export_all_results,           # batch results export
    export_basin_geometry,        # single layer export
    export_hms_results,           # single run export
    DuckSession,
)

# Full project archive
summary = export_full_project("MyProject.hms", "out/archive/")
print(f"Geometry files: {len(summary['geometry_files'])}")
print(f"Results files:  {len(summary['results_files'])}")

# Project manifest
manifest = get_project_manifest("MyProject.hms")
print(manifest["run_names"])  # JSON list of run names

# Single layer (original API unchanged)
from hms2cng.geometry import get_basin_layer_gdf
gdf = get_basin_layer_gdf("project.basin", layer="subbasins")
```

---

## Cloud Native GIS Stack

| Legacy | Cloud Native | Benefit |
|--------|-------------|---------|
| Shapefile | **GeoParquet** | Columnar, Arrow-native, compressed |
| WMS/WFS | **PMTiles** | Serverless HTTP range requests |
| PostGIS queries | **DuckDB** | In-process spatial SQL, no server |
| SDE layers | **PostGIS** | Open standard, cloud-ready |

---

## Development

```bash
git clone https://github.com/gpt-cmdr/hms2cng
cd hms2cng
uv pip install -e ".[all]"

# Run tests
uv run pytest tests/

# Interactive examples (marimo)
uv run marimo edit examples/06_full_project_export.py

# Preview docs
uv run mkdocs serve
```

---

## About CLB Engineering

**hms2cng** is an open-source project of [CLB Engineering Corporation](https://clbengineering.com/), the creators of [hms-commander](https://github.com/gpt-cmdr/hms-commander) and [ras-commander](https://github.com/gpt-cmdr/ras-commander).

CLB pioneered the **[LLM Forward](https://clbengineering.com/llm-forward)** approach to civil engineering — a framework where licensed professional engineers leverage Large Language Models to accelerate H&H modeling workflows while maintaining full professional responsibility.

**Contact**: [info@clbengineering.com](mailto:info@clbengineering.com) | **Website**: [clbengineering.com](https://clbengineering.com/)

---

## License

MIT — See [LICENSE](LICENSE)

**Author:** William M. Katzenmeyer, P.E., C.F.M. — CLB Engineering Corporation
