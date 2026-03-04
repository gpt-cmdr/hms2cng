# Installation

## Core Install

```bash
pip install hms2cng
```

This installs the geometry and results export pipeline. No DuckDB, PostGIS, or PMTiles extras needed for basic use.

## Full Install (All Extras)

```bash
pip install "hms2cng[all]"
```

Includes DuckDB, PostGIS (SQLAlchemy + psycopg v3), and rasterio.

Using `uv` (recommended, 10-100x faster):

```bash
uv pip install "hms2cng[all]"
```

## Install Specific Extras

```bash
pip install "hms2cng[duckdb]"    # DuckDB SQL analytics
pip install "hms2cng[postgis]"   # PostGIS sync
pip install "hms2cng[pmtiles]"   # rasterio (PMTiles pipeline also needs external CLIs)
```

## Dev Install from Source

```bash
git clone https://github.com/gpt-cmdr/hms2cng.git
cd hms2cng
uv pip install -e ".[all]"
```

## PMTiles External CLI Tools

The `hms2cng pmtiles` command requires two external CLI tools (not available via pip):

- **tippecanoe** — Generates MBTiles from GeoJSON
- **pmtiles** — Converts MBTiles to PMTiles

Install via conda-forge:

```bash
conda install -c conda-forge tippecanoe pmtiles
```

Or download binaries from [protomaps releases](https://github.com/protomaps/go-pmtiles/releases).

## Requirements

- Python >= 3.10
- [hms-commander](https://github.com/gpt-cmdr/hms-commander) — installed automatically as a dependency
- Core: pandas, geopandas, pyarrow, typer, rich
