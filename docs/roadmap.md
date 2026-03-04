# Roadmap

Future Cloud Native GIS capabilities planned for hms2cng:

| Feature | Description | Status |
|---------|-------------|--------|
| **COG output** | Export HMS grid results (precipitation, soil moisture, ET) as Cloud Optimized GeoTIFF | Planned |
| **COG input** | Read COG rasters as spatial input for spatial joins and visualization | Planned |
| **STAC catalog** | Publish exported GeoParquet and PMTiles as STAC items for discovery | Planned |
| **GeoArrow** | Zero-copy geometry passing between DuckDB and GeoPandas via Arrow tables | Planned |
| **DuckLake** | Lakehouse backend for HMS output archives (time travel, schema evolution on S3/R2) | Planned (awaiting DuckLake v1) |

## Philosophy

The hms2cng roadmap follows the broader Cloud Native Geospatial movement:

- **Zero-copy**: GeoArrow enables geometry to flow between DuckDB, GeoPandas, and web without serialization
- **Serverless delivery**: PMTiles and COGs deliver spatial data via HTTP range requests — no tile server, no WMS
- **Columnar analytics**: GeoParquet + DuckDB replace row-oriented formats and heavy spatial databases for analytical workloads
- **Open catalog**: STAC makes HMS model outputs discoverable across tools and organizations
- **Lakehouse persistence**: DuckLake extends DuckDB with versioned, schema-evolving storage on object stores (S3, Cloudflare R2)

## Current Stack

The current release covers the foundation:

```
GeoParquet → DuckDB → PMTiles → PostGIS
```

Each planned feature extends this stack without breaking existing workflows.
