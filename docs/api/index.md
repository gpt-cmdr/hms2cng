# API Reference

hms2cng exposes all public functions for programmatic use. Import from the package directly or from individual modules.

```python
# Package-level imports
from hms2cng import (
    export_basin_geometry, export_hms_results,
    export_all_basin_geometry, export_all_results,
    get_project_manifest, export_project_manifest, export_full_project,
    DuckSession,
)

# Module-level imports
from hms2cng.project import get_project_manifest, export_project_manifest, export_full_project
from hms2cng.geometry import export_basin_geometry, get_basin_layer_gdf, export_all_basin_geometry
from hms2cng.results import export_hms_results, export_all_results
from hms2cng.duckdb_session import DuckSession, query_parquet, spatial_join
from hms2cng.pmtiles import generate_pmtiles_from_input
from hms2cng.postgis_sync import sync_to_postgres
```

## hms2cng.project

::: hms2cng.project.get_project_manifest

::: hms2cng.project.export_project_manifest

::: hms2cng.project.export_full_project

::: hms2cng.project.slugify

## hms2cng.geometry

::: hms2cng.geometry.export_basin_geometry

::: hms2cng.geometry.get_basin_layer_gdf

::: hms2cng.geometry.export_all_basin_geometry

## hms2cng.results

::: hms2cng.results.export_hms_results

::: hms2cng.results.export_all_results

## hms2cng.duckdb_session

::: hms2cng.duckdb_session.DuckSession

::: hms2cng.duckdb_session.query_parquet

::: hms2cng.duckdb_session.spatial_join

## hms2cng.pmtiles

::: hms2cng.pmtiles.generate_pmtiles_from_input

## hms2cng.postgis_sync

::: hms2cng.postgis_sync.sync_to_postgres
