"""
hms2cng: HMS to Cloud Native GIS

An open-source project of CLB Engineering Corporation (https://clbengineering.com/)
GitHub: https://github.com/gpt-cmdr/hms2cng
Docs: https://hms2cng.readthedocs.io
Contact: info@clbengineering.com

CLI tool for exporting HEC-HMS project data to GeoParquet, PMTiles, and PostGIS.
Built on hms-commander (https://github.com/gpt-cmdr/hms-commander).
"""

__author__ = "CLB Engineering Corporation"

from hms2cng.geometry import export_basin_geometry, extract_watershed_boundary, export_all_basin_geometry, merge_all_layers, get_basin_layer_gdf
from hms2cng.results import export_hms_results, export_peak_flow_summary, export_all_results, merge_all_variables
from hms2cng.project import get_project_manifest, export_project_manifest, export_full_project
from hms2cng.catalog import Manifest, ManifestLayer
try:
    from hms2cng.duckdb_session import DuckSession, query_parquet, spatial_join
except ImportError:
    pass
from hms2cng.pmtiles import generate_pmtiles_from_input, generate_watershed_overview
try:
    from hms2cng.postgis_sync import sync_to_postgres, read_from_postgres, sync_watershed_to_postgis
except ImportError:
    pass

__version__ = "0.1.2"
__all__ = [
    # Single-layer exports (original)
    "export_basin_geometry",
    "extract_watershed_boundary",
    "get_basin_layer_gdf",
    "export_hms_results",
    "export_peak_flow_summary",
    # Full-project exports (new)
    "export_all_basin_geometry",
    "export_all_results",
    "merge_all_layers",
    "merge_all_variables",
    "get_project_manifest",
    "export_project_manifest",
    "export_full_project",
    # Catalog
    "Manifest",
    "ManifestLayer",
    # DuckDB
    "DuckSession",
    "query_parquet",
    "spatial_join",
    # PMTiles
    "generate_pmtiles_from_input",
    "generate_watershed_overview",
    # PostGIS
    "sync_to_postgres",
    "read_from_postgres",
    "sync_watershed_to_postgis",
]
