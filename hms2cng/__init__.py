"""
hms2cng: HMS to Cloud Native GIS — CLI for HEC-HMS to GeoParquet/PMTiles export
"""
from hms2cng.geometry import export_basin_geometry, extract_watershed_boundary
from hms2cng.results import export_hms_results, export_peak_flow_summary
from hms2cng.duckdb_session import DuckSession, query_parquet, spatial_join
from hms2cng.pmtiles import generate_pmtiles_from_input, generate_watershed_overview
from hms2cng.postgis_sync import sync_to_postgres, read_from_postgres, sync_watershed_to_postgis

__version__ = "0.1.0"
__all__ = [
    "export_basin_geometry",
    "extract_watershed_boundary",
    "export_hms_results",
    "export_peak_flow_summary",
    "DuckSession",
    "query_parquet",
    "spatial_join",
    "generate_pmtiles_from_input",
    "generate_watershed_overview",
    "sync_to_postgres",
    "read_from_postgres",
    "sync_watershed_to_postgis",
]
