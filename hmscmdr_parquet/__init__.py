"""
hmscmdr-parquet: CLI for HEC-HMS to GeoParquet/PMTiles export
"""
from hmscmdr_parquet.geometry import export_basin_geometry, extract_watershed_boundary
from hmscmdr_parquet.results import export_hms_results, export_peak_flow_summary
from hmscmdr_parquet.duckdb_session import DuckSession, query_parquet, spatial_join
from hmscmdr_parquet.pmtiles import generate_pmtiles_from_input, generate_watershed_overview
from hmscmdr_parquet.postgis_sync import sync_to_postgres, read_from_postgres, sync_watershed_to_postgis

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
