"""
PostGIS sync functions for hms2cng
"""
from pathlib import Path
from sqlalchemy import create_engine, text
import geopandas as gpd
from typing import Optional


def sync_to_postgres(
    input_file: Path,
    postgres_uri: str,
    table_name: str,
    schema: str = "public",
    if_exists: str = "replace"
):
    """
    Sync GeoParquet file to PostGIS table.
    
    Args:
        input_file: Path to GeoParquet file
        postgres_uri: PostgreSQL connection URI (postgresql://user:pass@host:port/db)
        table_name: Target table name
        schema: Target schema
        if_exists: 'replace', 'append', or 'fail'
    """
    # Read GeoParquet
    gdf = gpd.read_parquet(input_file)
    
    # Create SQLAlchemy engine
    # Prefer psycopg (v3) driver if available.
    uri = postgres_uri
    if uri.startswith("postgresql://"):
        # SQLAlchemy defaults to psycopg2 for postgresql://. If users installed psycopg (v3),
        # rewrite to the explicit dialect.
        uri_psycopg = "postgresql+psycopg://" + uri[len("postgresql://"):]
    else:
        uri_psycopg = uri

    try:
        engine = create_engine(uri)
    except ModuleNotFoundError as e:
        # Common on fresh Windows installs: psycopg2 not installed.
        if "psycopg2" in str(e):
            engine = create_engine(uri_psycopg, connect_args={"client_encoding": "UTF8"})
        else:
            raise
    
    # Ensure geometry column is properly typed
    if 'geometry' in gdf.columns:
        gdf = gdf.set_geometry('geometry')
    
    # Write to PostGIS
    gdf.to_postgis(
        table_name,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=False
    )
    
    # Create spatial index if geometry exists
    if 'geometry' in gdf.columns:
        with engine.connect() as conn:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS {table_name}_geom_idx 
                ON {schema}.{table_name} 
                USING GIST (geometry)
            """))
            conn.commit()


def read_from_postgres(
    postgres_uri: str,
    table_name: str,
    schema: str = "public",
    geometry_column: str = "geometry"
) -> gpd.GeoDataFrame:
    """
    Read data from PostGIS to GeoDataFrame.
    
    Args:
        postgres_uri: PostgreSQL connection URI
        table_name: Source table name
        schema: Source schema
        geometry_column: Geometry column name
    
    Returns:
        GeoDataFrame with PostGIS data
    """
    import pandas as pd
    from shapely import wkb
    
    engine = create_engine(postgres_uri)
    
    # Read with geometry
    query = f"""
    SELECT *, ST_AsBinary({geometry_column}) as geom_wkb 
    FROM {schema}.{table_name}
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
    
    # Convert to GeoDataFrame
    df = pd.DataFrame(rows, columns=result.keys())
    if 'geom_wkb' in df.columns:
        df['geometry'] = df['geom_wkb'].apply(lambda x: wkb.loads(bytes(x)) if x else None)
        df = df.drop(columns=['geom_wkb'])
        df = gpd.GeoDataFrame(df, geometry='geometry')
    
    return df


def sync_watershed_to_postgis(
    basin_file: Path,
    results_dir: Path,
    postgres_uri: str,
    schema: str = "uberclaw",
    run_id: Optional[str] = None
):
    """
    Complete watershed sync: geometry + results to PostGIS.
    
    Creates/updates:
    - {schema}.hms_subbasins (geometry)
    - {schema}.hms_reaches (geometry)
    - {schema}.hms_subbasin_results (results with run_id)
    - {schema}.hms_reach_results (results with run_id)
    """
    import tempfile
    from hms2cng.geometry import export_basin_geometry
    from hms2cng.results import export_hms_results
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Export geometry
        export_basin_geometry(basin_file, tmpdir / "subbasins.parquet", layer="subbasins")
        export_basin_geometry(basin_file, tmpdir / "reaches.parquet", layer="reaches")
        
        # Sync geometry
        sync_to_postgres(tmpdir / "subbasins.parquet", postgres_uri, "hms_subbasins", schema, "replace")
        sync_to_postgres(tmpdir / "reaches.parquet", postgres_uri, "hms_reaches", schema, "replace")
        
        # Export and sync results
        export_hms_results(results_dir, tmpdir / "subbasin_results.parquet", element_type="subbasin")
        export_hms_results(results_dir, tmpdir / "reach_results.parquet", element_type="reach")
        
        # Add run_id column
        import pandas as pd
        if run_id:
            for results_file in [tmpdir / "subbasin_results.parquet", tmpdir / "reach_results.parquet"]:
                if results_file.exists():
                    df = pd.read_parquet(results_file)
                    df['run_id'] = run_id
                    df.to_parquet(results_file)
        
        sync_to_postgres(tmpdir / "subbasin_results.parquet", postgres_uri, "hms_subbasin_results", schema, "append")
        sync_to_postgres(tmpdir / "reach_results.parquet", postgres_uri, "hms_reach_results", schema, "append")
