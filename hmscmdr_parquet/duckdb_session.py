"""
DuckDB session wrapper for querying GeoParquet files
"""
import duckdb
from pathlib import Path
import pandas as pd
from typing import Optional


class DuckSession:
    """DuckDB session with spatial extension pre-loaded"""
    
    def __init__(self, db_path: str = ":memory:"):
        self.con = duckdb.connect(db_path)
        self.con.execute("INSTALL spatial; LOAD spatial;")
    
    def register_parquet(self, path: Path, name: str = "_"):
        """Register a Parquet file as a table view"""
        self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}');")
        return self
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        return self.con.execute(sql).df()
    
    def close(self):
        self.con.close()


def query_parquet(input_file: Path, sql: str) -> pd.DataFrame:
    """
    Quick helper to query a single GeoParquet file.
    
    Args:
        input_file: Path to GeoParquet file
        sql: SQL query (use '_' as table name)
    
    Returns:
        pandas DataFrame with query results
    
    Example:
        df = query_parquet("subbasins.parquet", "SELECT * FROM _ WHERE max_value > 1000")
    """
    session = DuckSession()
    session.register_parquet(input_file)
    try:
        return session.query(sql)
    finally:
        session.close()


def spatial_join(
    left_file: Path,
    right_file: Path,
    predicate: str = "ST_Intersects",
    output_file: Optional[Path] = None
) -> pd.DataFrame:
    """
    Perform spatial join between two GeoParquet files.
    
    Args:
        left_file: Left GeoParquet file
        right_file: Right GeoParquet file
        predicate: Spatial predicate (ST_Intersects, ST_Contains, ST_Within, etc.)
        output_file: Optional output file for results
    
    Returns:
        DataFrame with joined results
    """
    session = DuckSession()
    session.register_parquet(left_file, "left")
    session.register_parquet(right_file, "right")
    
    sql = f"""
    SELECT l.*, r.*
    FROM left AS l
    JOIN right AS r
    ON {predicate}(l.geometry, r.geometry)
    """
    
    df = session.query(sql)
    session.close()
    
    if output_file:
        df.to_parquet(output_file, index=False)
    
    return df
