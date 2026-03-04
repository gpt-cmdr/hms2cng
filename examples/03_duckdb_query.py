import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    03 — DuckDB Queries
    ===================
    Demonstrates SQL analytics on exported GeoParquet files using DuckDB.
    Depends on outputs from notebook 01 (geometry) and 02 (results).
    Run those notebooks first, or this notebook will generate the parquet files.
    """
    import marimo as mo
    mo.md("## 03 — DuckDB Queries")


@app.cell
def __():
    from pathlib import Path
    from hms_commander import HmsExamples
    from hms2cng.geometry import export_basin_geometry
    from hms2cng.results import export_hms_results

    # Ensure output files exist (run inline if notebooks 01/02 haven't been run)
    geom_dir = Path("out/01_export_geometry")
    results_dir_out = Path("out/02_export_results")
    geom_dir.mkdir(parents=True, exist_ok=True)
    results_dir_out.mkdir(parents=True, exist_ok=True)

    subbasins_parquet = geom_dir / "subbasins.parquet"
    results_parquet = results_dir_out / "subbasin_outflow.parquet"

    if not subbasins_parquet.exists() or not results_parquet.exists():
        print("Generating parquet files from Tifton example project...")
        project_path = HmsExamples.extract_project("tifton")
        basin = next(project_path.glob("*.basin"))
        results_src = project_path / "results"
        if not results_src.is_dir():
            results_src = project_path

        if not subbasins_parquet.exists():
            export_basin_geometry(basin, subbasins_parquet, layer="subbasins")
        if not results_parquet.exists():
            export_hms_results(results_src, results_parquet, element_type="subbasin", variable="Outflow")

    print(f"Subbasins:  {subbasins_parquet}")
    print(f"Results:    {results_parquet}")
    return Path, results_parquet, subbasins_parquet


@app.cell
def __(results_parquet):
    from hms2cng.duckdb_session import query_parquet

    # Basic query: top subbasins by peak outflow
    df_top = query_parquet(
        results_parquet,
        "SELECT name, max_value, time_of_max FROM _ ORDER BY max_value DESC",
    )
    print("Top subbasins by peak outflow:")
    print(df_top.to_string(index=False))
    return df_top, query_parquet


@app.cell
def __(query_parquet, results_parquet):
    # Aggregation: summary statistics across the watershed
    df_stats = query_parquet(
        results_parquet,
        """
        SELECT
            COUNT(*) AS n_subbasins,
            ROUND(MIN(max_value), 2) AS min_peak_cfs,
            ROUND(AVG(max_value), 2) AS avg_peak_cfs,
            ROUND(MAX(max_value), 2) AS max_peak_cfs
        FROM _
        """,
    )
    print("Watershed-wide outflow statistics:")
    print(df_stats.to_string(index=False))
    return df_stats,


@app.cell
def __(results_parquet, subbasins_parquet):
    from hms2cng.duckdb_session import DuckSession

    # Multi-table query: join results with geometry attributes
    s = DuckSession()
    try:
        s.register_parquet(results_parquet, "results")
        s.register_parquet(subbasins_parquet, "geom")

        df_joined = s.query(
            """
            SELECT
                r.name,
                g.area,
                r.max_value AS peak_cfs,
                ROUND(r.max_value / NULLIF(g.area, 0), 2) AS cfs_per_sqmi
            FROM results r
            JOIN geom g ON r.name = g.name
            ORDER BY cfs_per_sqmi DESC
            """
        )
        print("Peak outflow normalized by area (cfs/sq mi):")
        print(df_joined.to_string(index=False))
    finally:
        s.close()
    return DuckSession,


if __name__ == "__main__":
    app.run()
