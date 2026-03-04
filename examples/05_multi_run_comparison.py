import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    05 — Multi-Run Comparison
    =========================
    Compares peak outflow results across two HMS example projects (tifton and castro)
    using DuckDB joins. Demonstrates the analytical power of GeoParquet + DuckDB for
    cross-project comparison without any database server.
    Output goes to out/05_multi_run_comparison/
    """
    import marimo as mo
    mo.md("## 05 — Multi-Run Comparison")


@app.cell
def __():
    from pathlib import Path
    from hms_commander import HmsExamples
    from hms2cng.results import export_hms_results

    out_dir = Path("out/05_multi_run_comparison")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Extract tifton project
    tifton_path = HmsExamples.extract_project("tifton")
    tifton_results_dir = tifton_path / "results"
    if not tifton_results_dir.is_dir():
        tifton_results_dir = tifton_path
    print(f"Tifton: {tifton_path.name}")

    # Extract castro project
    castro_path = HmsExamples.extract_project("castro")
    castro_results_dir = castro_path / "results"
    if not castro_results_dir.is_dir():
        castro_results_dir = castro_path
    print(f"Castro: {castro_path.name}")
    return Path, castro_path, castro_results_dir, out_dir, tifton_path, tifton_results_dir


@app.cell
def __(castro_results_dir, out_dir, tifton_results_dir):
    from hms2cng.results import export_hms_results

    tifton_parquet = out_dir / "tifton_subbasin_outflow.parquet"
    castro_parquet = out_dir / "castro_subbasin_outflow.parquet"

    # Export tifton results
    try:
        export_hms_results(tifton_results_dir, tifton_parquet, element_type="subbasin", variable="Outflow")
        print(f"Tifton results -> {tifton_parquet.name}")
    except ValueError as e:
        print(f"Tifton export skipped: {e}")

    # Export castro results
    try:
        export_hms_results(castro_results_dir, castro_parquet, element_type="subbasin", variable="Outflow")
        print(f"Castro results -> {castro_parquet.name}")
    except ValueError as e:
        print(f"Castro export skipped: {e}")
    return castro_parquet, tifton_parquet


@app.cell
def __(castro_parquet, tifton_parquet):
    # Inspect each project independently
    import geopandas as gpd

    print("=== Tifton Subbasin Peak Outflow ===")
    if tifton_parquet.exists():
        gdf_t = gpd.read_parquet(tifton_parquet)
        cols_t = [c for c in ["name", "max_value", "units"] if c in gdf_t.columns]
        print(gdf_t[cols_t].sort_values("max_value", ascending=False).to_string(index=False))
    else:
        print("  (no results)")

    print()
    print("=== Castro Subbasin Peak Outflow ===")
    if castro_parquet.exists():
        gdf_c = gpd.read_parquet(castro_parquet)
        cols_c = [c for c in ["name", "max_value", "units"] if c in gdf_c.columns]
        print(gdf_c[cols_c].sort_values("max_value", ascending=False).to_string(index=False))
    else:
        print("  (no results)")
    return gdf_c, gdf_t, gpd


@app.cell
def __(castro_parquet, tifton_parquet):
    # Cross-project comparison with DuckDB
    # Both projects have different subbasin names, so we compare aggregate stats
    from hms2cng.duckdb_session import DuckSession

    if tifton_parquet.exists() and castro_parquet.exists():
        s = DuckSession()
        try:
            s.register_parquet(tifton_parquet, "tifton")
            s.register_parquet(castro_parquet, "castro")

            df_compare = s.query(
                """
                SELECT 'tifton' AS project,
                    COUNT(*) AS n_subbasins,
                    ROUND(AVG(max_value), 1) AS avg_peak_cfs,
                    ROUND(MAX(max_value), 1) AS max_peak_cfs
                FROM tifton
                UNION ALL
                SELECT 'castro' AS project,
                    COUNT(*) AS n_subbasins,
                    ROUND(AVG(max_value), 1) AS avg_peak_cfs,
                    ROUND(MAX(max_value), 1) AS max_peak_cfs
                FROM castro
                """
            )
            print("Cross-project summary comparison:")
            print(df_compare.to_string(index=False))
        finally:
            s.close()
    else:
        print("Skipped — one or both result files not available.")
    return DuckSession,


if __name__ == "__main__":
    app.run()
