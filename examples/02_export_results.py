import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    02 — Export Results
    ===================
    Exports HMS simulation results (outflow summary statistics) from the Tifton
    example project. Results are spatially joined with basin geometry.
    Reads from RUN_*.results XML files — NOT DSS files.
    Output goes to out/02_export_results/
    """
    import marimo as mo
    mo.md("## 02 — Export Results")


@app.cell
def __():
    from pathlib import Path
    from hms_commander import HmsExamples

    project_path = HmsExamples.extract_project("tifton")
    print(f"Project: {project_path}")

    results_dir = project_path / "results"
    if not results_dir.is_dir():
        results_dir = project_path
    results_files = sorted(results_dir.glob("RUN_*.results"))
    print(f"Results dir: {results_dir}")
    print(f"Results files: {[r.name for r in results_files]}")
    return Path, project_path, results_dir, results_files


@app.cell
def __(Path, results_dir):
    from hms2cng.results import export_hms_results

    out_dir = Path("out/02_export_results")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Export subbasin peak outflow results
    subbasin_out = out_dir / "subbasin_outflow.parquet"
    export_hms_results(
        results_dir,
        subbasin_out,
        element_type="subbasin",
        variable="Outflow",
    )
    print(f"Subbasin outflow -> {subbasin_out}")
    return export_hms_results, out_dir, subbasin_out


@app.cell
def __(subbasin_out):
    import geopandas as gpd

    gdf = gpd.read_parquet(subbasin_out)
    print(f"Rows: {len(gdf)}")
    print(f"Columns: {list(gdf.columns)}")
    print()
    # Show peak outflow table sorted descending
    cols = [c for c in ["name", "max_value", "time_of_max", "units"] if c in gdf.columns]
    print(gdf[cols].sort_values("max_value", ascending=False).to_string(index=False))
    return gdf, gpd


@app.cell
def __(export_hms_results, out_dir, results_dir):
    # Export all element types (subbasins + junctions + reaches, etc.)
    all_out = out_dir / "all_outflow.parquet"
    try:
        export_hms_results(
            results_dir,
            all_out,
            element_type="all",
            variable="Outflow",
        )
        import geopandas as gpd2
        gdf_all = gpd2.read_parquet(all_out)
        print(f"All elements outflow: {len(gdf_all)} rows")
        if "type" in gdf_all.columns:
            print(gdf_all.groupby("type").size().to_string())
    except ValueError as e:
        print(f"All-element export: {e}")
    return


if __name__ == "__main__":
    app.run()
