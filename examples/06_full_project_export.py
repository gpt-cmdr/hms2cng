import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    from pathlib import Path
    from hms_commander import HmsExamples

    # Auto-extract the river_bend example (has 3 basin models, 3 runs)
    project_path = HmsExamples.extract_project("river_bend")
    hms_file = next(project_path.glob("*.hms"))
    print(f"Project: {project_path}")
    print(f"HMS file: {hms_file}")
    return Path, HmsExamples, hms_file, project_path


@app.cell
def __(hms_file):
    """Show the project manifest before exporting."""
    from hms2cng.project import get_project_manifest
    import json

    manifest = get_project_manifest(hms_file)
    print(f"Project:      {manifest['project_name']}")
    print(f"CRS:          {manifest['crs_epsg']}")
    print(f"Basin models: {json.loads(manifest['basin_models'])}")
    print(f"Runs ({manifest['num_runs']}):  {json.loads(manifest['run_names'])}")
    return get_project_manifest, json, manifest


@app.cell
def __(hms_file):
    """Show the run registry (run → basin + met + control)."""
    import pandas as pd
    from hms2cng.project import _find_hms_file, _init_project

    prj = _init_project(_find_hms_file(hms_file))
    run_df = prj.run_df[["name", "basin_model", "met_model", "control_spec"]].copy()
    run_df.columns = ["run_name", "basin_model", "met_model", "control_spec"]
    print(run_df.to_string(index=False))
    return _find_hms_file, _init_project, pd, prj, run_df


@app.cell
def __(Path, hms_file):
    """Export the full project archive."""
    from hms2cng.project import export_full_project

    out_dir = Path("out/06_full_project_export")
    summary = export_full_project(hms_file, out_dir)

    print(f"Manifest:        {summary['manifest']}")
    print(f"Run registry:    {summary['run_registry']}")
    print(f"Basin inventory: {summary['basin_inventory']}")
    print(f"Geometry files:  {len(summary['geometry_files'])}")
    print(f"Results files:   {len(summary['results_files'])}")
    if summary["errors"]:
        print("\nErrors:")
        for e in summary["errors"]:
            print(f"  {e}")
    return export_full_project, out_dir, summary


@app.cell
def __(out_dir, pd):
    """Inspect the run_registry.parquet."""
    registry = pd.read_parquet(out_dir / "run_registry.parquet")
    print("Run Registry:")
    print(registry[["run_name", "basin_model", "met_model", "control_spec", "has_results"]].to_string(index=False))
    return (registry,)


@app.cell
def __(out_dir, pd):
    """Inspect the basin_inventory.parquet."""
    inventory = pd.read_parquet(out_dir / "basin_inventory.parquet")
    print("Basin Inventory:")
    print(inventory[["basin_model", "num_subbasins", "num_reaches", "num_junctions", "total_area"]].to_string(index=False))
    return (inventory,)


@app.cell
def __(out_dir, summary):
    """Load and inspect one geometry file."""
    import geopandas as gpd

    if summary["geometry_files"]:
        gf = summary["geometry_files"][0]
        gdf = gpd.read_parquet(gf)
        print(f"File: {gf.relative_to(out_dir)}")
        print(f"Rows: {len(gdf)}  |  CRS: {gdf.crs}  |  Geom: {gdf.geometry.geom_type.unique()}")
        print(f"Columns: {list(gdf.columns)}")
        print(gdf[["name", "project_name", "basin_model"]].head(5).to_string(index=False))
    return (gpd,)


@app.cell
def __(out_dir, summary):
    """Cross-run DuckDB query across all results files."""
    import duckdb

    if not summary["results_files"]:
        print("No results files to query.")
    else:
        # Build a glob pattern covering all run subdirectories
        glob_pattern = str(out_dir / "results" / "*" / "*.parquet")
        conn = duckdb.connect()
        try:
            conn.execute("INSTALL spatial; LOAD spatial;")
        except Exception:
            pass
        df = conn.execute(f"""
            SELECT project_name, run_name, basin_model, name, max_value, units
            FROM read_parquet('{glob_pattern}', union_by_name=true)
            WHERE max_value IS NOT NULL
            ORDER BY max_value DESC
            LIMIT 15
        """).df()
        print("Top peak flows across all runs:")
        print(df.to_string(index=False))
    return (duckdb,)


if __name__ == "__main__":
    app.run()
