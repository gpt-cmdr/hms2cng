import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    01 — Export Basin Geometry
    ==========================
    Exports all geometry layers (subbasins, junctions, reaches, watershed)
    from the Tifton HMS example project to GeoParquet.
    Output goes to out/01_export_geometry/
    """
    import marimo as mo
    mo.md("## 01 — Export Basin Geometry")


@app.cell
def __():
    from pathlib import Path
    from hms_commander import HmsExamples

    # Auto-extract the Tifton example project (bundled with HMS installation)
    project_path = HmsExamples.extract_project("tifton")
    print(f"Project: {project_path}")

    basin = next(project_path.glob("*.basin"))
    print(f"Basin file: {basin.name}")
    return HmsExamples, Path, basin, project_path


@app.cell
def __(Path, basin):
    from hms2cng.geometry import export_basin_geometry, get_basin_layer_gdf

    out_dir = Path("out/01_export_geometry")
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Subbasins (point geometry, one per subbasin) ---
    gdf_subbasins = get_basin_layer_gdf(basin, layer="subbasins")
    print(f"Subbasins: {len(gdf_subbasins)} features, CRS={gdf_subbasins.crs}")
    print(gdf_subbasins[["name", "area"]].head())

    export_basin_geometry(basin, out_dir / "subbasins.parquet", layer="subbasins")
    print(f"  -> {out_dir / 'subbasins.parquet'}")
    return export_basin_geometry, get_basin_layer_gdf, gdf_subbasins, out_dir


@app.cell
def __(basin, export_basin_geometry, out_dir):
    # --- Junctions ---
    export_basin_geometry(basin, out_dir / "junctions.parquet", layer="junctions")

    import geopandas as gpd
    gdf_j = gpd.read_parquet(out_dir / "junctions.parquet")
    print(f"Junctions: {len(gdf_j)} features")
    print(gdf_j[["name"]].head())
    return gdf_j, gpd


@app.cell
def __(basin, export_basin_geometry, out_dir):
    # --- Reaches (linestring geometry) ---
    try:
        export_basin_geometry(basin, out_dir / "reaches.parquet", layer="reaches")
        import geopandas as gpd2
        gdf_r = gpd2.read_parquet(out_dir / "reaches.parquet")
        print(f"Reaches: {len(gdf_r)} features, geom_type={gdf_r.geometry.iloc[0].geom_type}")
    except (ValueError, FileNotFoundError) as e:
        print(f"Reaches: skipped — {e}")
    return


@app.cell
def __(basin, export_basin_geometry, out_dir):
    # --- Watershed boundary polygon (requires .map file) ---
    try:
        export_basin_geometry(basin, out_dir / "watershed.parquet", layer="watershed")
        import geopandas as gpd3
        gdf_w = gpd3.read_parquet(out_dir / "watershed.parquet")
        print(f"Watershed: {len(gdf_w)} polygon(s), area_m2={gdf_w.geometry.area.sum():.0f}")
    except (ValueError, FileNotFoundError) as e:
        print(f"Watershed: skipped — {e}")
    return


@app.cell
def __(out_dir):
    import os
    files = sorted(out_dir.glob("*.parquet"))
    print(f"\nOutput files in {out_dir}:")
    for f in files:
        size_kb = os.path.getsize(f) / 1024
        print(f"  {f.name:40s}  {size_kb:.1f} KB")
    return


if __name__ == "__main__":
    app.run()
