import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    04 — Generate PMTiles
    =====================
    Generates PMTiles (serverless vector tiles) from GeoParquet for web visualization.
    Requires tippecanoe and pmtiles CLIs on PATH (not available via pip).

    Install via conda-forge:
        conda install -c conda-forge tippecanoe pmtiles

    Output goes to out/04_generate_pmtiles/
    """
    import marimo as mo
    mo.md("## 04 — Generate PMTiles")


@app.cell
def __():
    import shutil

    # Check for required external CLIs
    tippecanoe_available = shutil.which("tippecanoe") is not None
    pmtiles_available = shutil.which("pmtiles") is not None

    print(f"tippecanoe on PATH: {tippecanoe_available}")
    print(f"pmtiles on PATH:    {pmtiles_available}")

    if not (tippecanoe_available and pmtiles_available):
        print("\nNote: Install via conda-forge to enable PMTiles generation:")
        print("  conda install -c conda-forge tippecanoe pmtiles")
    return pmtiles_available, shutil, tippecanoe_available


@app.cell
def __(tippecanoe_available):
    from pathlib import Path
    from hms_commander import HmsExamples
    from hms2cng.geometry import export_basin_geometry

    # Ensure subbasins.parquet exists
    geom_dir = Path("out/01_export_geometry")
    geom_dir.mkdir(parents=True, exist_ok=True)
    subbasins_parquet = geom_dir / "subbasins.parquet"

    if not subbasins_parquet.exists():
        print("Generating subbasins.parquet from Tifton example project...")
        project_path = HmsExamples.extract_project("tifton")
        basin = next(project_path.glob("*.basin"))
        export_basin_geometry(basin, subbasins_parquet, layer="subbasins")

    print(f"Input:  {subbasins_parquet}")
    return Path, subbasins_parquet


@app.cell
def __(Path, pmtiles_available, subbasins_parquet, tippecanoe_available):
    from hms2cng.pmtiles import generate_pmtiles_from_input

    out_dir = Path("out/04_generate_pmtiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    pmtiles_out = out_dir / "subbasins.pmtiles"

    if tippecanoe_available and pmtiles_available:
        generate_pmtiles_from_input(
            subbasins_parquet,
            pmtiles_out,
            layer_name="subbasins",
            min_zoom=8,
            max_zoom=14,
        )
        import os
        size_kb = os.path.getsize(pmtiles_out) / 1024
        print(f"Generated: {pmtiles_out}")
        print(f"Size:      {size_kb:.1f} KB")
        print()
        print("Serve with: npx pmtiles serve out/04_generate_pmtiles/")
        print("View at:    https://pmtiles.io (drag and drop the file)")
    else:
        print("Skipped — tippecanoe or pmtiles not found on PATH.")
        print("Install: conda install -c conda-forge tippecanoe pmtiles")
    return generate_pmtiles_from_input, out_dir, pmtiles_out


if __name__ == "__main__":
    app.run()
