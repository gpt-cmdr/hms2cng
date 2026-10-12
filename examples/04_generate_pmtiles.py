import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    04 — Generate PMTiles
    =====================
    Generates PMTiles (serverless vector tiles) from GeoParquet for web visualization.
    Uses the freestiler tiler installed with hms2cng. With
    HMS2CNG_VECTOR_TILER=tippecanoe it needs the tippecanoe and pmtiles CLIs on PATH:
        conda install -c conda-forge tippecanoe pmtiles

    Output goes to out/04_generate_pmtiles/
    """
    import marimo as mo
    mo.md("## 04 — Generate PMTiles")


@app.cell
def __():
    import importlib.util
    import os
    import shutil

    # The default freestiler backend needs no external tools; tippecanoe needs two CLIs.
    backend = os.environ.get("HMS2CNG_VECTOR_TILER", "freestiler").strip().lower() or "freestiler"
    if backend == "tippecanoe":
        tiler_available = all(shutil.which(cli) for cli in ("tippecanoe", "pmtiles"))
    else:
        tiler_available = importlib.util.find_spec("freestiler") is not None

    print(f"Vector tiler: {backend} ({'available' if tiler_available else 'not installed'})")
    if not tiler_available:
        print("\nInstall freestiler (pip install freestiler), or the tippecanoe CLIs:")
        print("  conda install -c conda-forge tippecanoe pmtiles")
    return backend, tiler_available


@app.cell
def __():
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
def __(Path, backend, subbasins_parquet, tiler_available):
    from hms2cng.pmtiles import generate_pmtiles_from_input

    out_dir = Path("out/04_generate_pmtiles")
    out_dir.mkdir(parents=True, exist_ok=True)
    pmtiles_out = out_dir / "subbasins.pmtiles"

    if tiler_available:
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
        print(f"Skipped — the {backend} vector tiler is not available.")
    return generate_pmtiles_from_input, out_dir, pmtiles_out


if __name__ == "__main__":
    app.run()
