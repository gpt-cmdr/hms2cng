"""PMTiles generation for hms2cng.

Notes
-----
The original scaffold assumed `tippecanoe -o out.pmtiles ...` which is not how
Tippecanoe works: it outputs MBTiles. To produce PMTiles you typically:

  1) tippecanoe -> out.mbtiles
  2) pmtiles convert out.mbtiles out.pmtiles

This module implements that pipeline and emits friendly errors when the required
CLIs are not available.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import shutil
import subprocess
import tempfile

import geopandas as gpd


def _require_cmd(cmd: str) -> str:
    exe = shutil.which(cmd)
    if not exe:
        raise FileNotFoundError(
            f"Required command not found on PATH: {cmd}. "
            "Install it (e.g. conda-forge tippecanoe, protomaps pmtiles) or run under WSL/Linux."
        )
    return exe


def generate_pmtiles_from_input(
    input_file: Path,
    output: Path,
    layer_name: str = "layer",
    min_zoom: Optional[int] = None,
    max_zoom: Optional[int] = None,
):
    """Generate PMTiles (or MBTiles) from GeoParquet (vector)."""

    input_path = Path(input_file)
    output_path = Path(output)

    if input_path.suffix.lower() not in {".parquet", ".gpq"}:
        raise ValueError(f"Unsupported input format: {input_path.suffix}")

    if output_path.suffix.lower() not in {".pmtiles", ".mbtiles"}:
        raise ValueError("Output must end with .pmtiles or .mbtiles")

    generate_vector_tiles(
        input_path,
        output_path,
        layer_name=layer_name,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
    )


def generate_vector_tiles(
    input_file: Path,
    output: Path,
    layer_name: str = "layer",
    min_zoom: Optional[int] = None,
    max_zoom: Optional[int] = None,
):
    """Generate vector tiles from GeoParquet.

    If output is .pmtiles, performs: tippecanoe -> mbtiles -> pmtiles convert.
    """

    tippecanoe = _require_cmd("tippecanoe")

    # Read GeoParquet
    gdf = gpd.read_parquet(input_file)

    # Convert to GeoJSON intermediate
    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        geojson_path = Path(tmp.name)

    gdf.to_file(geojson_path, driver="GeoJSON")

    try:
        if output.suffix.lower() == ".mbtiles":
            mbtiles_path = output
        else:
            mbtiles_path = output.with_suffix(".mbtiles")

        # Build tippecanoe command
        cmd = [
            tippecanoe,
            "-o",
            str(mbtiles_path),
            "--layer",
            layer_name,
            "-zg",  # auto zooms
            "--force",
        ]

        if min_zoom is not None:
            cmd.extend(["-Z", str(min_zoom)])
        if max_zoom is not None:
            cmd.extend(["-z", str(max_zoom)])

        cmd.append(str(geojson_path))

        subprocess.run(cmd, check=True)

        if output.suffix.lower() == ".pmtiles":
            pmtiles = _require_cmd("pmtiles")
            subprocess.run([pmtiles, "convert", str(mbtiles_path), str(output), "--force"], check=True)

    finally:
        geojson_path.unlink(missing_ok=True)


def generate_watershed_overview(
    basin_file: Path,
    results_dir: Path,
    output: Path,
):
    """Generate PMTiles with a simple watershed overview (subbasins + outflow peaks).

    This is a convenience function; real-world styling is usually done downstream.
    """

    from hms2cng.geometry import export_basin_geometry
    from hms2cng.results import export_hms_results

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Export geometry
        export_basin_geometry(basin_file, tmpdir / "subbasins.parquet", layer="subbasins")

        # Export results summary
        export_hms_results(results_dir, tmpdir / "subbasin_results.parquet", element_type="subbasin", variable="Outflow")

        # Merge
        subbasins = gpd.read_parquet(tmpdir / "subbasins.parquet")
        results = gpd.read_parquet(tmpdir / "subbasin_results.parquet")

        merged = subbasins.merge(
            results[["name", "max_value", "time_of_max", "mean_value", "units"]],
            on="name",
            how="left",
        )
        merged_path = tmpdir / "merged.parquet"
        merged.to_parquet(merged_path)

        # Generate tiles
        generate_vector_tiles(
            merged_path,
            Path(output),
            layer_name="watershed",
            min_zoom=8,
            max_zoom=16,
        )
