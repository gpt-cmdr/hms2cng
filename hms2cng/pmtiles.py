"""PMTiles generation for hms2cng.

Notes
-----
The original scaffold assumed `tippecanoe -o out.pmtiles ...` which is not how
older Tippecanoe works: it outputs MBTiles. To produce PMTiles you typically:

  1) tippecanoe -> out.mbtiles
  2) pmtiles convert out.mbtiles out.pmtiles

This module implements that pipeline and emits friendly errors when the required
CLIs are not available.

Tiling policy here mirrors ``ras2cng.cog`` / ``ras2cng.pmtiles``, which is the
upstream reference for both repos:

* Geometry is reprojected to EPSG:4326 before serialization. Tippecanoe reads
  RFC 7946 GeoJSON and does not reproject; handing it projected coordinates
  produces a valid, silently wrong tileset rather than an error.
* Intermediates are written to a scratch directory and the final artifact is
  swapped into place atomically, so a failure part-way through cannot destroy
  the previous good tileset or leave an orphan beside it.
* Tippecanoe's stderr is logged rather than discarded, because it reports
  dropped features on a zero exit.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Iterator, Optional
import uuid

import geopandas as gpd

LOGGER = logging.getLogger(__name__)


def _require_cmd(cmd: str) -> str:
    exe = shutil.which(cmd)
    if not exe:
        raise FileNotFoundError(
            f"Required command not found on PATH: {cmd}. "
            "Install it (e.g. conda-forge tippecanoe, protomaps pmtiles) or run under WSL/Linux."
        )
    return exe


@contextmanager
def _atomic_output(path: Path) -> Iterator[Path]:
    """Yield a staging path beside ``path``; replace ``path`` only on success.

    The partial is a hidden sibling in the destination directory, so the rename
    stays on one filesystem and is atomic, and it is PID/UUID-namespaced so
    concurrent writers cannot collide. The previous good artifact survives a
    failed rebuild.
    """

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(
        f".{destination.name}.partial-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    )
    try:
        yield staged
        if not staged.exists():
            raise FileNotFoundError(
                f"Staged output was never written and {destination} was left untouched: {staged}"
            )
        os.replace(staged, destination)
    finally:
        staged.unlink(missing_ok=True)


def _bounded_tiles_requested() -> bool:
    """Whether to cap tile size instead of choosing fidelity unconditionally.

    ``--drop-densest-as-needed`` is a *ceiling*, not a mandate: on a sparse layer
    it never fires, so enabling it costs nothing where density is not a problem.
    It stays opt-in because most HMS layers -- subbasins, junctions, outlets --
    are sparse enough that the ceiling is irrelevant, while a large basin model's
    reach and flowpath layers can benefit.
    """

    return os.environ.get("HMS2CNG_TIPPECANOE_BOUNDED", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _log_tool_output(tool: str, stderr: str | None) -> None:
    """Surface a tool's diagnostics instead of discarding them."""

    for line in (stderr or "").splitlines():
        text = line.strip()
        if not text:
            continue
        if any(token in text.lower() for token in ("dropp", "exceed", "too large", "warning")):
            LOGGER.warning("%s: %s", tool, text)
        else:
            LOGGER.debug("%s: %s", tool, text)


def _to_wgs84(gdf: gpd.GeoDataFrame, source: Path) -> gpd.GeoDataFrame:
    """Reproject to EPSG:4326, the only CRS tippecanoe accepts.

    HMS basin geometry reaches this module in whatever CRS ``crs_epsg`` /
    ``out_crs`` produced -- commonly a State Plane zone in feet, and sometimes
    none at all when the basin file carried no projection. Feeding projected
    coordinates to tippecanoe yields values in the millions, which are clamped
    or wrapped into a meaningless tileset with no error raised.
    """

    if gdf.crs is None:
        raise ValueError(
            f"Layer has no CRS, so it cannot be reprojected for tiling: {source}. "
            "Re-export with --crs-epsg (or --out-crs) so the source projection is recorded."
        )
    if gdf.crs.to_epsg() == 4326:
        return gdf
    return gdf.to_crs("EPSG:4326")


def _write_ndgeojson(gdf: gpd.GeoDataFrame, path: Path) -> int:
    """Write features as newline-delimited GeoJSON, one line at a time.

    Avoids materializing the whole layer as a single string, and avoids the
    GeoJSON driver's default of writing coordinates in the source CRS.
    """

    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for feature in gdf.iterfeatures(drop_id=True, na="null", show_bbox=False):
            json.dump(feature, handle, default=str, separators=(",", ":"))
            handle.write("\n")
            count += 1
    return count


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
    *,
    bounded: Optional[bool] = None,
):
    """Generate vector tiles from GeoParquet.

    If output is .pmtiles, performs: tippecanoe -> mbtiles -> pmtiles convert.

    ``bounded`` caps individual tile size with ``--drop-densest-as-needed``;
    paired with ``--extend-zooms-if-still-dropping`` that moves features to a
    higher zoom rather than discarding them.  Defaults to the
    ``HMS2CNG_TIPPECANOE_BOUNDED`` environment variable.
    """

    tippecanoe = _require_cmd("tippecanoe")
    if bounded is None:
        bounded = _bounded_tiles_requested()
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    wants_pmtiles = output.suffix.lower() == ".pmtiles"
    if wants_pmtiles:
        # Resolve the second tool before doing any expensive work, so a missing
        # binary fails before the tileset is built rather than after.
        pmtiles = _require_cmd("pmtiles")

    gdf = _to_wgs84(gpd.read_parquet(input_file), Path(input_file))

    with tempfile.TemporaryDirectory(prefix="hms2cng-tiles-") as scratch:
        scratch_dir = Path(scratch)
        geojson_path = scratch_dir / "layer.ndgeojson"
        if not _write_ndgeojson(gdf, geojson_path):
            raise ValueError(f"No features to tile in {input_file}")

        # The intermediate stays in scratch: writing it beside the output left
        # an orphan .mbtiles in the delivery directory on every run.
        mbtiles_path = scratch_dir / f"{output.stem}.mbtiles"

        cmd = [
            tippecanoe,
            "-o",
            str(mbtiles_path),
            "--layer",
            layer_name,
            "-zg",  # auto zooms
            "--force",
            # HMS subbasins tile the watershed and share every interior
            # boundary. Simplified independently, those shared edges diverge
            # and open visible slivers between subbasins at low zoom.
            "--no-simplification-of-shared-nodes",
            "--temporary-directory",
            str(scratch_dir),
        ]

        if bounded:
            # Never --drop-fraction-as-needed: it makes tippecanoe retry the
            # sparsest features until the native binary crashes on a dense mixed
            # geometry layer.
            cmd.append("--drop-densest-as-needed")
            # Only acts when features are dropping, so it is inert unless the
            # ceiling above actually fires.
            cmd.append("--extend-zooms-if-still-dropping")

        if min_zoom is not None:
            cmd.extend(["-Z", str(min_zoom)])
        if max_zoom is not None:
            cmd.extend(["-z", str(max_zoom)])

        cmd.append(str(geojson_path))

        completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
        _log_tool_output("tippecanoe", completed.stderr)

        with _atomic_output(output) as staged:
            if wants_pmtiles:
                converted = subprocess.run(
                    [pmtiles, "convert", str(mbtiles_path), str(staged), "--force"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                _log_tool_output("pmtiles", converted.stderr)
            else:
                shutil.move(str(mbtiles_path), str(staged))


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
