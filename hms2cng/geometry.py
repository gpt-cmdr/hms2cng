"""hms2cng geometry utilities.

This module intentionally uses the *public* hms-commander API (v0.2+).
The earlier scaffold referenced non-existent symbols (HmsProject, hms_commander.geometry).*.

Primary source of element coordinates for most HMS models is the basin file
(Canvas X/Y, From Canvas X/Y). Where a .map file exists, we can also build
polygon boundaries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Literal

import geopandas as gpd
import pandas as pd


LayerName = Literal[
    "subbasins", "reaches", "junctions", "diversions", "reservoirs", "sources", "sinks",
    "watershed",
    # SQLite-based layers (gridded HMS models with terrain preprocessing)
    "subbasin_polygons", "longest_flowpaths", "centroidal_flowpaths",
    "teneightyfive_flowpaths", "subbasin_statistics",
]


def _ensure_parent(path: Path) -> None:
    path = Path(path)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def _find_basin_file(basin_input: Path) -> Path:
    basin_input = Path(basin_input)

    if basin_input.is_file() and basin_input.suffix.lower() == ".basin":
        return basin_input

    if basin_input.is_dir():
        basin_files = sorted(basin_input.glob("*.basin"))
        if basin_files:
            return basin_files[0]

    raise FileNotFoundError(
        f"Could not locate a .basin file from input: {basin_input} (pass a .basin or a folder containing one)"
    )


def _find_sqlite_file(project_dir: Path, basin_file: Path) -> Optional[Path]:
    """Find the .sqlite file associated with a basin file.

    Looks for a .sqlite file whose stem matches the basin file stem,
    then falls back to the first .sqlite in the project directory.
    """
    project_dir = Path(project_dir)
    # Prefer matching stem (e.g. Harvey_2017.basin -> Harvey_2017.sqlite)
    exact = project_dir / f"{basin_file.stem}.sqlite"
    if exact.is_file():
        return exact
    # Fall back to any .sqlite
    sqlite_files = sorted(project_dir.glob("*.sqlite"))
    return sqlite_files[0] if sqlite_files else None


def _maybe_to_crs(gdf: gpd.GeoDataFrame, out_crs: Optional[str]) -> gpd.GeoDataFrame:
    if out_crs is None:
        return gdf
    if gdf.crs is None:
        # Cannot transform without a CRS. Keep native coordinates.
        return gdf
    return gdf.to_crs(out_crs)


def get_basin_layer_gdf(
    basin_input: Path,
    layer: LayerName = "subbasins",
    *,
    crs_epsg: Optional[str] = None,
    out_crs: Optional[str] = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """Return a GeoDataFrame for a basin geometry layer.

    Notes:
      - If crs_epsg is not provided and cannot be inferred, output CRS will be
        unset and no transformation to out_crs will occur.
      - For "watershed", requires a .map file to construct polygons.
    """

    basin_file = _find_basin_file(basin_input)
    project_dir = basin_file.parent

    # Try to infer CRS from HMS project if present.
    if crs_epsg is None:
        try:
            from hms_commander import init_hms_project

            h = init_hms_project(project_dir)
            crs_epsg = h.crs_epsg
        except Exception:
            crs_epsg = None

    from shapely.geometry import Point, LineString

    from hms_commander import HmsBasin

    if layer == "subbasins":
        df = HmsBasin.get_subbasins(basin_file)
        if df.empty:
            raise ValueError(f"No subbasins found in basin: {basin_file}")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df["canvas_x"], df["canvas_y"])],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    if layer == "junctions":
        df = HmsBasin.get_junctions(basin_file)
        if df.empty:
            raise ValueError(f"No junctions found in basin: {basin_file}")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df["canvas_x"], df["canvas_y"])],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    if layer == "reaches":
        df = HmsBasin.get_reaches(basin_file)
        if df.empty:
            raise ValueError(f"No reaches found in basin: {basin_file}")

        lines = []
        for _, r in df.iterrows():
            fx, fy = r.get("from_canvas_x"), r.get("from_canvas_y")
            tx, ty = r.get("canvas_x"), r.get("canvas_y")
            if pd.isna(fx) or pd.isna(fy) or pd.isna(tx) or pd.isna(ty):
                lines.append(None)
            else:
                lines.append(LineString([(float(fx), float(fy)), (float(tx), float(ty))]))

        gdf = gpd.GeoDataFrame(df, geometry=lines, crs=crs_epsg)
        return _maybe_to_crs(gdf, out_crs)

    if layer == "diversions":
        df = HmsBasin.get_diversions(basin_file)
        if df.empty:
            raise ValueError(f"No diversions found in basin: {basin_file}")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df["canvas_x"], df["canvas_y"])],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    if layer == "reservoirs":
        df = HmsBasin.get_reservoirs(basin_file)
        if df.empty:
            raise ValueError(f"No reservoirs found in basin: {basin_file}")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df["canvas_x"], df["canvas_y"])],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    if layer == "sources":
        df = HmsBasin.get_sources(basin_file)
        if df.empty:
            raise ValueError(f"No sources found in basin: {basin_file}")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df["canvas_x"], df["canvas_y"])],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    if layer == "sinks":
        df = HmsBasin.get_sinks(basin_file)
        if df.empty:
            raise ValueError(f"No sinks found in basin: {basin_file}")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=[Point(xy) for xy in zip(df["canvas_x"], df["canvas_y"])],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    # --- SQLite-based layers (gridded models with terrain data) ---

    if layer in ("subbasin_polygons", "longest_flowpaths", "centroidal_flowpaths",
                 "teneightyfive_flowpaths", "subbasin_statistics"):
        from hms_commander import HmsSqlite

        sqlite_file = _find_sqlite_file(project_dir, basin_file)
        if sqlite_file is None:
            raise FileNotFoundError(
                f"No .sqlite file found in {project_dir}. "
                f"Layer '{layer}' requires an HMS SQLite grid database."
            )

        # Map layer names to HmsSqlite methods
        _sqlite_geo_methods = {
            "subbasin_polygons": ("get_subbasins", "subbasin polygons"),
            "longest_flowpaths": ("get_longest_flowpaths", "longest flowpaths"),
            "centroidal_flowpaths": ("get_centroidal_flowpaths", "centroidal flowpaths"),
            "teneightyfive_flowpaths": ("get_teneightyfive_flowpaths", "10-85 flowpaths"),
        }

        if layer in _sqlite_geo_methods:
            method_name, label = _sqlite_geo_methods[layer]
            try:
                gdf = getattr(HmsSqlite, method_name)(sqlite_file)
            except ValueError as exc:
                raise ValueError(f"No {label} found in: {sqlite_file}") from exc
            if gdf.empty:
                raise ValueError(f"No {label} found in: {sqlite_file}")
            return _maybe_to_crs(gdf, out_crs)

        if layer == "subbasin_statistics":
            try:
                df = HmsSqlite.get_subbasin_statistics(sqlite_file)
            except ValueError as exc:
                raise ValueError(f"No subbasin statistics found in: {sqlite_file}") from exc
            if df.empty:
                raise ValueError(f"No subbasin statistics found in: {sqlite_file}")
            # Non-spatial: use subbasin point geometry for spatial reference
            sub_df = HmsBasin.get_subbasins(basin_file)
            if not sub_df.empty:
                from shapely.geometry import Point
                sub_gdf = gpd.GeoDataFrame(
                    sub_df[["name", "canvas_x", "canvas_y"]],
                    geometry=[Point(xy) for xy in zip(sub_df["canvas_x"], sub_df["canvas_y"])],
                    crs=crs_epsg,
                )
                merged = sub_gdf.merge(df, left_on="name", right_on="subbasin_name", how="right")
                merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=crs_epsg)
                return _maybe_to_crs(merged, out_crs)
            # Fallback: return as plain DataFrame (no geometry)
            return gpd.GeoDataFrame(df)

    if layer == "watershed":
        # Requires a .map file.
        map_files = sorted(project_dir.glob("*.map"))
        if not map_files:
            raise FileNotFoundError(
                f"No .map file found in {project_dir}. "
                "Watershed boundary extraction requires the HMS map file."
            )

        from hms_commander import HmsGeo
        from shapely.geometry import Polygon
        from shapely.ops import unary_union

        map_data = HmsGeo.parse_map_file(map_files[0])
        boundaries = map_data.get("boundaries", [])
        polys = []
        for b in boundaries:
            coords = b.get("coordinates") or []
            if len(coords) < 3:
                continue
            # ensure closed
            if coords[0] != coords[-1]:
                coords = coords + [coords[0]]
            try:
                polys.append(Polygon(coords))
            except Exception:
                continue

        if not polys:
            raise ValueError(f"No boundary polygons found in map file: {map_files[0]}")

        union = unary_union(polys)
        gdf = gpd.GeoDataFrame(
            [{"name": "watershed"}],
            geometry=[union],
            crs=crs_epsg,
        )
        return _maybe_to_crs(gdf, out_crs)

    raise ValueError(f"Unknown layer: {layer}")


def export_basin_geometry(
    basin_input: Path,
    output: Path,
    layer: Optional[str] = None,
    *,
    crs_epsg: Optional[str] = None,
    out_crs: Optional[str] = "EPSG:4326",
) -> None:
    """Export a basin geometry layer to GeoParquet."""

    _ensure_parent(Path(output))

    layer_name: LayerName = "subbasins" if layer is None else layer  # type: ignore[assignment]
    valid_layers = {
        "subbasins", "reaches", "junctions", "diversions", "reservoirs", "sources", "sinks",
        "watershed", "subbasin_polygons", "longest_flowpaths", "centroidal_flowpaths",
        "teneightyfive_flowpaths", "subbasin_statistics",
    }
    if layer_name not in valid_layers:
        raise ValueError(f"layer must be one of: {', '.join(sorted(valid_layers))}")

    gdf = get_basin_layer_gdf(
        basin_input,
        layer=layer_name,
        crs_epsg=crs_epsg,
        out_crs=out_crs,
    )

    gdf.to_parquet(output, compression="snappy")


def extract_watershed_boundary(
    basin_file: Path,
    output: Path,
    *,
    crs_epsg: Optional[str] = None,
    out_crs: Optional[str] = "EPSG:4326",
) -> None:
    """Backwards-compatible wrapper."""

    export_basin_geometry(
        basin_file,
        output,
        layer="watershed",
        crs_epsg=crs_epsg,
        out_crs=out_crs,
    )
