from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from hms2cng.geometry import get_basin_layer_gdf, export_basin_geometry


def _write_minimal_basin(path: Path) -> None:
    # Minimal basin file that hms-commander's HmsBasin parser can read.
    path.write_text(
        """
Basin: Test

Subbasin: S1
  Area: 1.0
  Downstream: J1
  Canvas X: 10
  Canvas Y: 20
End:

Junction: J1
  Downstream: 
  Canvas X: 30
  Canvas Y: 40
End:

Reach: R1
  Downstream: J1
  Route: Muskingum
  From Canvas X: 10
  From Canvas Y: 20
  Canvas X: 30
  Canvas Y: 40
End:
""".lstrip(),
        encoding="utf-8",
    )


def test_get_subbasins_points(tmp_path: Path):
    basin = tmp_path / "test.basin"
    _write_minimal_basin(basin)

    gdf = get_basin_layer_gdf(basin, layer="subbasins", crs_epsg=None, out_crs=None)
    assert len(gdf) == 1
    assert gdf.iloc[0]["name"] == "S1"
    assert gdf.geometry.iloc[0].geom_type == "Point"
    assert round(gdf.geometry.iloc[0].x, 6) == 10
    assert round(gdf.geometry.iloc[0].y, 6) == 20


def test_get_reaches_lines(tmp_path: Path):
    basin = tmp_path / "test.basin"
    _write_minimal_basin(basin)

    gdf = get_basin_layer_gdf(basin, layer="reaches", crs_epsg=None, out_crs=None)
    assert len(gdf) == 1
    assert gdf.iloc[0]["name"] == "R1"
    assert gdf.geometry.iloc[0].geom_type == "LineString"


def test_export_geometry_parquet(tmp_path: Path):
    basin = tmp_path / "test.basin"
    _write_minimal_basin(basin)

    out = tmp_path / "subbasins.parquet"
    export_basin_geometry(basin, out, layer="subbasins", crs_epsg=None, out_crs=None)
    assert out.exists()

    gdf = gpd.read_parquet(out)
    assert len(gdf) == 1
    assert gdf.iloc[0]["name"] == "S1"
