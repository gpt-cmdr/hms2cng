from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString

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


def test_get_reaches_prefers_sqlite_reach2d_geometry(tmp_path: Path, monkeypatch):
    basin = tmp_path / "test.basin"
    sqlite_file = tmp_path / "test.sqlite"
    _write_minimal_basin(basin)
    sqlite_file.touch()

    sqlite_gdf = gpd.GeoDataFrame(
        {"name": ["R1"]},
        geometry=[LineString([(100, 200), (300, 400)])],
        crs=None,
    )
    calls = []

    from hms_commander import HmsSqlite

    def fake_get_reaches(path):
        calls.append(Path(path).name)
        return sqlite_gdf

    monkeypatch.setattr(HmsSqlite, "get_reaches", fake_get_reaches)

    gdf = get_basin_layer_gdf(basin, layer="reaches", crs_epsg=None, out_crs=None)

    assert calls == ["test.sqlite"]
    assert list(gdf.iloc[0].geometry.coords) == [(100.0, 200.0), (300.0, 400.0)]


def test_sqlite_flowpath_layer_falls_back_when_preferred_sqlite_lacks_table(
    tmp_path: Path,
    monkeypatch,
):
    basin = tmp_path / "test.basin"
    preferred_sqlite = tmp_path / "test.sqlite"
    fallback_sqlite = tmp_path / "z_flowpaths.sqlite"
    _write_minimal_basin(basin)
    preferred_sqlite.touch()
    fallback_sqlite.touch()

    flowpath_gdf = gpd.GeoDataFrame(
        {"subbasin": ["S1"]},
        geometry=[LineString([(10, 20), (15, 30), (30, 40)])],
        crs=None,
    )
    calls = []

    from hms_commander import HmsSqlite

    def fake_get_longest_flowpaths(path):
        calls.append(Path(path).name)
        if Path(path).name == "test.sqlite":
            raise ValueError("Layer 'longest_flowpath' not found")
        return flowpath_gdf

    monkeypatch.setattr(HmsSqlite, "get_longest_flowpaths", fake_get_longest_flowpaths)

    gdf = get_basin_layer_gdf(
        basin,
        layer="longest_flowpaths",
        crs_epsg=None,
        out_crs=None,
    )

    assert calls == ["test.sqlite", "z_flowpaths.sqlite"]
    assert len(gdf) == 1
    assert gdf.iloc[0]["subbasin"] == "S1"
    assert list(gdf.iloc[0].geometry.coords) == [
        (10.0, 20.0),
        (15.0, 30.0),
        (30.0, 40.0),
    ]


def test_get_outlets_from_terminal_junctions(tmp_path: Path):
    basin = tmp_path / "test.basin"
    _write_minimal_basin(basin)

    gdf = get_basin_layer_gdf(basin, layer="outlets", crs_epsg=None, out_crs=None)
    assert len(gdf) == 1
    assert gdf.iloc[0]["name"] == "J1"
    assert gdf.geometry.iloc[0].geom_type == "Point"
    assert round(gdf.geometry.iloc[0].x, 6) == 30
    assert round(gdf.geometry.iloc[0].y, 6) == 40


def test_export_geometry_parquet(tmp_path: Path):
    basin = tmp_path / "test.basin"
    _write_minimal_basin(basin)

    out = tmp_path / "subbasins.parquet"
    export_basin_geometry(basin, out, layer="subbasins", crs_epsg=None, out_crs=None)
    assert out.exists()

    gdf = gpd.read_parquet(out)
    assert len(gdf) == 1
    assert gdf.iloc[0]["name"] == "S1"
