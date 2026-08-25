"""Tests for the vector tiling pipeline: reprojection, atomicity, tippecanoe flags."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from hms2cng import pmtiles


def _completed(command, stdout: str = "", stderr: str = ""):
    """Stand-in for subprocess.CompletedProcess used by the fake runners."""

    return type(
        "Completed",
        (),
        {"args": command, "returncode": 0, "stdout": stdout, "stderr": stderr},
    )()


def _subbasins(crs: str | None) -> gpd.GeoDataFrame:
    """Two subbasins sharing an interior boundary, as HMS basins do."""

    # Realistic EPSG:2278 (Texas South Central, ftUS) coordinates near Houston,
    # so a missing reprojection cannot accidentally land in a plausible place.
    east, north = 3_100_000, 13_800_000
    left = Polygon([
        (east, north), (east + 1000, north), (east + 1000, north + 1000), (east, north + 1000),
    ])
    right = Polygon([
        (east + 1000, north), (east + 2000, north),
        (east + 2000, north + 1000), (east + 1000, north + 1000),
    ])
    return gpd.GeoDataFrame(
        {"name": ["upper", "lower"], "max_value": [120.0, 340.0]},
        geometry=[left, right],
        crs=crs,
    )


@pytest.fixture
def stub_clis(monkeypatch):
    """Pretend tippecanoe and pmtiles are installed, and record their commands."""

    monkeypatch.setattr(pmtiles, "_require_cmd", lambda name: name)
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(list(command))
        if command[0] == "tippecanoe":
            Path(command[command.index("-o") + 1]).write_bytes(b"mbtiles")
        else:
            Path(command[3]).write_bytes(b"pmtiles")
        return _completed(command)

    monkeypatch.setattr(pmtiles.subprocess, "run", fake_run)
    return calls


# ---------------------------------------------------------------------------
# Reprojection
# ---------------------------------------------------------------------------


def test_projected_geometry_is_reprojected_before_tiling(tmp_path: Path, stub_clis) -> None:
    """Tippecanoe must receive lon/lat, never State Plane feet.

    Tippecanoe reads RFC 7946 GeoJSON and does not reproject. Feeding it
    projected coordinates produces a valid, silently wrong tileset.
    """

    source = tmp_path / "subbasins.parquet"
    # EPSG:2278 is Texas South Central (ftUS) -- coordinates in the millions.
    _subbasins("EPSG:2278").to_parquet(source)
    captured: dict[str, list[dict]] = {}

    original = pmtiles._write_ndgeojson

    def capture(gdf, path):
        count = original(gdf, path)
        captured["features"] = [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]
        return count

    pmtiles._write_ndgeojson = capture
    try:
        pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles", layer_name="subbasins")
    finally:
        pmtiles._write_ndgeojson = original

    coordinates = captured["features"][0]["geometry"]["coordinates"][0]
    longitudes = [point[0] for point in coordinates]
    latitudes = [point[1] for point in coordinates]
    assert all(-180.0 <= value <= 180.0 for value in longitudes), longitudes
    assert all(-90.0 <= value <= 90.0 for value in latitudes), latitudes
    # Sanity: the fixture really is in Texas, so a no-op passthrough would fail.
    assert -107.0 < longitudes[0] < -93.0
    assert 25.0 < latitudes[0] < 37.0


def test_a_layer_without_a_crs_is_refused_rather_than_silently_wrong(
    tmp_path: Path, stub_clis
) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins(None).to_parquet(source)

    with pytest.raises(ValueError, match="no CRS"):
        pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles")


def test_a_wgs84_layer_is_passed_through_unchanged(tmp_path: Path) -> None:
    frame = _subbasins("EPSG:2278").to_crs("EPSG:4326")

    assert pmtiles._to_wgs84(frame, Path("x")) is frame


# ---------------------------------------------------------------------------
# Tippecanoe invocation
# ---------------------------------------------------------------------------


def test_shared_subbasin_boundaries_are_not_simplified_independently(
    tmp_path: Path, stub_clis
) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)

    pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles", layer_name="subbasins")

    tippecanoe = stub_clis[0]
    # Adjacent subbasins share every interior edge; simplified independently
    # those edges diverge and open slivers between them at low zoom.
    assert "--no-simplification-of-shared-nodes" in tippecanoe
    assert tippecanoe[tippecanoe.index("--layer") + 1] == "subbasins"


def test_the_mbtiles_intermediate_stays_out_of_the_delivery_directory(
    tmp_path: Path, stub_clis
) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)
    output = tmp_path / "delivery" / "out.pmtiles"

    pmtiles.generate_vector_tiles(source, output)

    intermediate = Path(stub_clis[0][stub_clis[0].index("-o") + 1])
    assert intermediate.parent != output.parent
    assert not output.with_suffix(".mbtiles").exists()
    assert sorted(p.name for p in output.parent.iterdir()) == ["out.pmtiles"]


def test_mbtiles_output_is_produced_directly(tmp_path: Path, stub_clis) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)
    output = tmp_path / "out.mbtiles"

    pmtiles.generate_vector_tiles(source, output)

    assert output.read_bytes() == b"mbtiles"
    assert len(stub_clis) == 1, "no pmtiles conversion is needed for an .mbtiles target"


def test_tile_size_is_unbounded_by_default(tmp_path: Path, stub_clis) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)

    pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles")

    assert "--drop-densest-as-needed" not in stub_clis[0]


def test_bounded_caps_tile_size_without_risking_the_crashing_flag(
    tmp_path: Path, stub_clis
) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)

    pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles", bounded=True)

    command = stub_clis[0]
    assert "--drop-densest-as-needed" in command
    # Inert unless the ceiling fires; when it does, dropped features move to a
    # higher zoom rather than disappearing.
    assert "--extend-zooms-if-still-dropping" in command
    # Never this one: it makes tippecanoe retry the sparsest features until the
    # native binary crashes on a dense mixed geometry layer.
    assert "--drop-fraction-as-needed" not in command


def test_bounded_can_be_requested_by_environment(tmp_path: Path, stub_clis, monkeypatch) -> None:
    monkeypatch.setenv("HMS2CNG_TIPPECANOE_BOUNDED", "1")
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)

    pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles")

    assert "--drop-densest-as-needed" in stub_clis[0]


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------


def test_a_failed_conversion_keeps_the_previous_tileset(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)
    output = tmp_path / "out.pmtiles"
    output.write_bytes(b"previous-good-tileset")

    monkeypatch.setattr(pmtiles, "_require_cmd", lambda name: name)

    def fake_run(command, **_kwargs):
        if command[0] == "tippecanoe":
            Path(command[command.index("-o") + 1]).write_bytes(b"mbtiles")
            return _completed(command)
        raise subprocess_error(command)

    def subprocess_error(command):
        import subprocess

        return subprocess.CalledProcessError(1, command, stderr="pmtiles: disk full")

    monkeypatch.setattr(pmtiles.subprocess, "run", fake_run)

    with pytest.raises(Exception):
        pmtiles.generate_vector_tiles(source, output)

    assert output.read_bytes() == b"previous-good-tileset"
    assert list(tmp_path.glob(".*partial*")) == []


def test_atomic_output_replaces_only_after_the_write_succeeds(tmp_path: Path) -> None:
    target = tmp_path / "tiles.pmtiles"
    target.write_bytes(b"previous")

    with pmtiles._atomic_output(target) as staged:
        assert staged != target
        staged.write_bytes(b"next")
    assert target.read_bytes() == b"next"

    with pytest.raises(FileNotFoundError):
        with pmtiles._atomic_output(target):
            pass
    assert target.read_bytes() == b"next"


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def test_tippecanoe_feature_dropping_is_logged(tmp_path: Path, monkeypatch, caplog) -> None:
    source = tmp_path / "subbasins.parquet"
    _subbasins("EPSG:2278").to_parquet(source)
    monkeypatch.setattr(pmtiles, "_require_cmd", lambda name: name)

    def fake_run(command, **_kwargs):
        if command[0] == "tippecanoe":
            Path(command[command.index("-o") + 1]).write_bytes(b"mbtiles")
            return _completed(command, stderr="Dropping 41 features at zoom 9\n")
        Path(command[3]).write_bytes(b"pmtiles")
        return _completed(command)

    monkeypatch.setattr(pmtiles.subprocess, "run", fake_run)

    with caplog.at_level("WARNING", logger=pmtiles.LOGGER.name):
        pmtiles.generate_vector_tiles(source, tmp_path / "out.pmtiles")

    # Silent data loss on a zero exit is the failure mode being prevented.
    assert any("Dropping 41 features" in record.message for record in caplog.records)
