"""Tests for hms2cng.project module."""
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from hms2cng.project import slugify


# ---------------------------------------------------------------------------
# Slug tests (no HMS data needed)
# ---------------------------------------------------------------------------

def test_slugify_basic():
    assert slugify("Run 1") == "run_1"


def test_slugify_special_chars():
    assert slugify("Basin - A (test)") == "basin___a__test_"


def test_slugify_already_clean():
    assert slugify("calibration") == "calibration"


def test_slugify_leading_trailing_space():
    assert slugify("  Run 2  ") == "run_2"


# ---------------------------------------------------------------------------
# Catalog unit tests (no HMS data needed)
# ---------------------------------------------------------------------------

def test_manifest_create_and_serialize():
    from hms2cng.catalog import Manifest

    m = Manifest.create(project_name="TestProject")
    m.add_layer("subbasins", rows=5, geometry_type="Point", crs="EPSG:4326")
    m.add_layer("reaches", rows=3, geometry_type="LineString", crs="EPSG:4326")

    d = m.to_dict()
    assert d["schema_version"] == "2.0"
    assert d["project_name"] == "TestProject"
    assert len(d["layers"]) == 2
    assert d["layers"][0]["name"] == "subbasins"
    assert d["layers"][0]["rows"] == 5
    assert d["layers"][1]["geometry_type"] == "LineString"


def test_manifest_json_roundtrip():
    from hms2cng.catalog import Manifest

    m = Manifest.create(
        project_name="RoundTrip",
        hms_version="4.12",
        parquet_file="roundtrip.parquet",
        total_rows=100,
        basin_models=["Basin1"],
        run_names=["Run1", "Run2"],
    )
    m.add_layer("subbasins", rows=50, geometry_type="Point")
    m.add_layer("outflow", rows=50)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "manifest.json"
        m.write(path)

        loaded = Manifest.load(path)
        assert loaded.project_name == "RoundTrip"
        assert loaded.schema_version == "2.0"
        assert loaded.total_rows == 100
        assert len(loaded.layers) == 2
        assert loaded.layers[0].name == "subbasins"
        assert loaded.layers[0].rows == 50
        assert loaded.layers[1].name == "outflow"
        assert loaded.layers[1].geometry_type is None
        assert loaded.basin_models == ["Basin1"]
        assert loaded.run_names == ["Run1", "Run2"]


# ---------------------------------------------------------------------------
# Integration: requires HMS example projects on disk
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_export_project_manifest_river_bend():
    """Export manifest/registry/inventory for river_bend example project."""
    from hms_commander import HmsExamples
    from hms2cng.project import export_project_manifest

    project_path = HmsExamples.extract_project("river_bend")
    hms_file = next(project_path.glob("*.hms"))

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir)
        mp, rp, bp = export_project_manifest(hms_file, out)

        # manifest.parquet
        assert mp.is_file()
        manifest = pd.read_parquet(mp)
        assert len(manifest) == 1
        assert "project_name" in manifest.columns
        assert "num_runs" in manifest.columns
        assert "run_names" in manifest.columns
        run_names = json.loads(manifest.iloc[0]["run_names"])
        assert isinstance(run_names, list)
        assert len(run_names) > 0

        # run_registry.parquet
        assert rp.is_file()
        registry = pd.read_parquet(rp)
        assert len(registry) > 0
        assert "run_name" in registry.columns
        assert "basin_model" in registry.columns
        assert "met_model" in registry.columns
        assert "project_name" in registry.columns

        # basin_inventory.parquet
        assert bp.is_file()
        inventory = pd.read_parquet(bp)
        assert len(inventory) > 0
        assert "basin_model" in inventory.columns
        assert "project_name" in inventory.columns
        assert "num_subbasins" in inventory.columns


@pytest.mark.integration
def test_export_full_project_river_bend():
    """Full project export produces consolidated parquet + manifest.json."""
    from hms_commander import HmsExamples
    from hms2cng.project import export_full_project

    project_path = HmsExamples.extract_project("river_bend")
    hms_file = next(project_path.glob("*.hms"))

    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir)
        summary = export_full_project(hms_file, out)

        # Single consolidated parquet
        assert summary["parquet_file"] is not None
        assert summary["parquet_file"].is_file()
        assert summary["parquet_file"].suffix == ".parquet"

        # manifest.json
        assert summary["manifest_json"] is not None
        assert summary["manifest_json"].is_file()

        # No nested subdirectories (flat output)
        subdirs = [p for p in out.iterdir() if p.is_dir()]
        assert len(subdirs) == 0

        # Verify parquet content
        import geopandas as gpd
        gdf = gpd.read_parquet(summary["parquet_file"])
        assert "layer" in gdf.columns
        assert "project_name" in gdf.columns
        assert len(gdf) > 0

        # Should have at least some geometry layers
        layers = set(gdf["layer"].unique())
        assert "subbasins" in layers

        # Verify bbox columns
        for col in ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"):
            assert col in gdf.columns

        # Row counts
        assert summary["geometry_rows"] > 0

        # Verify manifest.json content
        from hms2cng.catalog import Manifest
        manifest = Manifest.load(summary["manifest_json"])
        assert manifest.project_name is not None
        assert manifest.schema_version == "2.0"
        assert len(manifest.layers) > 0
        assert manifest.total_rows == len(gdf)


@pytest.mark.integration
def test_get_project_manifest_river_bend():
    """get_project_manifest returns expected keys."""
    from hms_commander import HmsExamples
    from hms2cng.project import get_project_manifest

    project_path = HmsExamples.extract_project("river_bend")
    hms_file = next(project_path.glob("*.hms"))

    manifest = get_project_manifest(hms_file)

    required_keys = {
        "project_name", "project_file", "num_basin_models",
        "num_met_models", "num_runs", "basin_models", "run_names",
        "export_timestamp",
    }
    for key in required_keys:
        assert key in manifest, f"Missing key: {key}"

    assert manifest["num_runs"] > 0
    run_names = json.loads(manifest["run_names"])
    assert len(run_names) == manifest["num_runs"]
