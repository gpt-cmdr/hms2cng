"""Integration tests: export geometry & results from real HMS projects.

Run with:
    pytest tests/test_integration.py -v --tb=short

Requires:
    - HmsExamples at C:/GH/hms-commander/examples/hms_example_projects (+ hms_example_projects/)
    - HCFCD M3 models at C:/GH/hms-commander/workspace/m3_hms_projects
    - LWI Region 4 at L:/Region_4
    - Output directory I:/hmscmdr-parquet-testing (232 GB SSD)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from hms2cng.geometry import export_basin_geometry
from hms2cng.results import export_hms_results

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OUTPUT_ROOT = Path("I:/hmscmdr-parquet-testing")

SOURCES: dict[str, list[Path]] = {
    "hms_examples": [
        Path("C:/GH/hms-commander/examples/hms_example_projects"),
        Path("C:/GH/hms-commander/hms_example_projects"),
    ],
    "m3": [
        Path("C:/GH/hms-commander/workspace/m3_hms_projects"),
    ],
    "lwi_region4": [
        Path("L:/Region_4"),
    ],
}

GEOMETRY_LAYERS = ["subbasins", "junctions", "reaches", "diversions", "reservoirs", "sources", "sinks"]

# SQLite-based layers (only for projects with .sqlite files)
SQLITE_LAYERS = ["subbasin_polygons", "longest_flowpaths", "centroidal_flowpaths",
                 "teneightyfive_flowpaths", "subbasin_statistics"]


# ---------------------------------------------------------------------------
# Project discovery
# ---------------------------------------------------------------------------

def _sanitize(name: str) -> str:
    """Make a string safe for use as a directory/file name."""
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip().rstrip('.')


def discover_projects(source_name: str, roots: list[Path]) -> list[dict[str, Any]]:
    """Find all HMS project directories under *roots*.

    Returns a list of dicts with keys:
        source, project_id, project_dir, basin_files, results_files
    """
    projects: list[dict[str, Any]] = []
    seen_dirs: set[Path] = set()

    for root in roots:
        if not root.exists():
            continue
        for hms_file in sorted(root.rglob("*.hms")):
            proj_dir = hms_file.parent
            if proj_dir in seen_dirs:
                continue
            seen_dirs.add(proj_dir)

            # Build a human-readable project ID from the relative path
            try:
                rel = proj_dir.relative_to(root)
            except ValueError:
                rel = Path(proj_dir.name)
            project_id = _sanitize(str(rel).replace("\\", "/"))

            basin_files = sorted(proj_dir.glob("*.basin"))

            # Collect results: look in results/ subdirectory first, then project dir
            results_dir = proj_dir / "results"
            if results_dir.is_dir():
                results_files = sorted(results_dir.glob("RUN_*.results"))
            else:
                results_files = sorted(proj_dir.glob("RUN_*.results"))
            # Also grab any *.results if no RUN_ files found
            if not results_files:
                if results_dir.is_dir():
                    results_files = sorted(results_dir.glob("*.results"))
                else:
                    results_files = sorted(proj_dir.glob("*.results"))

            projects.append({
                "source": source_name,
                "project_id": project_id,
                "project_dir": proj_dir,
                "basin_files": basin_files,
                "results_files": results_files,
            })

    return projects


def _all_projects() -> list[dict[str, Any]]:
    """Discover all projects across all sources."""
    all_projs = []
    for source_name, roots in SOURCES.items():
        all_projs.extend(discover_projects(source_name, roots))
    return all_projs


# Build project list at module level for parametrization
ALL_PROJECTS = _all_projects()

# Build (project, basin_file) pairs for geometry tests
GEOMETRY_PARAMS = []
for proj in ALL_PROJECTS:
    for basin in proj["basin_files"]:
        label = f"{proj['source']}/{proj['project_id']}/{basin.stem}"
        GEOMETRY_PARAMS.append(
            pytest.param(proj, basin, id=_sanitize(label))
        )

# Build (project, basin_file) pairs for sqlite-based geometry tests
# Only include basins that have a matching .sqlite file
SQLITE_PARAMS = []
for proj in ALL_PROJECTS:
    for basin in proj["basin_files"]:
        sqlite_match = basin.parent / f"{basin.stem}.sqlite"
        if sqlite_match.is_file():
            label = f"{proj['source']}/{proj['project_id']}/{basin.stem}"
            SQLITE_PARAMS.append(
                pytest.param(proj, basin, id=_sanitize(label))
            )

# Build (project, results_file) pairs for results tests
RESULTS_PARAMS = []
for proj in ALL_PROJECTS:
    for res in proj["results_files"]:
        label = f"{proj['source']}/{proj['project_id']}/{res.stem}"
        RESULTS_PARAMS.append(
            pytest.param(proj, res, id=_sanitize(label))
        )


def _output_dir(source: str, project_id: str) -> Path:
    return OUTPUT_ROOT / source / project_id


def _validate_parquet(path: Path) -> tuple[int, int]:
    """Read a parquet file and return (row_count, file_size_bytes).

    Tries geopandas first (for GeoParquet), falls back to pandas.
    """
    size = path.stat().st_size
    try:
        import geopandas as gpd
        df = gpd.read_parquet(path)
    except Exception:
        df = pd.read_parquet(path)
    return len(df), size


# ---------------------------------------------------------------------------
# Geometry export tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestGeometryExport:
    """Export subbasins/junctions/reaches for every discovered basin file."""

    @pytest.mark.parametrize("proj,basin_file", GEOMETRY_PARAMS)
    @pytest.mark.parametrize("layer", GEOMETRY_LAYERS)
    def test_export_layer(self, proj, basin_file, layer, integration_report):
        source = proj["source"]
        project_id = proj["project_id"]
        out_dir = _output_dir(source, project_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{basin_file.stem}_{layer}.parquet"
        operation = f"geometry:{layer}:{basin_file.stem}"

        try:
            export_basin_geometry(
                basin_file, out_file, layer=layer,
            )
        except (ValueError, FileNotFoundError) as exc:
            # "No reaches found in basin" / "No junctions found" / "No .map file"
            # are expected for models that lack those element types.
            msg = str(exc)
            if "No " in msg and "found" in msg:
                integration_report.add(
                    source=source, project=project_id,
                    operation=operation, output_path=str(out_file),
                    status="skip", error=msg,
                )
                pytest.skip(f"{operation}: {msg}")
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=msg,
            )
            pytest.fail(f"{operation} failed: {exc}")
        except Exception as exc:
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=str(exc),
            )
            pytest.fail(f"{operation} failed: {exc}")

        assert out_file.exists(), f"Output not created: {out_file}"
        rows, size = _validate_parquet(out_file)
        assert rows > 0, f"Empty parquet: {out_file}"

        integration_report.add(
            source=source, project=project_id,
            operation=operation, output_path=str(out_file),
            status="ok", rows=rows, file_size=size,
        )


# ---------------------------------------------------------------------------
# SQLite geometry export tests (subbasin polygons, flowpaths, statistics)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestSqliteGeometryExport:
    """Export sqlite-based layers for basin files that have .sqlite counterparts."""

    @pytest.mark.parametrize("proj,basin_file", SQLITE_PARAMS)
    @pytest.mark.parametrize("layer", SQLITE_LAYERS)
    def test_export_sqlite_layer(self, proj, basin_file, layer, integration_report):
        source = proj["source"]
        project_id = proj["project_id"]
        out_dir = _output_dir(source, project_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{basin_file.stem}_{layer}.parquet"
        operation = f"sqlite:{layer}:{basin_file.stem}"

        try:
            export_basin_geometry(
                basin_file, out_file, layer=layer,
            )
        except (ValueError, FileNotFoundError) as exc:
            msg = str(exc)
            if "No " in msg and "found" in msg:
                integration_report.add(
                    source=source, project=project_id,
                    operation=operation, output_path=str(out_file),
                    status="skip", error=msg,
                )
                pytest.skip(f"{operation}: {msg}")
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=msg,
            )
            pytest.fail(f"{operation} failed: {exc}")
        except Exception as exc:
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=str(exc),
            )
            pytest.fail(f"{operation} failed: {exc}")

        assert out_file.exists(), f"Output not created: {out_file}"
        rows, size = _validate_parquet(out_file)
        assert rows > 0, f"Empty parquet: {out_file}"

        integration_report.add(
            source=source, project=project_id,
            operation=operation, output_path=str(out_file),
            status="ok", rows=rows, file_size=size,
        )


# ---------------------------------------------------------------------------
# Results export tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestResultsExport:
    """Export results for every discovered RUN_*.results file."""

    @pytest.mark.parametrize("proj,results_file", RESULTS_PARAMS)
    def test_export_subbasin_outflow(self, proj, results_file, integration_report):
        source = proj["source"]
        project_id = proj["project_id"]
        out_dir = _output_dir(source, project_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{results_file.stem}_subbasin_outflow.parquet"
        operation = f"results:subbasin:Outflow:{results_file.stem}"

        try:
            export_hms_results(
                results_file, out_file,
                element_type="subbasin", variable="Outflow",
            )
        except ValueError as exc:
            msg = str(exc)
            if "No statistics found" in msg:
                integration_report.add(
                    source=source, project=project_id,
                    operation=operation, output_path=str(out_file),
                    status="skip", error=msg,
                )
                pytest.skip(f"{operation}: {msg}")
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=msg,
            )
            pytest.fail(f"{operation} failed: {exc}")
        except Exception as exc:
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=str(exc),
            )
            pytest.fail(f"{operation} failed: {exc}")

        assert out_file.exists(), f"Output not created: {out_file}"
        rows, size = _validate_parquet(out_file)
        assert rows > 0, f"Empty parquet: {out_file}"

        integration_report.add(
            source=source, project=project_id,
            operation=operation, output_path=str(out_file),
            status="ok", rows=rows, file_size=size,
        )

    @pytest.mark.parametrize("proj,results_file", RESULTS_PARAMS)
    def test_export_all_outflow(self, proj, results_file, integration_report):
        source = proj["source"]
        project_id = proj["project_id"]
        out_dir = _output_dir(source, project_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{results_file.stem}_all_outflow.parquet"
        operation = f"results:all:Outflow:{results_file.stem}"

        try:
            export_hms_results(
                results_file, out_file,
                element_type="all", variable="Outflow",
            )
        except ValueError as exc:
            msg = str(exc)
            if "No statistics found" in msg:
                integration_report.add(
                    source=source, project=project_id,
                    operation=operation, output_path=str(out_file),
                    status="skip", error=msg,
                )
                pytest.skip(f"{operation}: {msg}")
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=msg,
            )
            pytest.fail(f"{operation} failed: {exc}")
        except Exception as exc:
            integration_report.add(
                source=source, project=project_id,
                operation=operation, output_path=str(out_file),
                status="fail", error=str(exc),
            )
            pytest.fail(f"{operation} failed: {exc}")

        assert out_file.exists(), f"Output not created: {out_file}"
        rows, size = _validate_parquet(out_file)
        assert rows > 0, f"Empty parquet: {out_file}"

        integration_report.add(
            source=source, project=project_id,
            operation=operation, output_path=str(out_file),
            status="ok", rows=rows, file_size=size,
        )


# ---------------------------------------------------------------------------
# Summary report (printed at end of session)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def _print_summary(integration_report):
    yield
    summary = integration_report.summary()
    print("\n" + "=" * 70)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 70)
    print(f"  Total operations: {summary['total']}")
    print(f"  OK:               {summary['ok']}")
    print(f"  Failed:           {summary['fail']}")
    print(f"  Skipped:          {summary['skip']}")
    if summary['fail'] > 0:
        print("\n  FAILURES:")
        for r in integration_report.records:
            if r["status"] == "fail":
                print(f"    - [{r['source']}] {r['project']}: {r['operation']}")
                print(f"      Error: {r['error']}")
    print("=" * 70)
    print(f"  Report: {integration_report.REPORT_PATH if hasattr(integration_report, 'REPORT_PATH') else 'I:/hmscmdr-parquet-testing/report.json'}")
    print("=" * 70)
