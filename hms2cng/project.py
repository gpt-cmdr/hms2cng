"""hms2cng project-level access and archival.

Transforms hms2cng from a single-layer export tool into a full project
access and archival system.

``export_full_project()`` produces a **single consolidated GeoParquet** with
a ``layer`` discriminator column plus a ``manifest.json`` catalog::

    output_dir/
    ├── {project_slug}.parquet    # ALL geometry + results
    └── manifest.json             # JSON catalog (schema v2.0)

``export_project_manifest()`` writes three separate registry parquets::

    output_dir/
    ├── manifest.parquet          # 1 row: project-level metadata
    ├── run_registry.parquet      # 1 row per run
    └── basin_inventory.parquet   # 1 row per basin model

All output parquet files include 'project_name' and (where applicable)
'basin_model' columns for cross-project DuckDB glob queries.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def slugify(name: str) -> str:
    """Convert a run/basin name to a filesystem-safe slug.

    Replaces any non-word character with an underscore and lowercases.

    Args:
        name: The name to slugify (e.g. "Run 1 - Calibration").

    Returns:
        A lowercase slug, e.g. "run_1___calibration".
    """
    return re.sub(r"[^\w]", "_", name.strip()).lower()


def _find_hms_file(hms_input: Path) -> Path:
    """Return the .hms project file, accepting either the file or a directory."""
    p = Path(hms_input)
    if p.is_file() and p.suffix.lower() == ".hms":
        return p
    if p.is_dir():
        candidates = sorted(p.glob("*.hms"))
        if candidates:
            return candidates[0]
    raise FileNotFoundError(
        f"Could not locate a .hms file from input: {hms_input}"
    )


def _init_project(hms_file: Path):
    """Initialize a fresh HmsPrj instance for the given project file.

    Always creates a new instance (avoids polluting the global singleton
    when batch-processing many projects).
    """
    from hms_commander import HmsPrj, init_hms_project

    prj = HmsPrj()
    init_hms_project(hms_file.parent, hms_object=prj)
    return prj


def _find_results_xml_for_run(project_dir: Path, run_name: str) -> Optional[Path]:
    """Find the RUN_*.results XML file for a given run name."""
    results_dir = project_dir / "results"

    # Try exact match first
    exact = results_dir / f"RUN_{run_name}.results"
    if exact.is_file():
        return exact

    # Glob for pattern (handles spaces in run name via filesystem listing)
    if results_dir.is_dir():
        for candidate in results_dir.glob("RUN_*.results"):
            # Strip "RUN_" prefix and ".results" suffix to get the run name
            stem = candidate.stem  # e.g. "RUN_Calibration Run 1"
            candidate_run = stem[4:] if stem.startswith("RUN_") else stem
            if candidate_run == run_name:
                return candidate

    return None


def _write_geoparquet(gdf, output_path: Path) -> None:
    """Write a GeoDataFrame to GeoParquet with bbox columns and ZSTD compression.

    Adds per-row bbox columns (bbox_xmin/ymin/xmax/ymax) and patches the
    GeoParquet metadata with a ``covering`` spec for spatial predicate pushdown.
    """
    import geopandas as gpd

    gdf = gdf.copy()
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Add per-row bbox columns (NaN for null geometries)
    bounds = gdf.geometry.bounds
    gdf["bbox_xmin"] = bounds["minx"]
    gdf["bbox_ymin"] = bounds["miny"]
    gdf["bbox_xmax"] = bounds["maxx"]
    gdf["bbox_ymax"] = bounds["maxy"]

    gdf.to_parquet(output_path, compression="zstd")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_project_manifest(hms_file: Path) -> dict:
    """Return a dict with project-level metadata (suitable for a 1-row DataFrame).

    Args:
        hms_file: Path to the .hms project file or project directory.

    Returns:
        Dict with keys: project_name, project_file, hms_version, crs_epsg,
        is_gridded, num_basin_models, num_met_models, num_control_specs,
        num_runs, basin_models, met_models, run_names, export_timestamp.
        The list fields (basin_models, met_models, run_names) are JSON strings.
    """
    hms_file = _find_hms_file(Path(hms_file))
    prj = _init_project(hms_file)

    basin_names = list(prj.basin_df["name"]) if not prj.basin_df.empty else []
    met_names = list(prj.met_df["name"]) if not prj.met_df.empty else []
    run_names = list(prj.run_df["name"]) if not prj.run_df.empty else []

    return {
        "project_name": prj.project_name,
        "project_file": str(hms_file),
        "hms_version": getattr(prj, "hms_version", None),
        "crs_epsg": getattr(prj, "crs_epsg", None),
        "is_gridded": getattr(prj, "is_gridded", False),
        "num_basin_models": len(basin_names),
        "num_met_models": len(met_names),
        "num_control_specs": len(prj.control_df) if not prj.control_df.empty else 0,
        "num_runs": len(run_names),
        "basin_models": json.dumps(basin_names),
        "met_models": json.dumps(met_names),
        "run_names": json.dumps(run_names),
        "export_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def export_project_manifest(hms_file: Path, output_dir: Path) -> tuple[Path, Path, Path]:
    """Write manifest.parquet, run_registry.parquet, and basin_inventory.parquet.

    Args:
        hms_file: Path to the .hms project file or project directory.
        output_dir: Directory to write the three parquet files.

    Returns:
        Tuple of (manifest_path, run_registry_path, basin_inventory_path).
    """
    hms_file = _find_hms_file(Path(hms_file))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prj = _init_project(hms_file)
    project_name = prj.project_name
    project_dir = hms_file.parent

    # --- manifest.parquet ---
    manifest_data = get_project_manifest(hms_file)
    manifest_df = pd.DataFrame([manifest_data])
    manifest_path = output_dir / "manifest.parquet"
    manifest_df.to_parquet(manifest_path, compression="zstd", index=False)

    # --- run_registry.parquet ---
    run_records = []
    for _, run_row in prj.run_df.iterrows():
        run_name = run_row.get("name", "")
        basin_model = run_row.get("basin_model", "")
        met_model = run_row.get("met_model", "")
        control_spec = run_row.get("control_spec", "")

        # Look up control window from control_df
        start_date = end_date = time_interval_minutes = duration_hours = None
        if not prj.control_df.empty and control_spec:
            ctrl = prj.control_df[prj.control_df["name"] == control_spec]
            if not ctrl.empty:
                row = ctrl.iloc[0]
                start_date = row.get("start_date")
                end_date = row.get("end_date")
                time_interval_minutes = row.get("time_interval_minutes")
                duration_hours = row.get("duration_hours")

        # Locate results XML
        results_file = _find_results_xml_for_run(project_dir, run_name)

        run_records.append({
            "project_name": project_name,
            "run_name": run_name,
            "run_slug": slugify(run_name),
            "basin_model": basin_model,
            "met_model": met_model,
            "control_spec": control_spec,
            "start_date": start_date,
            "end_date": end_date,
            "time_interval_minutes": time_interval_minutes,
            "duration_hours": duration_hours,
            "last_execution_date": run_row.get("last_execution_date"),
            "last_execution_time": run_row.get("last_execution_time"),
            "results_file": str(results_file) if results_file else None,
            "has_results": results_file is not None,
        })

    run_registry_df = pd.DataFrame(run_records)
    run_registry_path = output_dir / "run_registry.parquet"
    run_registry_df.to_parquet(run_registry_path, compression="zstd", index=False)

    # --- basin_inventory.parquet ---
    basin_records = []
    for _, basin_row in prj.basin_df.iterrows():
        basin_name = basin_row.get("name", "")
        basin_records.append({
            "project_name": project_name,
            "basin_model": basin_name,
            "basin_slug": slugify(basin_name),
            "basin_file": str(basin_row.get("full_path", "")),
            "description": basin_row.get("description"),
            "num_subbasins": basin_row.get("num_subbasins"),
            "num_reaches": basin_row.get("num_reaches"),
            "num_junctions": basin_row.get("num_junctions"),
            "num_reservoirs": basin_row.get("num_reservoirs"),
            "total_area": basin_row.get("total_area"),
            "loss_methods": json.dumps(list(basin_row["loss_methods"]))
                if isinstance(basin_row.get("loss_methods"), (list, set)) else None,
            "transform_methods": json.dumps(list(basin_row["transform_methods"]))
                if isinstance(basin_row.get("transform_methods"), (list, set)) else None,
            "routing_methods": json.dumps(list(basin_row["routing_methods"]))
                if isinstance(basin_row.get("routing_methods"), (list, set)) else None,
        })

    basin_inventory_df = pd.DataFrame(basin_records)
    basin_inventory_path = output_dir / "basin_inventory.parquet"
    basin_inventory_df.to_parquet(basin_inventory_path, compression="zstd", index=False)

    return manifest_path, run_registry_path, basin_inventory_path


def export_full_project(
    hms_file: Path,
    output_dir: Path,
    *,
    layers: Optional[list] = None,
    variables: Optional[list] = None,
    out_crs: str = "EPSG:4326",
    skip_errors: bool = True,
    sort: bool = True,
) -> dict:
    """Export an entire HMS project to a single consolidated GeoParquet.

    Produces:
      output_dir/
        {project_slug}.parquet   -- all geometry + results with ``layer`` discriminator
        manifest.json            -- JSON catalog (schema v2.0)

    The ``layer`` column discriminates between geometry layers (subbasins,
    reaches, junctions, ...) and result variables (outflow, stage, ...).
    Query examples::

        SELECT * FROM 'project.parquet' WHERE layer = 'subbasins'
        SELECT * FROM 'project.parquet' WHERE layer = 'outflow' AND run_name = 'Run 1'

    Args:
        hms_file: Path to the .hms project file or project directory.
        output_dir: Root output directory for the archive.
        layers: Geometry layers to export (default: all available).
        variables: Result variables to export (default: Outflow, Inflow, Stage, Depth).
        out_crs: Output CRS (default EPSG:4326).
        skip_errors: If True, skip layers/runs with errors and continue.
        sort: If True, apply Hilbert spatial sorting within each layer.

    Returns:
        Summary dict with keys: parquet_file, manifest_json,
        geometry_rows, results_rows, errors.
    """
    import geopandas as gpd
    from hms2cng.geometry import merge_all_layers
    from hms2cng.results import merge_all_variables
    from hms2cng.catalog import Manifest

    hms_file = _find_hms_file(Path(hms_file))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prj = _init_project(hms_file)
    project_name = prj.project_name
    project_dir = hms_file.parent

    errors: list[str] = []
    all_parts: list = []

    # Build control lookup
    ctrl_lookup: dict = {}
    if not prj.control_df.empty:
        for _, cr in prj.control_df.iterrows():
            ctrl_lookup[cr.get("name", "")] = cr

    # --- Geometry for each basin model ---
    geometry_rows = 0
    for _, basin_row in prj.basin_df.iterrows():
        basin_name = basin_row.get("name", "")
        basin_file_path = Path(basin_row.get("full_path", ""))
        if not basin_file_path.is_file():
            errors.append(f"Basin file not found: {basin_file_path}")
            if not skip_errors:
                raise FileNotFoundError(f"Basin file not found: {basin_file_path}")
            continue

        try:
            merged = merge_all_layers(
                basin_file_path,
                layers=layers,
                out_crs=out_crs,
                project_name=project_name,
                basin_model=basin_name,
                sort=sort,
            )
        except Exception as exc:
            errors.append(f"basin={basin_name!r}: {exc}")
            if not skip_errors:
                raise
            continue

        if merged is not None:
            geometry_rows += len(merged)
            all_parts.append(merged)

    # --- Results for each run ---
    results_rows = 0
    for _, run_row in prj.run_df.iterrows():
        run_name = run_row.get("name", "")
        basin_model = run_row.get("basin_model", "")
        met_model = run_row.get("met_model", "")
        control_spec = run_row.get("control_spec", "")

        results_xml = _find_results_xml_for_run(project_dir, run_name)
        if results_xml is None:
            errors.append(f"No results XML for run: {run_name!r}")
            continue

        # Find basin file for geometry merge
        basin_file: Optional[Path] = None
        if not prj.basin_df.empty:
            match = prj.basin_df[prj.basin_df["name"] == basin_model]
            if not match.empty:
                bp = Path(match.iloc[0].get("full_path", ""))
                if bp.is_file():
                    basin_file = bp

        start_date = end_date = time_interval_minutes = None
        if control_spec in ctrl_lookup:
            cr = ctrl_lookup[control_spec]
            start_date = cr.get("start_date")
            end_date = cr.get("end_date")
            time_interval_minutes = cr.get("time_interval_minutes")

        try:
            merged = merge_all_variables(
                results_xml,
                basin_file,
                variables=variables,
                out_crs=out_crs,
                project_name=project_name,
                run_name=run_name,
                basin_model=basin_model,
                met_model=met_model,
                control_spec=control_spec,
                start_date=start_date,
                end_date=end_date,
                time_interval_minutes=time_interval_minutes,
            )
        except Exception as exc:
            errors.append(f"run={run_name!r}: {exc}")
            if not skip_errors:
                raise
            continue

        if merged is not None:
            results_rows += len(merged)
            all_parts.append(merged)

    # --- Consolidate into one GeoParquet ---
    if not all_parts:
        return {
            "parquet_file": None,
            "manifest_json": None,
            "geometry_rows": 0,
            "results_rows": 0,
            "errors": errors,
        }

    all_frames = pd.concat(all_parts, ignore_index=True)
    parquet_path = output_dir / f"{slugify(project_name)}.parquet"

    if "geometry" in all_frames.columns:
        combined = gpd.GeoDataFrame(all_frames, geometry="geometry")
        for part in all_parts:
            if isinstance(part, gpd.GeoDataFrame) and part.crs is not None:
                combined = combined.set_crs(part.crs, allow_override=True)
                break
        _write_geoparquet(combined, parquet_path)
    else:
        all_frames.to_parquet(parquet_path, compression="zstd", index=False)

    # --- Write manifest.json ---
    manifest = Manifest.create(
        project_name=project_name,
        hms_version=getattr(prj, "hms_version", None),
        crs_epsg=getattr(prj, "crs_epsg", None),
        parquet_file=parquet_path.name,
        total_rows=len(all_frames),
        basin_models=list(prj.basin_df["name"]) if not prj.basin_df.empty else [],
        run_names=list(prj.run_df["name"]) if not prj.run_df.empty else [],
        errors=errors,
    )

    if "geometry" in all_frames.columns and "layer" in all_frames.columns:
        crs_str = str(combined.crs) if combined.crs else None
        for layer_name in all_frames["layer"].unique():
            subset = all_frames[all_frames["layer"] == layer_name]
            geom_type = None
            geom_col = subset.get("geometry")
            if geom_col is not None and not geom_col.isna().all():
                geom_types = gpd.GeoSeries(geom_col.dropna()).geom_type.unique()
                geom_type = geom_types[0] if len(geom_types) == 1 else "Mixed"
            manifest.add_layer(
                name=layer_name,
                rows=len(subset),
                geometry_type=geom_type,
                crs=crs_str,
            )

    manifest_path = output_dir / "manifest.json"
    manifest.write(manifest_path)

    return {
        "parquet_file": parquet_path,
        "manifest_json": manifest_path,
        "geometry_rows": geometry_rows,
        "results_rows": results_rows,
        "errors": errors,
    }
