"""hms2cng results utilities.

The scaffold initially assumed a stateful `HmsResults` object + a `get_time_series()` API.
In hms-commander v0.2+, results access is largely *static* and DSS reading requires
`pyjnius`, which is currently difficult to install on Python 3.13 on Windows.

To keep the CLI useful on Windows out-of-the-box, this module supports parsing
HEC-HMS `RUN_*.results` XML files to extract *summary statistics* (peak/min/avg,
plus times), and then merges those statistics with basin geometry extracted from
`.basin` (Canvas X/Y).

If DSS support becomes available (pyjnius installed), we can extend this module
with full time-series extraction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Literal
import re
import xml.etree.ElementTree as ET

import geopandas as gpd
import pandas as pd

from hms2cng.geometry import get_basin_layer_gdf


ElementType = Literal["subbasin", "reach", "junction", "diversion", "reservoir", "source", "sink", "all"]


def _ensure_parent(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def _find_project_dir(start: Path) -> Optional[Path]:
    """Walk upward looking for a folder containing a .hms file."""
    start = Path(start)
    if start.is_file():
        cur = start.parent
    else:
        cur = start

    for _ in range(8):
        if any(cur.glob("*.hms")):
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def _find_basin_file_from_project(project_dir: Path) -> Optional[Path]:
    basin_files = sorted(Path(project_dir).glob("*.basin"))
    return basin_files[0] if basin_files else None


def _find_results_xml(results_input: Path) -> Path:
    p = Path(results_input)

    if p.is_file() and p.suffix.lower() == ".results":
        return p

    if p.is_dir():
        # Prefer RUN_*.results (standard HMS output)
        candidates = sorted(p.glob("RUN_*.results"))
        if not candidates:
            candidates = sorted(p.glob("*.results"))
        if candidates:
            # Choose the most recently modified
            candidates = sorted(candidates, key=lambda x: x.stat().st_mtime, reverse=True)
            return candidates[0]

    raise FileNotFoundError(
        f"Could not find a HMS .results XML file from: {results_input} "
        "(pass a RUN_*.results file or the project 'results' folder)"
    )


def _normalize_variable_prefix(variable: str) -> str:
    v = (variable or "").strip().lower()

    # Common CLI phrases → HMS statistic prefixes
    if v in {"flow out", "flowout", "outflow", "flow"}:
        return "Outflow"
    if v in {"inflow"}:
        return "Inflow"
    if v in {"stage", "water surface", "wse", "elevation"}:
        return "Stage"
    if v in {"depth"}:
        return "Depth"

    # If user passes something like "Outflow" or "Outflow Maximum", keep prefix only.
    # Strip trailing statistic word if present.
    v2 = variable.strip()
    v2 = re.sub(r"\s+(Maximum|Minimum|Average|Volume|Depth|Time)\s*$", "", v2, flags=re.IGNORECASE)
    return v2


def _parse_hms_datetime(text: Optional[str]) -> Optional[pd.Timestamp]:
    if not text:
        return None

    s = text.strip()

    # Common formats seen in HMS .results
    for fmt in (
        "%d%b%Y, %H:%M",
        "%d%b%Y, %H:%M:%S",
        "%d%b%Y %H:%M",
        "%d%b%Y %H:%M:%S",
    ):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass

    # Fallback: let pandas try
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def _parse_results_xml_statistics(
    results_xml: Path,
    *,
    element_type: ElementType = "subbasin",
    variable: str = "Flow Out",
) -> pd.DataFrame:
    results_xml = Path(results_xml)

    prefix = _normalize_variable_prefix(variable)

    tree = ET.parse(results_xml)
    root = tree.getroot()

    run_name = root.findtext("RunName")

    records: list[dict] = []
    for be in root.findall(".//BasinElement"):
        name = be.attrib.get("name")
        etype = (be.attrib.get("type") or "").strip().lower()

        # Normalize element type string
        if etype in {"subbasin", "subbasins"}:
            etype = "subbasin"
        elif etype in {"reach", "reaches"}:
            etype = "reach"
        elif etype in {"junction", "junctions"}:
            etype = "junction"
        elif etype in {"diversion", "diversions"}:
            etype = "diversion"
        elif etype in {"reservoir", "reservoirs"}:
            etype = "reservoir"
        elif etype in {"source", "sources"}:
            etype = "source"
        elif etype in {"sink", "sinks"}:
            etype = "sink"

        if element_type != "all" and etype != element_type:
            continue

        measures = {}
        units = {}
        for m in be.findall(".//StatisticMeasure"):
            mtype = m.attrib.get("type")
            if not mtype:
                continue
            measures[mtype] = m.attrib.get("value")
            units[mtype] = m.attrib.get("units")

        # Pull statistics for the variable prefix
        max_key = f"{prefix} Maximum"
        max_time_key = f"{prefix} Maximum Time"
        min_key = f"{prefix} Minimum"
        min_time_key = f"{prefix} Minimum Time"
        avg_key = f"{prefix} Average"
        vol_key = f"{prefix} Volume"
        depth_key = f"{prefix} Depth"

        rec = {
            "name": name,
            "element_type": etype,
            "run_name": run_name,
            "variable_prefix": prefix,
            "max_value": pd.to_numeric(measures.get(max_key), errors="coerce"),
            "time_of_max": _parse_hms_datetime(measures.get(max_time_key)),
            "min_value": pd.to_numeric(measures.get(min_key), errors="coerce"),
            "time_of_min": _parse_hms_datetime(measures.get(min_time_key)),
            "mean_value": pd.to_numeric(measures.get(avg_key), errors="coerce"),
            "volume": pd.to_numeric(measures.get(vol_key), errors="coerce"),
            "depth": pd.to_numeric(measures.get(depth_key), errors="coerce"),
            "units": units.get(max_key) or units.get(avg_key) or units.get(min_key),
        }

        records.append(rec)

    df = pd.DataFrame.from_records(records)
    if df.empty:
        raise ValueError(
            f"No statistics found in {results_xml} for element_type={element_type!r}, variable={variable!r}. "
            f"(prefix resolved to {prefix!r})"
        )

    return df


def export_hms_results(
    results_input: Path,
    output: Path,
    *,
    element_type: str = "subbasin",
    variable: str = "Flow Out",
    crs_epsg: Optional[str] = None,
    out_crs: Optional[str] = "EPSG:4326",
) -> None:
    """Export HMS results summary statistics to GeoParquet.

    - If given a folder, we look for RUN_*.results XML and parse it.
    - Geometry is inferred from the parent HMS project (find *.hms and *.basin).

    This currently exports *summary* statistics (peak/min/avg). Full time-series
    export can be added once DSS reading dependencies are standardized.
    """

    _ensure_parent(Path(output))

    et: ElementType
    et_lower = (element_type or "subbasin").strip().lower()
    valid_types = {"subbasin", "reach", "junction", "diversion", "reservoir", "source", "sink", "all"}
    if et_lower not in valid_types:
        raise ValueError(f"--type must be one of: {', '.join(sorted(valid_types))}")
    et = et_lower  # type: ignore[assignment]

    results_path = Path(results_input)

    # Resolve the XML file.
    if results_path.is_dir() and results_path.name.lower() != "results":
        # user passed project dir maybe; allow .../ProjectRoot
        if (results_path / "results").is_dir():
            results_xml = _find_results_xml(results_path / "results")
        else:
            results_xml = _find_results_xml(results_path)
    else:
        results_xml = _find_results_xml(results_path)

    # Infer project + basin file for geometry.
    project_dir = _find_project_dir(results_xml)
    basin_file = _find_basin_file_from_project(project_dir) if project_dir else None

    stats_df = _parse_results_xml_statistics(results_xml, element_type=et, variable=variable)

    if basin_file is None:
        # No geometry available; write plain Parquet.
        stats_df.to_parquet(output, compression="zstd", index=False)
        return

    # Build geometry for the requested element type(s)
    if et == "all":
        layers = []
        for lyr, e in [
            ("subbasins", "subbasin"), ("junctions", "junction"), ("reaches", "reach"),
            ("diversions", "diversion"), ("reservoirs", "reservoir"),
            ("sources", "source"), ("sinks", "sink"),
        ]:
            try:
                geom = get_basin_layer_gdf(basin_file, layer=lyr, crs_epsg=crs_epsg, out_crs=out_crs)
            except Exception:
                continue
            geom["element_type"] = e
            layers.append(geom)
        if not layers:
            # fallback without geometry
            stats_df.to_parquet(output, compression="zstd", index=False)
            return
        geom_gdf = pd.concat(layers, ignore_index=True)
        geom_gdf = gpd.GeoDataFrame(geom_gdf, geometry="geometry", crs=layers[0].crs)

        merged = geom_gdf.merge(stats_df, on=["name", "element_type"], how="left")
        merged.to_parquet(output, compression="zstd")
        return

    # single element type
    layer_map = {
        "subbasin": "subbasins", "junction": "junctions", "reach": "reaches",
        "diversion": "diversions", "reservoir": "reservoirs",
        "source": "sources", "sink": "sinks",
    }
    geom_gdf = get_basin_layer_gdf(basin_file, layer=layer_map[et], crs_epsg=crs_epsg, out_crs=out_crs)

    merged = geom_gdf.merge(stats_df, on="name", how="left")
    merged.to_parquet(output, compression="zstd")


def export_peak_flow_summary(
    results_dir: Path,
    output: Path,
    *,
    crs_epsg: Optional[str] = None,
    out_crs: Optional[str] = "EPSG:4326",
) -> None:
    """Convenience wrapper: peak outflow for all element types."""

    export_hms_results(
        results_dir,
        output,
        element_type="all",
        variable="Outflow",
        crs_epsg=crs_epsg,
        out_crs=out_crs,
    )


def export_all_results(
    hms_file: Path,
    output_dir: Path,
    *,
    variables: Optional[list] = None,
    out_crs: Optional[str] = "EPSG:4326",
    skip_errors: bool = True,
) -> tuple:
    """Export results for ALL runs in an HMS project.

    Output: output_dir/{run_slug}/{variable_slug}.parquet
    Each file is enriched with: project_name, run_name, basin_model,
    met_model, control_spec, start_date, end_date, time_interval_minutes.

    Args:
        hms_file: Path to the .hms project file or project directory.
        output_dir: Root output directory (results go in per-run subdirs).
        variables: Result variables to export. Defaults to ["Outflow"].
        out_crs: Output CRS (default EPSG:4326).
        skip_errors: If True, skip runs/variables that fail; otherwise raise.

    Returns:
        Tuple of (created_paths: list[Path], errors: list[str]).
    """
    import re
    from hms2cng.project import _find_hms_file, _init_project, slugify, _find_results_xml_for_run

    hms_file = _find_hms_file(Path(hms_file))
    output_dir = Path(output_dir)
    vars_to_export = variables if variables is not None else ["Outflow"]

    prj = _init_project(hms_file)
    project_name = prj.project_name
    project_dir = hms_file.parent

    # Build control lookup: control_spec name -> row
    ctrl_lookup: dict = {}
    if not prj.control_df.empty:
        for _, cr in prj.control_df.iterrows():
            ctrl_lookup[cr.get("name", "")] = cr

    created: list = []
    errors: list = []

    for _, run_row in prj.run_df.iterrows():
        run_name = run_row.get("name", "")
        basin_model = run_row.get("basin_model", "")
        met_model = run_row.get("met_model", "")
        control_spec = run_row.get("control_spec", "")
        run_slug = slugify(run_name)

        # Locate results XML
        results_xml = _find_results_xml_for_run(project_dir, run_name)
        if results_xml is None:
            errors.append(f"No results XML found for run: {run_name!r}")
            continue

        # Control window metadata
        start_date = end_date = time_interval_minutes = None
        if control_spec in ctrl_lookup:
            cr = ctrl_lookup[control_spec]
            start_date = cr.get("start_date")
            end_date = cr.get("end_date")
            time_interval_minutes = cr.get("time_interval_minutes")

        # Find basin file for geometry
        basin_file: Optional[Path] = None
        if not prj.basin_df.empty:
            match = prj.basin_df[prj.basin_df["name"] == basin_model]
            if not match.empty:
                bp = Path(match.iloc[0].get("full_path", ""))
                if bp.is_file():
                    basin_file = bp

        run_out_dir = output_dir / run_slug
        run_out_dir.mkdir(parents=True, exist_ok=True)

        for variable in vars_to_export:
            var_slug = re.sub(r"[^\w]", "_", variable.strip()).lower()
            out_path = run_out_dir / f"{var_slug}.parquet"
            try:
                stats_df = _parse_results_xml_statistics(
                    results_xml,
                    element_type="all",
                    variable=variable,
                )
            except (ValueError, FileNotFoundError) as exc:
                errors.append(f"run={run_name!r}, var={variable!r}: {exc}")
                if not skip_errors:
                    raise
                continue
            except Exception as exc:
                errors.append(f"run={run_name!r}, var={variable!r}: {exc}")
                if not skip_errors:
                    raise
                continue

            # Enrich with lineage columns
            for col, val in [
                ("project_name", project_name),
                ("run_name", run_name),
                ("basin_model", basin_model),
                ("met_model", met_model),
                ("control_spec", control_spec),
                ("start_date", start_date),
                ("end_date", end_date),
                ("time_interval_minutes", time_interval_minutes),
            ]:
                stats_df[col] = val

            # Merge geometry if available
            if basin_file is not None:
                layer_map = {
                    "subbasin": "subbasins", "junction": "junctions", "reach": "reaches",
                    "diversion": "diversions", "reservoir": "reservoirs",
                    "source": "sources", "sink": "sinks",
                }
                geo_parts = []
                for et, lyr in layer_map.items():
                    subset = stats_df[stats_df["element_type"] == et].copy()
                    if subset.empty:
                        continue
                    try:
                        geom_gdf = get_basin_layer_gdf(
                            basin_file, layer=lyr, out_crs=out_crs  # type: ignore[arg-type]
                        )
                        merged = geom_gdf.merge(
                            subset.drop(columns=["element_type"], errors="ignore"),
                            on="name",
                            how="inner",
                        )
                        if not merged.empty:
                            merged["element_type"] = et
                            geo_parts.append(merged)
                    except Exception:
                        # Layer not available: keep stats without geometry
                        geo_parts.append(subset)

                if geo_parts:
                    import geopandas as gpd
                    # Combine: some parts may be plain DataFrames (no geometry)
                    geo_gdfs = [p for p in geo_parts if isinstance(p, gpd.GeoDataFrame)]
                    plain_dfs = [p for p in geo_parts if not isinstance(p, gpd.GeoDataFrame)]
                    if geo_gdfs:
                        combined = gpd.GeoDataFrame(
                            pd.concat(geo_gdfs + plain_dfs, ignore_index=True),
                            geometry="geometry",
                            crs=geo_gdfs[0].crs,
                        )
                        combined.to_parquet(out_path, compression="zstd")
                    else:
                        pd.concat(plain_dfs, ignore_index=True).to_parquet(
                            out_path, compression="zstd", index=False
                        )
                else:
                    stats_df.to_parquet(out_path, compression="zstd", index=False)
            else:
                stats_df.to_parquet(out_path, compression="zstd", index=False)

            created.append(out_path)

    return created, errors


_AVAILABLE_VARIABLES: list[str] = ["Outflow", "Inflow", "Stage", "Depth"]


def merge_all_variables(
    results_xml: Path,
    basin_file: Optional[Path] = None,
    *,
    variables: Optional[list[str]] = None,
    out_crs: Optional[str] = "EPSG:4326",
    project_name: Optional[str] = None,
    run_name: Optional[str] = None,
    basin_model: Optional[str] = None,
    met_model: Optional[str] = None,
    control_spec: Optional[str] = None,
    start_date=None,
    end_date=None,
    time_interval_minutes=None,
) -> Optional[gpd.GeoDataFrame]:
    """Merge all result variables for a run into a single GeoDataFrame.

    Each row gets a ``layer`` column with the snake_case variable name
    (e.g. ``outflow``, ``stage``). Variables that raise ValueError or
    FileNotFoundError are silently skipped.

    Returns None if no variables could be parsed.
    """
    vars_to_try = variables if variables is not None else _AVAILABLE_VARIABLES

    layer_map = {
        "subbasin": "subbasins", "junction": "junctions", "reach": "reaches",
        "diversion": "diversions", "reservoir": "reservoirs",
        "source": "sources", "sink": "sinks",
    }

    all_parts: list = []

    for variable in vars_to_try:
        try:
            stats_df = _parse_results_xml_statistics(
                results_xml, element_type="all", variable=variable,
            )
        except (ValueError, FileNotFoundError):
            continue

        var_slug = re.sub(r"[^\w]", "_", variable.strip()).lower()

        # Merge with geometry if basin file available
        if basin_file is not None:
            geo_chunks: list = []
            for et, lyr in layer_map.items():
                subset = stats_df[stats_df["element_type"] == et].copy()
                if subset.empty:
                    continue
                try:
                    geom_gdf = get_basin_layer_gdf(basin_file, layer=lyr, out_crs=out_crs)
                    merged = geom_gdf.merge(
                        subset.drop(columns=["element_type"], errors="ignore"),
                        on="name", how="inner",
                    )
                    if not merged.empty:
                        merged["element_type"] = et
                        geo_chunks.append(merged)
                except Exception:
                    geo_chunks.append(subset)

            if geo_chunks:
                geo_gdfs = [p for p in geo_chunks if isinstance(p, gpd.GeoDataFrame)]
                plain_dfs = [p for p in geo_chunks if not isinstance(p, gpd.GeoDataFrame)]
                if geo_gdfs:
                    combined = gpd.GeoDataFrame(
                        pd.concat(geo_gdfs + plain_dfs, ignore_index=True),
                        geometry="geometry", crs=geo_gdfs[0].crs,
                    )
                else:
                    combined = pd.concat(plain_dfs, ignore_index=True)
            else:
                combined = stats_df
        else:
            combined = stats_df

        combined = combined.copy()
        combined["layer"] = var_slug

        for col, val in [
            ("project_name", project_name),
            ("run_name", run_name),
            ("basin_model", basin_model),
            ("met_model", met_model),
            ("control_spec", control_spec),
            ("start_date", start_date),
            ("end_date", end_date),
            ("time_interval_minutes", time_interval_minutes),
        ]:
            if val is not None:
                combined[col] = val

        all_parts.append(combined)

    if not all_parts:
        return None

    result = pd.concat(all_parts, ignore_index=True)
    geo_parts = [p for p in all_parts if isinstance(p, gpd.GeoDataFrame)]
    if geo_parts:
        result = gpd.GeoDataFrame(result, geometry="geometry", crs=geo_parts[0].crs)
    return result
