"""hmscmdr-parquet results utilities.

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

from hmscmdr_parquet.geometry import get_basin_layer_gdf


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
        stats_df.to_parquet(output, compression="snappy", index=False)
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
            stats_df.to_parquet(output, compression="snappy", index=False)
            return
        geom_gdf = pd.concat(layers, ignore_index=True)
        geom_gdf = gpd.GeoDataFrame(geom_gdf, geometry="geometry", crs=layers[0].crs)

        merged = geom_gdf.merge(stats_df, on=["name", "element_type"], how="left")
        merged.to_parquet(output, compression="snappy")
        return

    # single element type
    layer_map = {
        "subbasin": "subbasins", "junction": "junctions", "reach": "reaches",
        "diversion": "diversions", "reservoir": "reservoirs",
        "source": "sources", "sink": "sinks",
    }
    geom_gdf = get_basin_layer_gdf(basin_file, layer=layer_map[et], crs_epsg=crs_epsg, out_crs=out_crs)

    merged = geom_gdf.merge(stats_df, on="name", how="left")
    merged.to_parquet(output, compression="snappy")


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
