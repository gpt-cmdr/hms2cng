from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from hms2cng.results import export_hms_results


def _write_minimal_basin(path: Path) -> None:
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
""".lstrip(),
        encoding="utf-8",
    )


def _write_results_xml(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<RunResults>
  <RunName>RUN1</RunName>
  <BasinElement name="S1" type="Subbasin">
    <Statistics>
      <StatisticMeasure type="Outflow Maximum" displayString="Maximum Outflow" value="123.4" units="CFS" />
      <StatisticMeasure type="Outflow Maximum Time" displayString="Time of Maximum Outflow" value="01Jan2000, 01:00" />
      <StatisticMeasure type="Outflow Average" displayString="Average Outflow" value="10.0" units="CFS" />
    </Statistics>
  </BasinElement>
</RunResults>
""",
        encoding="utf-8",
    )


def test_export_results_from_xml(tmp_path: Path):
    # Arrange a fake project folder so export_hms_results can infer basin geometry
    (tmp_path / "test.hms").write_text("Project: Test\n", encoding="utf-8")

    basin = tmp_path / "test.basin"
    _write_minimal_basin(basin)

    results_dir = tmp_path / "results"
    results_dir.mkdir()
    results_xml = results_dir / "RUN_test.results"
    _write_results_xml(results_xml)

    out = tmp_path / "out.parquet"

    # Act
    export_hms_results(results_dir, out, element_type="subbasin", variable="Flow Out", crs_epsg=None, out_crs=None)

    # Assert
    gdf = gpd.read_parquet(out)
    assert len(gdf) == 1
    assert gdf.iloc[0]["name"] == "S1"
    assert abs(float(gdf.iloc[0]["max_value"]) - 123.4) < 1e-6
    assert gdf.iloc[0]["units"] == "CFS"
