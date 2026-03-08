"""hms2cng catalog -- Manifest schema v2.0 for consolidated parquet archives."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class ManifestLayer:
    """Metadata for one layer within the consolidated parquet."""

    name: str
    rows: int
    geometry_type: Optional[str] = None
    crs: Optional[str] = None


@dataclass
class Manifest:
    """Top-level catalog for a consolidated parquet archive."""

    schema_version: str
    project_name: str
    hms_version: Optional[str]
    crs_epsg: Optional[str]
    export_timestamp: str
    parquet_file: str
    total_rows: int
    layers: list[ManifestLayer] = field(default_factory=list)
    basin_models: list[str] = field(default_factory=list)
    run_names: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @classmethod
    def create(
        cls,
        *,
        project_name: str,
        hms_version: Optional[str] = None,
        crs_epsg: Optional[str] = None,
        parquet_file: str = "",
        total_rows: int = 0,
        basin_models: Optional[list[str]] = None,
        run_names: Optional[list[str]] = None,
        errors: Optional[list[str]] = None,
    ) -> Manifest:
        return cls(
            schema_version="2.0",
            project_name=project_name,
            hms_version=hms_version,
            crs_epsg=crs_epsg,
            export_timestamp=datetime.now(timezone.utc).isoformat(),
            parquet_file=parquet_file,
            total_rows=total_rows,
            basin_models=basin_models or [],
            run_names=run_names or [],
            errors=errors or [],
        )

    def add_layer(
        self,
        name: str,
        rows: int,
        geometry_type: Optional[str] = None,
        crs: Optional[str] = None,
    ) -> None:
        self.layers.append(
            ManifestLayer(name=name, rows=rows, geometry_type=geometry_type, crs=crs)
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, **kwargs) -> str:
        return json.dumps(self.to_dict(), indent=2, **kwargs)

    def write(self, path: Path) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> Manifest:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        raw_layers = data.pop("layers", [])
        layers = [ManifestLayer(**layer) for layer in raw_layers]
        return cls(layers=layers, **data)
