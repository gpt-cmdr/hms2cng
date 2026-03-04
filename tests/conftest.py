"""Shared pytest configuration for hms2cng tests."""

import json
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (requires real HMS data on disk)",
    )


# ---------------------------------------------------------------------------
# Integration test report collector
# ---------------------------------------------------------------------------

REPORT_PATH = Path("I:/hmscmdr-parquet-testing/report.json")


class IntegrationReport:
    """Collects results from integration tests and writes a JSON report."""

    def __init__(self):
        self.records: list[dict] = []

    def add(self, *, source: str, project: str, operation: str,
            output_path: str | None = None, status: str = "ok",
            error: str | None = None, rows: int | None = None,
            file_size: int | None = None):
        self.records.append({
            "source": source,
            "project": project,
            "operation": operation,
            "output_path": output_path,
            "status": status,
            "error": error,
            "rows": rows,
            "file_size_bytes": file_size,
        })

    def summary(self) -> dict:
        total = len(self.records)
        ok = sum(1 for r in self.records if r["status"] == "ok")
        fail = sum(1 for r in self.records if r["status"] == "fail")
        skip = sum(1 for r in self.records if r["status"] == "skip")
        return {"total": total, "ok": ok, "fail": fail, "skip": skip}

    def write(self, path: Path | None = None):
        path = path or REPORT_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"summary": self.summary(), "details": self.records}
        path.write_text(json.dumps(payload, indent=2, default=str))


@pytest.fixture(scope="session")
def integration_report():
    """Session-scoped fixture that collects integration test results."""
    report = IntegrationReport()
    yield report
    if report.records:
        report.write()
