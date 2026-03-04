# Example Notebooks

All examples are [marimo](https://marimo.io) reactive notebooks (`.py` format). They auto-extract HMS example projects via `HmsExamples` from hms-commander — no manual project path configuration needed.

## Run Interactively

```bash
# Install marimo (included in hms2cng[dev] or hms2cng[all])
pip install "hms2cng[all]"

# Open a notebook in the marimo editor
marimo edit examples/01_export_geometry.py

# Run as a script
python examples/01_export_geometry.py
```

## Notebooks

| Notebook | Description |
|----------|-------------|
| [00_using_hms_examples.py](https://github.com/gpt-cmdr/hms2cng/blob/master/examples/00_using_hms_examples.py) | Introduction to `HmsExamples`: list versions, list projects, extract project paths |
| [01_export_geometry.py](https://github.com/gpt-cmdr/hms2cng/blob/master/examples/01_export_geometry.py) | Export all geometry layers (subbasins, junctions, reaches, watershed) from the Tifton example project |
| [02_export_results.py](https://github.com/gpt-cmdr/hms2cng/blob/master/examples/02_export_results.py) | Export simulation results (outflow summary statistics) spatially joined with geometry |
| [03_duckdb_query.py](https://github.com/gpt-cmdr/hms2cng/blob/master/examples/03_duckdb_query.py) | SQL analytics on exported GeoParquet files using DuckDB |
| [04_generate_pmtiles.py](https://github.com/gpt-cmdr/hms2cng/blob/master/examples/04_generate_pmtiles.py) | Generate PMTiles for web visualization (requires tippecanoe + pmtiles on PATH) |
| [05_multi_run_comparison.py](https://github.com/gpt-cmdr/hms2cng/blob/master/examples/05_multi_run_comparison.py) | Compare results across multiple HMS projects using DuckDB joins |

## Output

Each notebook writes output to `out/<notebook_name>/`. This directory is git-ignored. Shared geometry outputs from notebook 01 are used by notebooks 02-04.

## Notes

- Marimo notebooks are plain `.py` files — clean git diffs, runnable as scripts
- `HmsExamples.extract_project()` auto-extracts bundled example projects from hms-commander to a temp directory
- If tippecanoe/pmtiles are not on PATH, notebook 04 will print a helpful error and skip tile generation
