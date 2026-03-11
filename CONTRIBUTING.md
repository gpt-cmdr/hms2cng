# Contributing to hms2cng

Thank you for your interest in contributing to **hms2cng** -- the CLI tool for converting HEC-HMS project spatial information to cloud-native GIS formats. This project is maintained by [CLB Engineering Corporation](https://clbengineering.com/) and released under the MIT license.

---

## Our Philosophy: Don't Ask Me, Ask a GPT!

hms2cng was built with LLMs, and we welcome LLM-assisted contributions. Use whatever agent or model works for you -- Claude Code, Cursor, Copilot, Aider, or anything else. We have one expectation: **your LLM reviews its own work before you open a PR.**

This is not about gatekeeping. It is about reducing the back-and-forth that slows everyone down. A self-reviewed PR from an LLM-assisted contributor is typically ready to merge with minimal maintainer effort.

Learn more about our approach: [LLM Forward Engineering](https://clbengineering.com/llm-forward)

---

## Quick Start

```bash
# 1. Fork and clone
git clone https://github.com/<your-username>/hms2cng.git
cd hms2cng

# 2. Install in development mode
pip install -e ".[dev]"

# 3. Verify tests pass
pytest tests/

# 4. Launch your preferred LLM agent and start working
```

If you use `uv`:

```bash
uv pip install -e ".[dev]"
uv run pytest tests/
```

### Project Structure

```
hms2cng/
  cli.py          # Typer CLI commands (entry point)
  geometry.py     # Basin geometry extraction via hms-commander
  results.py      # Simulation results extraction
  project.py      # Full project export orchestration
  catalog.py      # Parquet catalog utilities
  duckdb_session.py  # DuckDB query session
  pmtiles.py      # PMTiles generation (tippecanoe)
  postgis_sync.py # PostGIS synchronization
```

---

## The Self-Review Contract

Before opening a PR, have your LLM review its own output. Paste the diff or point your agent at the changed files and ask it to evaluate the checklist below. Include the results in your PR description.

This is the single most impactful thing you can do to speed up the review process.

---

## LLM Self-Review Checklist

Have your LLM confirm each item. Copy this into your PR and check the boxes.

### Code Quality

- [ ] All public functions have docstrings (Args, Returns, Raises)
- [ ] Logging used for operational messages (`logger.info/warning/error`), not `print()`
- [ ] Errors are caught with specific exceptions and clear messages
- [ ] File paths use `pathlib.Path`, not string concatenation
- [ ] Type hints on function signatures
- [ ] No hardcoded paths or credentials

### CLI Specifics

- [ ] CLI arguments follow existing Typer patterns in `cli.py`
- [ ] New commands include `--help` text and parameter descriptions
- [ ] Output formats target GeoParquet (`.parquet`) as the primary format
- [ ] PMTiles and PostGIS paths remain optional features
- [ ] Cloud-native patterns preserved (columnar storage, HTTP range requests, no server dependency)

### HEC-HMS Specifics

- [ ] HEC-HMS data extraction uses `hms-commander` API, not raw file parsing
- [ ] Basin model layer names match hms-commander conventions (subbasins, reaches, junctions, etc.)
- [ ] Simulation results reference the correct HMS output structure
- [ ] No assumptions about HMS version-specific file formats outside hms-commander

---

## What We Accept

- New export formats (FlatGeobuf, GeoJSON, Arrow IPC)
- CLI enhancements (new subcommands, better progress output, batch modes)
- DuckDB query improvements and cross-project analytics
- Documentation improvements and example notebooks
- Bug fixes with clear reproduction steps
- Test coverage improvements
- Performance improvements with benchmarks
- Integration patterns with other cloud-native GIS tooling

---

## What We Don't Accept

- **Breaking CLI interface changes without discussion.** Open an issue first if you want to change existing command signatures, argument names, or output structure. Users script against these interfaces.
- **Unjustified new dependencies.** If your change adds a new dependency, explain why an existing dependency or the standard library cannot do the job. Keep the base install lightweight.
- **Raw HMS file parsing that bypasses hms-commander.** The hms-commander library is the abstraction layer for HEC-HMS. Contributions should use its API.
- **Code without any form of testing.** At minimum, describe how you tested. Preferably add a pytest case.

---

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(cli): add FlatGeobuf export command
fix(results): handle missing outflow time series gracefully
docs: update Quick Start with uv instructions
test(geometry): add reach extraction coverage
refactor(project): simplify manifest generation
```

If an LLM contributed to the code, include attribution:

```
feat(cli): add batch export mode

Co-Authored-By: Claude <noreply@anthropic.com>
```

---

## Branching and PRs

1. Create a feature branch from `main`: `git checkout -b feat/my-feature`
2. Make your changes in focused, reviewable commits
3. Run `pytest tests/` and confirm tests pass
4. Open a PR against `main` using the PR template
5. Fill out the LLM Self-Review section in the PR description

Keep PRs focused. One feature or fix per PR is ideal.

---

## Development Tips

```bash
# CLI
hms2cng --help
hms2cng manifest path/to/Project.hms

# Tests
pytest tests/                        # All tests
pytest tests/ -m "not integration"   # Skip tests requiring HMS data

# Docs
pip install -e ".[docs]" && mkdocs serve
```

---

## Community Standards

- Be respectful and constructive in all interactions
- Technical disagreements are resolved with evidence and benchmarks, not authority
- LLM-assisted contributions are first-class citizens -- no stigma, no gatekeeping
- Professional conduct applies to all participants

This project follows the [LLM Forward](https://clbengineering.com/llm-forward) philosophy: technology accelerates engineering insight without replacing professional judgment.

---

## Getting Help

- **Questions about the codebase:** Open a GitHub Discussion or issue
- **Bug reports:** Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md)
- **Feature ideas:** Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md)
- **hms-commander questions:** See [hms-commander](https://github.com/gpt-cmdr/hms-commander)
- **CLB Engineering:** [info@clbengineering.com](mailto:info@clbengineering.com)

Or just ask your LLM. It probably knows.

---

**Maintained by [CLB Engineering Corporation](https://clbengineering.com/) -- MIT License**
