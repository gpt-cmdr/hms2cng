import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    """
    00 — Using HmsExamples
    ======================
    Demonstrates the HmsExamples API from hms-commander:
    discover installed HMS versions, list example projects, and extract one.
    All other notebooks use this pattern to avoid hardcoded paths.
    """
    import marimo as mo
    mo.md("## 00 — Using HmsExamples")


@app.cell
def __():
    from hms_commander import HmsExamples

    # Detect all installed HMS versions that have bundled example projects
    versions = HmsExamples.list_versions()
    print(f"Found {len(versions)} HMS version(s) with examples:")
    for v, path in versions.items():
        print(f"  {v}  ->  {path}")
    return HmsExamples, versions


@app.cell
def __(HmsExamples, versions):
    # List projects available in the latest installed version
    all_projects = HmsExamples.list_projects()
    latest_version = list(versions.keys())[0]
    project_names = all_projects.get(latest_version, [])
    print(f"Projects in HMS {latest_version}: {project_names}")
    return all_projects, latest_version, project_names


@app.cell
def __(HmsExamples):
    # Extract a project to a local folder for use
    # Returns: Path to the extracted project directory
    project_path = HmsExamples.extract_project("tifton")
    print(f"Extracted project at: {project_path}")

    # Find the basin file
    from pathlib import Path
    basin_files = list(project_path.glob("*.basin"))
    results_dirs = [project_path / "results"] if (project_path / "results").is_dir() else []
    print(f"Basin files:   {[b.name for b in basin_files]}")
    print(f"Results dirs:  {[str(r) for r in results_dirs]}")
    return Path, project_path


@app.cell
def __(HmsExamples):
    # Extract a second project (castro) to show it works for any project
    castro_path = HmsExamples.extract_project("castro")
    print(f"Castro project: {castro_path}")
    basins = list(castro_path.glob("*.basin"))
    print(f"Basin files:    {[b.name for b in basins]}")
    return castro_path,


if __name__ == "__main__":
    app.run()
