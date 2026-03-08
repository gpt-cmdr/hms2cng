"""hms2cng: CLI for exporting HEC-HMS geometry/results to GeoParquet and PMTiles."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

app = typer.Typer(
    help="Export HEC-HMS results to GeoParquet, query with DuckDB, and generate PMTiles"
)

# Avoid Unicode glyphs in status messages; SSH sessions on Windows often default to cp1252.
console = Console(emoji=False)


@app.command("geometry")
def export_geometry(
    basin_file: Path = typer.Argument(..., help="HEC-HMS basin model file (.basin) or project directory"),
    output: Path = typer.Argument(..., help="Output GeoParquet file path"),
    layer: Optional[str] = typer.Option(
        None,
        "--layer",
        "-l",
        help="Geometry layer: subbasins, reaches, junctions, diversions, reservoirs, sources, sinks, watershed, subbasin_polygons, longest_flowpaths, centroidal_flowpaths, teneightyfive_flowpaths, subbasin_statistics",
    ),
    crs_epsg: Optional[str] = typer.Option(
        None,
        "--crs",
        help="Input/project CRS (e.g. EPSG:2278). If omitted, attempt auto-detect from project.",
    ),
    out_crs: Optional[str] = typer.Option(
        "EPSG:4326",
        "--out-crs",
        help="Output CRS (default EPSG:4326). If input CRS is unknown, no reprojection is done.",
    ),
):
    """Export HEC-HMS basin geometry to GeoParquet."""

    from hms2cng.geometry import export_basin_geometry

    console.print(f"[bold blue]Exporting geometry:[/bold blue] {basin_file}")
    try:
        export_basin_geometry(basin_file, output, layer=layer, crs_epsg=crs_epsg, out_crs=out_crs)
        console.print(f"[green]OK[/green] Exported: {output}")
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)


@app.command("results")
def export_results(
    results_dir: Path = typer.Argument(..., help="HMS results folder (usually <project>/results) or RUN_*.results file"),
    output: Path = typer.Argument(..., help="Output GeoParquet file path"),
    element_type: str = typer.Option(
        "subbasin",
        "--type",
        "-t",
        help="Element type: subbasin, reach, junction, diversion, reservoir, source, sink, all",
    ),
    variable: str = typer.Option(
        "Flow Out",
        "--var",
        "-v",
        help="Variable prefix to export from HMS statistics (e.g. 'Outflow', 'Inflow', 'Stage').",
    ),
    crs_epsg: Optional[str] = typer.Option(
        None,
        "--crs",
        help="Input/project CRS (e.g. EPSG:2278). If omitted, attempt auto-detect from project.",
    ),
    out_crs: Optional[str] = typer.Option(
        "EPSG:4326",
        "--out-crs",
        help="Output CRS (default EPSG:4326). If input CRS is unknown, no reprojection is done.",
    ),
):
    """Export HMS results summary statistics to GeoParquet."""

    from hms2cng.results import export_hms_results

    console.print(f"[bold blue]Exporting results:[/bold blue] {results_dir}")
    console.print(f"[dim]type={element_type}, var={variable}[/dim]")
    try:
        export_hms_results(
            results_dir,
            output,
            element_type=element_type,
            variable=variable,
            crs_epsg=crs_epsg,
            out_crs=out_crs,
        )
        console.print(f"[green]OK[/green] Exported: {output}")
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)


@app.command("query")
def query_parquet(
    input_file: Path = typer.Argument(..., help="Input GeoParquet file"),
    sql: str = typer.Argument(..., help="SQL query (use _ as table name)"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Optional output file (CSV or Parquet)"),
):
    """Query GeoParquet files using DuckDB SQL."""

    from hms2cng.duckdb_session import query_parquet as _query

    console.print(f"[bold blue]Querying:[/bold blue] {input_file}")
    try:
        df = _query(input_file, sql)
        console.print(f"[green]OK[/green] rows={len(df)}")

        if output:
            if output.suffix.lower() == ".csv":
                df.to_csv(output, index=False)
            else:
                df.to_parquet(output, index=False)
            console.print(f"[green]OK[/green] Saved: {output}")
        else:
            console.print(df.head(20).to_string())
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)


@app.command("pmtiles")
def generate_pmtiles(
    input_file: Path = typer.Argument(..., help="Input GeoParquet file"),
    output: Path = typer.Argument(..., help="Output PMTiles file path"),
    layer_name: str = typer.Option("layer", "--layer", "-l", help="Layer name for vector tiles"),
    min_zoom: Optional[int] = typer.Option(None, "--min-zoom", help="Minimum zoom level"),
    max_zoom: Optional[int] = typer.Option(None, "--max-zoom", help="Maximum zoom level"),
):
    """Generate PMTiles from GeoParquet."""

    from hms2cng.pmtiles import generate_pmtiles_from_input

    console.print(f"[bold blue]Generating PMTiles:[/bold blue] {input_file}")
    try:
        generate_pmtiles_from_input(
            input_file,
            output,
            layer_name=layer_name,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
        )
        console.print(f"[green]OK[/green] PMTiles created: {output}")
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)


@app.command("sync")
def sync_to_postgis(
    input_file: Path = typer.Argument(..., help="Input GeoParquet file"),
    postgres_uri: str = typer.Argument(..., help="PostgreSQL connection URI"),
    table_name: str = typer.Argument(..., help="Target table name"),
    schema: str = typer.Option("public", "--schema", "-s", help="Target schema"),
):
    """Sync GeoParquet data to PostGIS."""

    from hms2cng.postgis_sync import sync_to_postgres

    console.print(f"[bold blue]Syncing to PostGIS:[/bold blue] {schema}.{table_name}")
    try:
        sync_to_postgres(input_file, postgres_uri, table_name, schema=schema)
        console.print(f"[green]OK[/green] Synced: {schema}.{table_name}")
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)


@app.command("manifest")
def show_manifest(
    hms_file: Path = typer.Argument(..., help="HEC-HMS project file (.hms) or project directory"),
    output_dir: Optional[Path] = typer.Option(
        None, "--output", "-o", help="If provided, write manifest parquet files here"
    ),
):
    """Show (or export) the project manifest: runs, basin models, met models."""

    from hms2cng.project import _find_hms_file, _init_project, _find_results_xml_for_run

    try:
        hms_path = _find_hms_file(Path(hms_file))
        prj = _init_project(hms_path)
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)

    from rich.table import Table

    # --- Run table ---
    run_table = Table(title=f"Runs — {prj.project_name}", show_lines=False)
    run_table.add_column("Run Name", style="bold")
    run_table.add_column("Basin Model")
    run_table.add_column("Met Model")
    run_table.add_column("Control Spec")
    run_table.add_column("Has Results", justify="center")

    for _, row in prj.run_df.iterrows():
        run_name = row.get("name", "")
        results_xml = _find_results_xml_for_run(hms_path.parent, run_name)
        run_table.add_row(
            run_name,
            row.get("basin_model", ""),
            row.get("met_model", ""),
            row.get("control_spec", ""),
            "[green]yes[/green]" if results_xml else "[dim]no[/dim]",
        )

    console.print(run_table)
    console.print(
        f"[dim]{len(prj.basin_df)} basin model(s), "
        f"{len(prj.met_df)} met model(s), "
        f"{len(prj.run_df)} run(s)[/dim]"
    )

    if output_dir is not None:
        from hms2cng.project import export_project_manifest

        try:
            mp, rp, bp = export_project_manifest(hms_file, output_dir)
            console.print(f"[green]OK[/green] manifest    → {mp}")
            console.print(f"[green]OK[/green] run_registry → {rp}")
            console.print(f"[green]OK[/green] basin_inventory → {bp}")
        except Exception as e:
            console.print(f"[red]ERROR[/red] {e}")
            raise typer.Exit(1)


@app.command("project")
def export_project(
    hms_file: Path = typer.Argument(..., help="HEC-HMS project file (.hms) or project directory"),
    output_dir: Path = typer.Argument(..., help="Output directory for consolidated archive"),
    layers: Optional[str] = typer.Option(
        None, "--layers",
        help="Comma-separated geometry layers (default: all available)"
    ),
    variables: Optional[str] = typer.Option(
        None, "--vars",
        help="Comma-separated result variables (default: Outflow,Inflow,Stage,Depth)"
    ),
    out_crs: str = typer.Option("EPSG:4326", "--out-crs", help="Output CRS"),
    skip_errors: bool = typer.Option(True, "--skip-errors/--fail-fast"),
    no_sort: bool = typer.Option(False, "--no-sort", help="Disable Hilbert spatial sorting"),
):
    """Export an entire HMS project to a single consolidated GeoParquet."""

    from hms2cng.project import export_full_project

    layer_list = [s.strip() for s in layers.split(",")] if layers else None
    var_list = [s.strip() for s in variables.split(",")] if variables else None

    console.print(f"[bold blue]Exporting project:[/bold blue] {hms_file}")
    console.print(f"[dim]output -> {output_dir}[/dim]")

    try:
        summary = export_full_project(
            hms_file,
            output_dir,
            layers=layer_list,
            variables=var_list,
            out_crs=out_crs,
            skip_errors=skip_errors,
            sort=not no_sort,
        )
    except Exception as e:
        console.print(f"[red]ERROR[/red] {e}")
        raise typer.Exit(1)

    from rich.table import Table

    result_table = Table(title="Export Summary", show_lines=False)
    result_table.add_column("Category", style="bold")
    result_table.add_column("Count", justify="right")
    result_table.add_row("Geometry rows", str(summary["geometry_rows"]))
    result_table.add_row("Results rows", str(summary["results_rows"]))
    result_table.add_row("Errors", str(len(summary["errors"])))
    console.print(result_table)

    if summary["errors"]:
        console.print("[yellow]Errors:[/yellow]")
        for err in summary["errors"]:
            console.print(f"  [dim]{err}[/dim]")

    if summary["parquet_file"]:
        console.print(f"[green]OK[/green] Parquet: {summary['parquet_file']}")
    if summary["manifest_json"]:
        console.print(f"[green]OK[/green] Manifest: {summary['manifest_json']}")


if __name__ == "__main__":
    app()
