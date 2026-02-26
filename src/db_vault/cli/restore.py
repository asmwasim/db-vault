"""CLI restore subcommands."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from db_vault.core.models import DatabaseType

restore_app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")
console = Console()


@restore_app.command("run")
def restore_run(
        # Database connection
        db_type: DatabaseType = typer.Option(..., "--db-type", "-t", help="Database type."),
        host: str = typer.Option("localhost", "--host", "-H", help="Database host."),
        port: int | None = typer.Option(None, "--port", "-P", help="Database port."),
        username: str | None = typer.Option(None, "--username", "-u", help="Database user."),
        password: str | None = typer.Option(
            None, "--password", "-p", help="Database password.", hide_input=True
        ),
        database: str = typer.Option("", "--database", "-d", help="Database name."),
        ssl: bool = typer.Option(False, "--ssl", help="Use SSL."),
        # Restore options
        backup_file: Path = typer.Option(
            ..., "--file", "-f", help="Path to the backup file."
        ),
        target_db: str | None = typer.Option(
            None, "--target-db", help="Restore to a different database name."
        ),
        tables: str | None = typer.Option(
            None, "--tables", help="Comma-separated list of tables to restore (selective)."
        ),
        drop_existing: bool = typer.Option(
            False, "--drop-existing", help="Drop existing objects before restore."
        ),
        no_owner: bool = typer.Option(
            False, "--no-owner",
            help="Skip restoring object ownership (useful for cross-server restores).",
        ),
        dry_run: bool = typer.Option(
            False, "--dry-run", help="Show what would be restored without executing."
        ),
        yes: bool = typer.Option(
            False, "--yes", "-y", help="Skip confirmation prompt."
        ),
        # Config profile
        profile: str | None = typer.Option(
            None, "--profile", help="Use a named database profile from config."
        ),
) -> None:
    """Restore a database from a backup file.

    Examples:
        db-vault restore run --db-type sqlite --database ./my.db --file ./backups/backup.db
        db-vault restore run --db-type postgres -u admin -d mydb --file backup.dump
        db-vault restore run --db-type mysql -u root -d shop --file dump.sql --tables users,orders
    """
    from db_vault.compression.compressor import decompress_file, detect_algorithm
    from db_vault.core.config import load_config
    from db_vault.core.models import (
        CompressionAlgorithm,
        DatabaseConfig,
        RestoreRequest,
    )
    from db_vault.engines import get_engine
    from db_vault.logging import get_logger

    log = get_logger("restore")

    # ── Resolve config ──
    if profile:
        app_config = load_config()
        if profile not in app_config.databases:
            console.print(f"[red]Profile '{profile}' not found in config.[/red]")
            raise typer.Exit(code=1)
        db_config = app_config.databases[profile]
    else:
        db_config = DatabaseConfig(
            type=db_type,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            ssl=ssl,
        )

    # Validate backup file exists
    if not backup_file.exists():
        console.print(f"[red]Backup file not found: {backup_file}[/red]")
        raise typer.Exit(code=1)

    table_list = [t.strip() for t in tables.split(",")] if tables else None

    # ── Confirmation ──
    if not dry_run and not yes:
        console.print("\n[bold]Restore Summary:[/bold]")
        console.print(f"  Source:     {backup_file}")
        console.print(f"  Target DB:  {target_db or db_config.database or '(default)'}")
        console.print(f"  Engine:     {db_config.type.value}")
        if table_list:
            console.print(f"  Tables:     {', '.join(table_list)}")
        if drop_existing:
            console.print("  [yellow]⚠ Will DROP existing objects before restore[/yellow]")
        console.print()

        confirm = typer.confirm("Proceed with restore?")
        if not confirm:
            console.print("[yellow]Restore cancelled.[/yellow]")
            raise typer.Exit()

    try:
        # ── Step 1: Decompress if needed ──
        algorithm = detect_algorithm(backup_file)
        actual_file = backup_file
        if algorithm != CompressionAlgorithm.NONE:
            with console.status(f"[bold blue]Decompressing ({algorithm.value})..."):
                actual_file = decompress_file(backup_file)
            console.print(f"[green]✓[/green] Decompressed: {actual_file.name}")

        # ── Step 2: Test connection ──
        engine = get_engine(db_config)
        if db_config.type.value != "sqlite":
            with console.status("[bold blue]Testing connection..."):
                engine.test_connection()
            console.print("[green]✓[/green] Connection OK")

        # ── Step 3: Restore ──
        request = RestoreRequest(
            backup_file=actual_file,
            target_database=target_db,
            tables=table_list,
            dry_run=dry_run,
            drop_existing=drop_existing,
            no_owner=no_owner,
        )

        with console.status("[bold blue]Restoring..."):
            engine.restore(request)

        if dry_run:
            console.print("[green]✓[/green] Dry run complete (no changes made)")
        else:
            console.print("[bold green]✓ Restore completed successfully![/bold green]")

        # Clean up decompressed temp file
        if actual_file != backup_file:
            actual_file.unlink(missing_ok=True)

    except Exception as exc:
        console.print(f"\n[bold red]✗ Restore failed: {exc}[/bold red]")
        log.error("restore_failed", error=str(exc))
        raise typer.Exit(code=1)
