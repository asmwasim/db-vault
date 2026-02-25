"""Main Typer application entry point for db-vault CLI."""

from __future__ import annotations

import typer

from db_vault import __version__
from db_vault.cli.backup import backup_app
from db_vault.cli.config_cmd import config_app
from db_vault.cli.restore import restore_app
from db_vault.cli.schedule import schedule_app
from db_vault.core.config import ensure_dirs
from db_vault.core.models import DatabaseType
from db_vault.logging import setup_logging

app = typer.Typer(
    name="db-vault",
    help="A CLI utility for backing up and restoring databases.",
    no_args_is_help=True,
    rich_markup_mode="rich",
    add_completion=True,
)

# Register sub-command groups
app.add_typer(backup_app, name="backup", help="Backup operations")
app.add_typer(restore_app, name="restore", help="Restore operations")
app.add_typer(schedule_app, name="schedule", help="Manage scheduled backups")
app.add_typer(config_app, name="config", help="Configuration management")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"db-vault {__version__}")
        raise typer.Exit()


@app.callback()
def main_callback(
        version: bool = typer.Option(
            False,
            "--version",
            "-V",
            help="Show version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose (DEBUG) logging.",
        ),
        log_json: bool = typer.Option(
            False,
            "--log-json",
            help="Output logs in JSON format.",
        ),
) -> None:
    """db-vault — Database Backup & Restore Utility."""
    from db_vault.core.models import LogFormat

    ensure_dirs()
    level = "DEBUG" if verbose else "INFO"
    fmt = LogFormat.JSON if log_json else LogFormat.CONSOLE
    setup_logging(level=level, log_format=fmt)


# ──────────────────── test-connection command ────────────


@app.command("test-connection")
def test_connection(
        db_type: DatabaseType = typer.Option(..., "--db-type", "-t", help="Database type."),
        host: str = typer.Option("localhost", "--host", "-H", help="Database host."),
        port: int | None = typer.Option(None, "--port", "-P", help="Database port."),
        username: str | None = typer.Option(None, "--username", "-u", help="Database user."),
        password: str | None = typer.Option(
            None, "--password", "-p", help="Database password.", prompt=False, hide_input=True
        ),
        database: str = typer.Option("", "--database", "-d", help="Database name."),
        ssl: bool = typer.Option(False, "--ssl", help="Use SSL connection."),
) -> None:
    """Test database connectivity and validate credentials."""
    from db_vault.core.models import DatabaseConfig
    from db_vault.engines import get_engine

    config = DatabaseConfig(
        type=db_type,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        ssl=ssl,
    )

    engine = get_engine(config)
    try:
        engine.test_connection()
        typer.echo(
            typer.style("✓ Connection successful!", fg=typer.colors.GREEN, bold=True)
        )
        typer.echo(f"  Engine:   {config.type.value}")
        typer.echo(f"  Host:     {config.host}:{config.port}")
        typer.echo(f"  Database: {config.database or '(default)'}")
    except Exception as exc:
        typer.echo(
            typer.style(f"✗ Connection failed: {exc}", fg=typer.colors.RED, bold=True),
            err=True,
        )
        raise typer.Exit(code=1)


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
