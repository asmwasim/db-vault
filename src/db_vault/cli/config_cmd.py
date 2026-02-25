"""CLI config subcommands for managing db-vault configuration."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.syntax import Syntax

from db_vault.core.models import (
    CompressionAlgorithm,
    CompressionConfig,
    DatabaseType,
    LogFormat,
    LoggingConfig,
    NotificationConfig,
    StorageType,
)

config_app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")
console = Console()


@config_app.command("init")
def config_init(
        path: Path | None = typer.Option(
            None, "--path", help="Custom config file location."
        ),
) -> None:
    """Create or update the configuration file interactively.

    If no --path is given, writes to the default location:
      macOS:  ~/Library/Application Support/db-vault/config.toml
      Linux:  ~/.config/db-vault/config.toml
    """
    from db_vault.core.config import CONFIG_FILE, save_config_file
    from db_vault.core.models import AppConfig, DatabaseConfig, StorageConfig

    target = path or CONFIG_FILE

    if target.exists():
        overwrite = typer.confirm(f"Config already exists at {target}. Overwrite?")
        if not overwrite:
            console.print("[yellow]Aborted.[/yellow]")
            raise typer.Exit()

    console.print("[bold]db-vault configuration wizard[/bold]\n")

    # ── Database ──
    console.print("[bold blue]Database Connection[/bold blue]")
    db_type = typer.prompt(
        "Database type",
        type=typer.Choice([t.value for t in DatabaseType]),
        default="postgres",
    )
    db_name = typer.prompt("Profile name for this database", default="default")

    db_config_kwargs: dict = {"type": DatabaseType(db_type)}

    if db_type != "sqlite":
        db_config_kwargs["host"] = typer.prompt("Host", default="localhost")
        default_ports = {"postgres": 5432, "mysql": 3306, "mongodb": 27017}
        db_config_kwargs["port"] = typer.prompt(
            "Port", default=default_ports.get(db_type, 5432), type=int
        )
        db_config_kwargs["username"] = typer.prompt("Username", default="")
        pw = typer.prompt("Password (leave empty to skip)", default="", hide_input=True)
        if pw:
            db_config_kwargs["password"] = pw
        db_config_kwargs["database"] = typer.prompt("Database name", default="")
        db_config_kwargs["ssl"] = typer.confirm("Use SSL?", default=False)
    else:
        db_config_kwargs["database"] = typer.prompt("SQLite file path", default="./database.db")

    databases = {db_name: DatabaseConfig(**db_config_kwargs)}

    # ── Storage ──
    console.print("\n[bold blue]Storage[/bold blue]")
    storage_type = typer.prompt(
        "Storage type",
        type=typer.Choice([t.value for t in StorageType]),
        default="local",
    )
    storage_kwargs: dict = {"type": StorageType(storage_type)}

    if storage_type == "local":
        storage_kwargs["local_path"] = Path(
            typer.prompt("Local backup directory", default="./backups")
        )
    else:
        storage_kwargs["s3_bucket"] = typer.prompt("S3 bucket name")
        storage_kwargs["s3_prefix"] = typer.prompt("S3 key prefix", default="db-vault/")
        storage_kwargs["s3_region"] = typer.prompt("AWS region", default="us-east-1")

    storage = StorageConfig(**storage_kwargs)

    # ── Compression ──
    console.print("\n[bold blue]Compression[/bold blue]")
    comp_algo = typer.prompt(
        "Algorithm",
        type=typer.Choice([a.value for a in CompressionAlgorithm]),
        default="zstd",
    )
    comp_level = typer.prompt("Level (1-22)", default=3, type=int)
    compression = CompressionConfig(
        algorithm=CompressionAlgorithm(comp_algo), level=comp_level
    )

    # ── Notifications ──
    console.print("\n[bold blue]Notifications (optional)[/bold blue]")
    slack_url = typer.prompt("Slack webhook URL (leave empty to skip)", default="")
    notification = NotificationConfig(
        slack_webhook_url=slack_url if slack_url else None,
    )

    # ── Logging ──
    logging_config = LoggingConfig(level="INFO", format=LogFormat.CONSOLE)

    # ── Save ──
    config = AppConfig(
        databases=databases,
        storage=storage,
        compression=compression,
        notification=notification,
        logging=logging_config,
    )

    saved_path = save_config_file(config, target)
    console.print(f"\n[green]✓[/green] Config saved to: {saved_path}")
    console.print("  File permissions set to 600 (owner-only read/write).")


@config_app.command("show")
def config_show(
        path: Path | None = typer.Option(
            None, "--path", help="Custom config file location."
        ),
) -> None:
    """Display the current configuration."""
    from db_vault.core.config import CONFIG_FILE

    target = path or CONFIG_FILE

    if not target.exists():
        console.print(
            f"[yellow]No config file found at {target}.[/yellow]\n"
            f"Run [bold]db-vault config init[/bold] to create one."
        )
        raise typer.Exit()

    content = target.read_text()
    syntax = Syntax(content, "toml", theme="monokai", line_numbers=True)
    console.print(f"[bold]Config: {target}[/bold]\n")
    console.print(syntax)


@config_app.command("path")
def config_path() -> None:
    """Show config and data directory paths."""
    from db_vault.core.config import (
        CONFIG_DIR,
        CONFIG_FILE,
        DATA_DIR,
        LOG_DIR,
        METADATA_DIR,
        SCHEDULER_DB,
    )

    console.print("[bold]db-vault paths:[/bold]")
    console.print(f"  Config dir:    {CONFIG_DIR}")
    console.print(f"  Config file:   {CONFIG_FILE}")
    console.print(f"  Data dir:      {DATA_DIR}")
    console.print(f"  Logs dir:      {LOG_DIR}")
    console.print(f"  Metadata dir:  {METADATA_DIR}")
    console.print(f"  Scheduler DB:  {SCHEDULER_DB}")
