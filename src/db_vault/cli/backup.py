"""CLI backup subcommands."""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from db_vault.core.models import (
    BackupMetadata,
    BackupStatus,
    BackupType,
    CompressionAlgorithm,
    DatabaseType,
    StorageType,
)

backup_app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")
console = Console()


@backup_app.command("run")
def backup_run(
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
        # Backup options
        backup_type: BackupType = typer.Option(
            BackupType.FULL, "--backup-type", "-b", help="Backup type."
        ),
        tables: str | None = typer.Option(
            None, "--tables", help="Comma-separated list of tables/collections to back up."
        ),
        # Compression
        compression: CompressionAlgorithm = typer.Option(
            CompressionAlgorithm.ZSTD, "--compression", "-c", help="Compression algorithm."
        ),
        compression_level: int = typer.Option(3, "--compression-level", help="Compression level."),
        # Storage
        storage: StorageType = typer.Option(
            StorageType.LOCAL, "--storage", "-s", help="Storage backend."
        ),
        output_dir: Path = typer.Option(
            Path("./backups"), "--output-dir", "-o", help="Local output directory."
        ),
        s3_bucket: str | None = typer.Option(None, "--s3-bucket", help="S3 bucket name."),
        s3_prefix: str = typer.Option("db-vault/", "--s3-prefix", help="S3 key prefix."),
        s3_region: str = typer.Option("us-east-1", "--s3-region", help="AWS region."),
        s3_endpoint: str | None = typer.Option(None, "--s3-endpoint", help="S3 endpoint URL."),
        # Notification
        slack_webhook: str | None = typer.Option(
            None, "--slack-webhook",
            help="Slack webhook URL for notifications.",
            envvar="DB_VAULT_SLACK_WEBHOOK_URL",
        ),
        # Config profile
        profile: str | None = typer.Option(
            None, "--profile", help="Use a named database profile from config."
        ),
) -> None:
    """Execute a database backup.

    Examples:
        db-vault backup run --db-type sqlite --database ./my.db
        db-vault backup run --db-type postgres -H localhost -u admin -d mydb
        db-vault backup run --db-type mysql -u root -d shop --storage s3 --s3-bucket my-backups
    """
    from db_vault.compression.compressor import compress_file, compute_checksum
    from db_vault.core.config import load_config
    from db_vault.core.models import (
        DatabaseConfig,
        StorageConfig,
    )
    from db_vault.engines import get_engine
    from db_vault.logging import get_logger
    from db_vault.storage import get_storage

    log = get_logger("backup")

    # ── Resolve config (profile or CLI args) ──
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

    table_list = [t.strip() for t in tables.split(",")] if tables else None

    storage_config = StorageConfig(
        type=storage,
        local_path=output_dir,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_region=s3_region,
        s3_endpoint_url=s3_endpoint,
    )

    # ── Initialise metadata ──
    metadata = BackupMetadata(
        database_name=db_config.database or db_config.host,
        database_type=db_config.type,
        backup_type=backup_type,
        file_name="",
        file_path="",
        compression=compression,
        storage_type=storage_config.type,
        tables=table_list,
    )

    engine = get_engine(db_config)
    start_time = time.time()

    try:
        # ── Step 1: Test connection ──
        with console.status("[bold blue]Testing connection..."):
            engine.test_connection()
        console.print("[green]✓[/green] Connection OK")

        # ── Step 2: Backup ──
        with console.status("[bold blue]Running backup..."):
            raw_file = engine.backup(
                output_dir=output_dir,
                backup_type=backup_type,
                tables=table_list,
            )
        metadata.file_size = raw_file.stat().st_size
        console.print(f"[green]✓[/green] Backup created: {raw_file.name}")

        # ── Step 3: Compress ──
        if compression != CompressionAlgorithm.NONE:
            with console.status(f"[bold blue]Compressing ({compression.value})..."):
                compressed_file = compress_file(
                    raw_file,
                    algorithm=compression,
                    level=compression_level,
                )
                # Remove uncompressed file after successful compression
                if compressed_file != raw_file:
                    raw_file.unlink(missing_ok=True)
                    raw_file = compressed_file
            metadata.compressed_size = raw_file.stat().st_size
            ratio = metadata.compression_ratio
            console.print(
                f"[green]✓[/green] Compressed: {metadata.size_human} "
                f"({ratio:.0%} of original)"
            )
        else:
            metadata.compressed_size = metadata.file_size

        metadata.file_name = raw_file.name

        # ── Step 4: Checksum ──
        with console.status("[bold blue]Computing checksum..."):
            metadata.checksum_sha256 = compute_checksum(raw_file)
        console.print(f"[green]✓[/green] SHA-256: {metadata.checksum_sha256[:16]}...")

        # ── Step 5: Upload to storage ──
        storage_backend = get_storage(storage_config)
        remote_key = (
            f"{db_config.type.value}/{db_config.database or 'default'}/"
            f"{datetime.utcnow().strftime('%Y-%m-%d')}/{raw_file.name}"
        )
        with console.status(f"[bold blue]Uploading to {storage_config.type.value}..."):
            location = storage_backend.upload(raw_file, remote_key)
        metadata.file_path = location
        console.print(f"[green]✓[/green] Stored: {location}")

        # Clean up the temporary file if it was copied to a different location
        if raw_file.resolve() != Path(location).resolve():
            raw_file.unlink(missing_ok=True)

        # ── Finalise ──
        metadata.status = BackupStatus.COMPLETED
        elapsed = time.time() - start_time
        metadata.duration_seconds = elapsed

        # Save metadata
        _save_metadata(metadata)

        console.print()
        console.print(f"[bold green]Backup completed in {elapsed:.1f}s[/bold green]")
        console.print(f"  ID:         {metadata.id}")
        console.print(f"  File:       {metadata.file_name}")
        console.print(f"  Size:       {metadata.size_human}")
        console.print(f"  Checksum:   {metadata.checksum_sha256[:32]}...")

    except Exception as exc:
        metadata.status = BackupStatus.FAILED
        metadata.error_message = str(exc)
        metadata.duration_seconds = time.time() - start_time
        _save_metadata(metadata)

        console.print(f"\n[bold red]✗ Backup failed: {exc}[/bold red]", err=True)
        log.error("backup_failed", error=str(exc), database=db_config.database)
        raise typer.Exit(code=1)

    # ── Send notification (if configured) ──
    if slack_webhook:
        try:
            from db_vault.notifications.slack import SlackNotifier

            notifier = SlackNotifier(slack_webhook)
            if metadata.status == BackupStatus.COMPLETED:
                notifier.notify_success(metadata)
            else:
                notifier.notify_failure(metadata)
            console.print("[green]✓[/green] Slack notification sent")
        except Exception as exc:
            console.print(f"[yellow]⚠ Slack notification failed: {exc}[/yellow]", err=True)


@backup_app.command("list")
def backup_list(
        storage: StorageType = typer.Option(
            StorageType.LOCAL, "--storage", "-s", help="Storage backend."
        ),
        output_dir: Path = typer.Option(
            Path("./backups"), "--output-dir", "-o", help="Local backup directory."
        ),
        s3_bucket: str | None = typer.Option(None, "--s3-bucket", help="S3 bucket."),
        s3_prefix: str = typer.Option("db-vault/", "--s3-prefix", help="S3 key prefix."),
        s3_region: str = typer.Option("us-east-1", "--s3-region", help="AWS region."),
        s3_endpoint: str | None = typer.Option(None, "--s3-endpoint", help="S3 endpoint URL."),
        prefix: str = typer.Option("", "--prefix", help="Filter by key prefix."),
) -> None:
    """List available backups."""
    from db_vault.core.models import StorageConfig
    from db_vault.storage import get_storage

    storage_config = StorageConfig(
        type=storage,
        local_path=output_dir,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        s3_region=s3_region,
        s3_endpoint_url=s3_endpoint,
    )

    backend = get_storage(storage_config)
    backups = backend.list_backups(prefix=prefix)

    if not backups:
        console.print("[yellow]No backups found.[/yellow]")
        return

    table = Table(title="Available Backups", show_lines=True)
    table.add_column("Key", style="cyan")
    table.add_column("Size", style="green", justify="right")
    table.add_column("Last Modified", style="magenta")

    for b in backups:
        size = int(b["size"])
        from db_vault.core.models import _human_size

        table.add_row(b["key"], _human_size(size), b["last_modified"])

    console.print(table)


@backup_app.command("history")
def backup_history(
        limit: int = typer.Option(20, "--limit", "-n", help="Number of recent backups to show."),
) -> None:
    """Show backup history from metadata."""
    from db_vault.core.config import METADATA_DIR
    from db_vault.core.models import _human_size

    meta_files = sorted(METADATA_DIR.glob("*.json"), reverse=True)[:limit]

    if not meta_files:
        console.print("[yellow]No backup history found.[/yellow]")
        return

    table = Table(title="Backup History", show_lines=True)
    table.add_column("ID", style="cyan")
    table.add_column("Database", style="blue")
    table.add_column("Type", style="green")
    table.add_column("Status", style="bold")
    table.add_column("Size", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Timestamp", style="magenta")

    for mf in meta_files:
        try:
            data = json.loads(mf.read_text())
            status = data.get("status", "unknown")
            status_style = "green" if status == "completed" else "red"
            table.add_row(
                data.get("id", "?")[:12],
                data.get("database_name", "?"),
                data.get("backup_type", "?"),
                f"[{status_style}]{status}[/{status_style}]",
                _human_size(data.get("compressed_size", 0)),
                f"{data.get('duration_seconds', 0):.1f}s",
                data.get("timestamp", "?")[:19],
            )
        except Exception:
            continue

    console.print(table)


def _save_metadata(metadata: BackupMetadata) -> None:
    """Persist backup metadata to a JSON file."""
    from db_vault.core.config import METADATA_DIR

    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    meta_file = METADATA_DIR / f"{metadata.timestamp.strftime('%Y%m%d_%H%M%S')}_{metadata.id}.json"
    meta_file.write_text(metadata.model_dump_json(indent=2))
