"""CLI schedule subcommands for managing automatic backups."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from db_vault.core.models import (
    BackupType,
    CompressionAlgorithm,
    DatabaseType,
    StorageType,
)

schedule_app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")
console = Console()


@schedule_app.command("add")
def schedule_add(
        name: str = typer.Option(..., "--name", "-n", help="Name for this scheduled backup."),
        cron: str = typer.Option(
            ..., "--cron", help="Cron expression (e.g., '0 2 * * *' for daily at 2 AM)."
        ),
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
        compression: CompressionAlgorithm = typer.Option(
            CompressionAlgorithm.ZSTD, "--compression", "-c", help="Compression algorithm."
        ),
        storage: StorageType = typer.Option(
            StorageType.LOCAL, "--storage", "-s", help="Storage backend."
        ),
        output_dir: str = typer.Option(
            "./backups", "--output-dir", "-o", help="Local output directory."
        ),
        s3_bucket: str | None = typer.Option(None, "--s3-bucket", help="S3 bucket."),
        s3_prefix: str = typer.Option("db-vault/", "--s3-prefix", help="S3 prefix."),
        s3_region: str = typer.Option("us-east-1", "--s3-region", help="AWS region."),
        slack_webhook: str | None = typer.Option(
            None, "--slack-webhook", help="Slack webhook URL."
        ),
) -> None:
    """Add a new scheduled backup job.

    Example:
        db-vault schedule add --name daily-postgres --cron "0 2 * * *" \\
            --db-type postgres -H localhost -u admin -d mydb
    """
    from db_vault.scheduler.scheduler import BackupScheduler

    scheduler = BackupScheduler()

    # Store backup parameters as kwargs for the scheduled job
    job_kwargs = {
        "db_type": db_type.value,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
        "ssl": ssl,
        "backup_type": backup_type.value,
        "compression": compression.value,
        "storage": storage.value,
        "output_dir": output_dir,
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "s3_region": s3_region,
        "slack_webhook": slack_webhook,
    }

    scheduler.add_job(
        job_id=name,
        func=_run_scheduled_backup,
        cron_expression=cron,
        kwargs=job_kwargs,
        name=name,
    )

    console.print(f"[green]✓[/green] Scheduled backup '{name}' added: {cron}")
    console.print("  Run [bold]db-vault schedule start[/bold] to activate the scheduler.")


@schedule_app.command("list")
def schedule_list() -> None:
    """List all scheduled backup jobs."""
    from db_vault.scheduler.scheduler import BackupScheduler

    scheduler = BackupScheduler()
    jobs = scheduler.list_jobs()

    if not jobs:
        console.print("[yellow]No scheduled jobs found.[/yellow]")
        return

    table = Table(title="Scheduled Backup Jobs", show_lines=True)
    table.add_column("ID", style="cyan")
    table.add_column("Name", style="blue")
    table.add_column("Schedule", style="green")
    table.add_column("Next Run", style="magenta")

    for job in jobs:
        table.add_row(job["id"], job["name"], job["trigger"], job["next_run"])

    console.print(table)


@schedule_app.command("remove")
def schedule_remove(
        name: str = typer.Argument(help="Job name/ID to remove."),
) -> None:
    """Remove a scheduled backup job."""
    from db_vault.scheduler.scheduler import BackupScheduler

    scheduler = BackupScheduler()
    try:
        scheduler.remove_job(name)
        console.print(f"[green]✓[/green] Removed scheduled job: {name}")
    except Exception as exc:
        console.print(f"[red]✗ Failed to remove job: {exc}[/red]")
        raise typer.Exit(code=1)


@schedule_app.command("start")
def schedule_start() -> None:
    """Start the scheduler daemon (blocking).

    Runs all scheduled backup jobs. Press Ctrl+C to stop.
    Use this in a Docker container, systemd service, or terminal multiplexer.
    """
    from db_vault.scheduler.scheduler import BackupScheduler

    scheduler = BackupScheduler()
    jobs = scheduler.list_jobs()

    if not jobs:
        console.print(
            "[yellow]No scheduled jobs. Add jobs first with 'db-vault schedule add'.[/yellow]"
        )
        raise typer.Exit()

    console.print(f"[bold blue]Starting scheduler with {len(jobs)} job(s)...[/bold blue]")
    console.print("Press [bold]Ctrl+C[/bold] to stop.\n")

    for job in jobs:
        console.print(f"  • {job['name']} → next run: {job['next_run']}")
    console.print()

    scheduler.start()


def _run_scheduled_backup(**kwargs: str | int | bool | None) -> None:
    """Execute a backup as a scheduled job.

    This function is called by APScheduler with the stored kwargs.
    """

    # Invoke the backup_run with appropriate arguments
    # Since APScheduler calls this in a thread, we need to handle exceptions
    from db_vault.logging import get_logger

    log = get_logger("scheduler")

    try:
        import time
        from datetime import datetime
        from pathlib import Path

        from db_vault.compression.compressor import compress_file
        from db_vault.core.models import (
            BackupMetadata,
            BackupStatus,
            CompressionAlgorithm,
            DatabaseConfig,
            DatabaseType,
            StorageConfig,
        )
        from db_vault.engines import get_engine
        from db_vault.storage import get_storage

        db_config = DatabaseConfig(
            type=DatabaseType(kwargs["db_type"]),
            host=str(kwargs.get("host", "localhost")),
            port=int(kwargs["port"]) if kwargs.get("port") else None,
            username=str(kwargs["username"]) if kwargs.get("username") else None,
            password=str(kwargs["password"]) if kwargs.get("password") else None,
            database=str(kwargs.get("database", "")),
            ssl=bool(kwargs.get("ssl", False)),
        )

        backup_type_val = str(kwargs.get("backup_type", "full"))
        compression_val = str(kwargs.get("compression", "zstd"))
        storage_val = str(kwargs.get("storage", "local"))
        output_dir = Path(str(kwargs.get("output_dir", "./backups")))

        storage_config = StorageConfig(
            type=StorageType(storage_val),
            local_path=output_dir,
            s3_bucket=str(kwargs["s3_bucket"]) if kwargs.get("s3_bucket") else None,
            s3_prefix=str(kwargs.get("s3_prefix", "db-vault/")),
        )

        engine = get_engine(db_config)
        start = time.time()

        # Run backup
        raw_file = engine.backup(
            output_dir=output_dir,
            backup_type=BackupType(backup_type_val),
        )

        # Compress
        comp_algo = CompressionAlgorithm(compression_val)
        if comp_algo != CompressionAlgorithm.NONE:
            compressed = compress_file(raw_file, algorithm=comp_algo)
            if compressed != raw_file:
                raw_file.unlink(missing_ok=True)
                raw_file = compressed

        # Upload
        storage_backend = get_storage(storage_config)
        remote_key = (
            f"{db_config.type.value}/{db_config.database or 'default'}/"
            f"{datetime.utcnow().strftime('%Y-%m-%d')}/{raw_file.name}"
        )
        storage_backend.upload(raw_file, remote_key)

        elapsed = time.time() - start
        log.info(
            "scheduled_backup_complete",
            database=db_config.database,
            duration=f"{elapsed:.1f}s",
            file=raw_file.name,
        )

        # Slack notification
        if kwargs.get("slack_webhook"):
            try:
                from db_vault.notifications.slack import SlackNotifier

                metadata = BackupMetadata(
                    database_name=db_config.database or db_config.host,
                    database_type=db_config.type,
                    backup_type=BackupType(backup_type_val),
                    file_name=raw_file.name,
                    file_path=remote_key,
                    compressed_size=raw_file.stat().st_size,
                    duration_seconds=elapsed,
                    status=BackupStatus.COMPLETED,
                    storage_type=storage_config.type,
                )
                notifier = SlackNotifier(str(kwargs["slack_webhook"]))
                notifier.notify_success(metadata)
            except Exception as notify_exc:
                log.warning("scheduled_notification_failed", error=str(notify_exc))

    except Exception as exc:
        log.error("scheduled_backup_failed", error=str(exc))
        raise
