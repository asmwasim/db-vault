"""Tests for the CLI interface."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from db_vault.cli.app import app

runner = CliRunner()


class TestMainApp:
    def test_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "db-vault" in result.output.lower() or "backup" in result.output.lower()

    def test_version(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_no_args(self) -> None:
        result = runner.invoke(app)
        # Typer returns exit code 2 when showing help via no_args_is_help
        assert result.exit_code == 2


class TestBackupSubcommand:
    def test_backup_help(self) -> None:
        result = runner.invoke(app, ["backup", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output.lower()
        assert "list" in result.output.lower()

    def test_backup_run_help(self) -> None:
        result = runner.invoke(app, ["backup", "run", "--help"])
        assert result.exit_code == 0
        assert "--db-type" in result.output

    def test_backup_sqlite(self, sqlite_db: Path, tmp_path: Path) -> None:
        """End-to-end test: backup a SQLite database."""
        output_dir = tmp_path / "cli_backup"
        result = runner.invoke(app, [
            "backup", "run",
            "--db-type", "sqlite",
            "--database", str(sqlite_db),
            "--storage", "local",
            "--output-dir", str(output_dir),
            "--compression", "gzip",
        ])
        assert result.exit_code == 0, result.output
        assert "Backup completed" in result.output

        # Verify backup file exists
        backup_files = list(output_dir.rglob("*.gz"))
        assert len(backup_files) >= 1


class TestRestoreSubcommand:
    def test_restore_help(self) -> None:
        result = runner.invoke(app, ["restore", "--help"])
        assert result.exit_code == 0

    def test_restore_run_help(self) -> None:
        result = runner.invoke(app, ["restore", "run", "--help"])
        assert result.exit_code == 0
        assert "--file" in result.output


class TestScheduleSubcommand:
    def test_schedule_help(self) -> None:
        result = runner.invoke(app, ["schedule", "--help"])
        assert result.exit_code == 0
        assert "add" in result.output.lower()
        assert "list" in result.output.lower()


class TestConfigSubcommand:
    def test_config_help(self) -> None:
        result = runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0
        assert "init" in result.output.lower()
        assert "show" in result.output.lower()

    def test_config_path(self) -> None:
        result = runner.invoke(app, ["config", "path"])
        assert result.exit_code == 0
        assert "Config dir" in result.output


class TestTestConnection:
    def test_connection_help(self) -> None:
        result = runner.invoke(app, ["test-connection", "--help"])
        assert result.exit_code == 0
        assert "--db-type" in result.output

    def test_sqlite_connection(self, sqlite_db: Path) -> None:
        result = runner.invoke(app, [
            "test-connection",
            "--db-type", "sqlite",
            "--database", str(sqlite_db),
        ])
        assert result.exit_code == 0
        assert "Connection successful" in result.output
