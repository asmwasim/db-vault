# db-vault

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-91%20passed-brightgreen.svg)](#development)

A powerful CLI utility for backing up and restoring databases. Supports MySQL, PostgreSQL, MongoDB, and SQLite with
automatic scheduling, compression, cloud storage, and notifications.

---

## Features

- **Multi-DBMS Support** - PostgreSQL, MySQL, MongoDB, SQLite
- **Backup Types** - Full backups (incremental/differential documented as future work)
- **Compression** - zstd (default), gzip, lz4 with streaming support for large databases
- **Storage Backends** - Local filesystem and AWS S3 (with multipart upload)
- **Scheduling** - Built-in cron-based scheduler with persistent job store
- **Restore** - Full and selective (per-table/collection) restore with dry-run support
- **Notifications** - Slack webhook notifications with rich Block Kit formatting
- **Logging** - Structured logging (console + JSON) with sensitive field redaction
- **Security** - SHA-256 checksums, S3 server-side encryption, config file permissions

## Installation

### From source (pip)

```bash
# Clone the repository
git clone https://github.com/asmwasim/db-vault.git
cd db-vault

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Install in development mode
pip install -e ".[dev]"

# Verify installation
db-vault --version
```

### Docker

```bash
# Build the image
docker build -t db-vault .

# Run
docker run --rm db-vault --help

# Backup a PostgreSQL database
docker run --rm \
  --network host \
  -v $(pwd)/backups:/home/dbvault/backups \
  db-vault backup run \
    --db-type postgres \
    --host localhost \
    --username admin \
    --database mydb
```

## Quick Start

### 1. Test Database Connection

```bash
# SQLite
db-vault test-connection --db-type sqlite --database ./my.db

# PostgreSQL
db-vault test-connection \
  --db-type postgres \
  --host localhost \
  --port 5432 \
  --username admin \
  --database mydb

# MySQL
db-vault test-connection \
  --db-type mysql \
  --host localhost \
  --username root \
  --database mydb

# MongoDB
db-vault test-connection \
  --db-type mongodb \
  --host localhost \
  --username admin \
  --database mydb
```

### 2. Run a Backup

```bash
# SQLite — simplest case
db-vault backup run \
  --db-type sqlite \
  --database ./my.db \
  --output-dir ./backups

# PostgreSQL with gzip compression
db-vault backup run \
  --db-type postgres \
  --host localhost \
  --username admin \
  --database production \
  --compression gzip \
  --output-dir ./backups

# MySQL to S3
db-vault backup run \
  --db-type mysql \
  --host db.example.com \
  --username backup_user \
  --password secret \
  --database shop \
  --storage s3 \
  --s3-bucket my-backups \
  --s3-region us-east-1

# Backup specific tables only
db-vault backup run \
  --db-type postgres \
  --host localhost \
  --username admin \
  --database mydb \
  --tables users,orders
```

### 3. List Backups

```bash
# Local backups
db-vault backup list --output-dir ./backups

# S3 backups
db-vault backup list --storage s3 --s3-bucket my-backups

# Backup history with metadata
db-vault backup history
```

### 4. Restore from Backup

```bash
# Restore SQLite
db-vault restore run \
  --db-type sqlite \
  --database ./restored.db \
  --file ./backups/sqlite_my_20260225_120000.db.zst

# Restore PostgreSQL
db-vault restore run \
  --db-type postgres \
  --host localhost \
  --username admin \
  --database mydb_restored \
  --file ./backups/postgres_mydb_20260225_120000.dump.zst

# Selective restore (specific tables)
db-vault restore run \
  --db-type postgres \
  --host localhost \
  --username admin \
  --database mydb \
  --file ./backups/postgres_mydb_20260225_120000.dump \
  --tables users,orders

# Dry run — preview without executing
db-vault restore run \
  --db-type sqlite \
  --database ./my.db \
  --file ./backups/backup.db \
  --dry-run
```

### 5. Schedule Automatic Backups

```bash
# Add a daily backup at 2 AM
db-vault schedule add \
  --name daily-postgres \
  --cron "0 2 * * *" \
  --db-type postgres \
  --host localhost \
  --username admin \
  --database production \
  --storage s3 \
  --s3-bucket my-backups

# Add an hourly backup
db-vault schedule add \
  --name hourly-sqlite \
  --cron "0 * * * *" \
  --db-type sqlite \
  --database ./app.db

# List scheduled jobs
db-vault schedule list

# Start the scheduler (runs in foreground)
db-vault schedule start

# Remove a job
db-vault schedule remove daily-postgres
```

### 6. Notifications

Send Slack notifications on backup completion:

```bash
db-vault backup run \
  --db-type postgres \
  --host localhost \
  --username admin \
  --database mydb \
  --slack-webhook https://hooks.slack.com/services/T00/B00/xxx
```

Or set via environment variable:

```bash
export DB_VAULT_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00/B00/xxx
```

## Configuration

### Interactive Setup

```bash
db-vault config init
```

This creates a TOML configuration file at:

- **macOS:** `~/Library/Application Support/db-vault/config.toml`
- **Linux:** `~/.config/db-vault/config.toml`

### Configuration File Example

```toml
[databases.production]
type = "postgres"
host = "db.example.com"
port = 5432
username = "backup_user"
password = "secret"
database = "production"
ssl = true

[databases.analytics]
type = "mysql"
host = "mysql.example.com"
port = 3306
username = "readonly"
database = "analytics"

[storage]
type = "s3"
s3_bucket = "company-backups"
s3_prefix = "db-vault/"
s3_region = "us-east-1"

[compression]
algorithm = "zstd"
level = 3

[notification]
slack_webhook_url = "https://hooks.slack.com/services/..."
notify_on_success = true
notify_on_failure = true

[logging]
level = "INFO"
format = "console"
```

### Using Profiles

```bash
# Use a named profile from config
db-vault backup run --profile production
db-vault restore run --profile production --file backup.dump
```

### Environment Variables

All settings can be overridden via environment variables with the `DB_VAULT_` prefix:

| Variable                      | Description                                   |
|-------------------------------|-----------------------------------------------|
| `DB_VAULT_DB_TYPE`            | Database type (postgres/mysql/mongodb/sqlite) |
| `DB_VAULT_DB_HOST`            | Database host                                 |
| `DB_VAULT_DB_PORT`            | Database port                                 |
| `DB_VAULT_DB_USERNAME`        | Database username                             |
| `DB_VAULT_DB_PASSWORD`        | Database password                             |
| `DB_VAULT_DB_NAME`            | Database name                                 |
| `DB_VAULT_STORAGE_TYPE`       | Storage backend (local/s3)                    |
| `DB_VAULT_STORAGE_LOCAL_PATH` | Local backup directory                        |
| `DB_VAULT_S3_BUCKET`          | S3 bucket name                                |
| `DB_VAULT_S3_PREFIX`          | S3 key prefix                                 |
| `DB_VAULT_S3_REGION`          | AWS region                                    |
| `DB_VAULT_COMPRESSION`        | Compression algorithm (zstd/gzip/lz4/none)    |
| `DB_VAULT_COMPRESSION_LEVEL`  | Compression level (1-22)                      |
| `DB_VAULT_SLACK_WEBHOOK_URL`  | Slack webhook URL                             |
| `DB_VAULT_LOG_LEVEL`          | Log level (DEBUG/INFO/WARNING/ERROR)          |
| `DB_VAULT_LOG_FORMAT`         | Log format (console/json)                     |

## CLI Reference

```
db-vault [OPTIONS] COMMAND [ARGS]

Commands:
  backup          Backup operations
    run           Execute a database backup
    list          List available backups
    history       Show backup history
  restore         Restore operations
    run           Restore from a backup file
  schedule        Manage scheduled backups
    add           Add a scheduled backup job
    list          List scheduled jobs
    remove        Remove a scheduled job
    start         Start the scheduler daemon
  config          Configuration management
    init          Interactive config setup
    show          Display current configuration
    path          Show config/data paths
  test-connection Test database connectivity

Options:
  -V, --version   Show version
  -v, --verbose   Enable debug logging
  --log-json      Output logs as JSON
  --help          Show help
```

## Prerequisites

### Native Database Tools

db-vault uses native database tools for backup/restore:

| DBMS       | Required Tool               | Install Command                                                               |
|------------|-----------------------------|-------------------------------------------------------------------------------|
| PostgreSQL | `pg_dump`, `pg_restore`     | `brew install libpq` / `apt install postgresql-client`                        |
| MySQL      | `mysqldump`, `mysql`        | `brew install mysql-client` / `apt install default-mysql-client`              |
| MongoDB    | `mongodump`, `mongorestore` | [MongoDB Database Tools](https://www.mongodb.com/try/download/database-tools) |
| SQLite     | (none — uses Python stdlib) | Built-in                                                                      |

### AWS S3

For S3 storage, configure AWS credentials using any standard method:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM role (EC2/ECS/Lambda)

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=db_vault --cov-report=html

# Lint
ruff check src/ tests/

# Type check
mypy src/

# Start test databases
docker compose up -d

# Run integration tests
pytest tests/integration/
```

## Architecture

```
src/db_vault/
├── cli/            # Typer CLI commands (backup, restore, schedule, config)
├── core/           # Config, models (Pydantic), exceptions
├── engines/        # Database engines (postgres, mysql, mongodb, sqlite)
├── storage/        # Storage backends (local filesystem, AWS S3)
├── compression/    # Streaming compression (zstd, gzip, lz4)
├── scheduler/      # APScheduler-based backup scheduling
├── notifications/  # Slack webhook notifications
└── logging.py      # structlog configuration
```

## License

MIT - see [LICENSE](LICENSE).

---

**GitHub:** [https://github.com/asmwasim/db-vault](https://github.com/asmwasim/db-vault)
