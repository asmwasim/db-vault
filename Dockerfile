# ──────────────────────────────────────────────────────────
# db-vault — Database Backup & Restore CLI
# Multi-stage build for minimal image size
# ──────────────────────────────────────────────────────────

# ─── Stage 1: Builder ────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN pip install --no-cache-dir hatchling

# Copy project files
COPY pyproject.toml README.md LICENSE ./
COPY src/ src/

# Build wheel
RUN pip wheel --no-deps --wheel-dir /wheels .

# ─── Stage 2: Runtime ────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL maintainer="db-vault contributors"
LABEL description="CLI utility for backing up and restoring databases"

# Install native database client tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    default-mysql-client \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install MongoDB Database Tools
RUN curl -fsSL https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.10.0.deb \
    -o /tmp/mongo-tools.deb \
    && dpkg -i /tmp/mongo-tools.deb \
    && rm /tmp/mongo-tools.deb \
    || echo "MongoDB tools installation skipped (non-x86_64 arch)"

# Install the wheel from builder stage
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Create a non-root user
RUN useradd --create-home --shell /bin/bash dbvault
USER dbvault
WORKDIR /home/dbvault

# Create default directories
RUN mkdir -p /home/dbvault/backups /home/dbvault/.config/db-vault

# Default volume for backups
VOLUME ["/home/dbvault/backups"]

ENTRYPOINT ["db-vault"]
CMD ["--help"]
