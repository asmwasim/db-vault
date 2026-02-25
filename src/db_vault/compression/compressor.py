"""Compression and decompression utilities for backup files.

Supports streaming compression to avoid loading entire backups into memory.
Default: zstd level 3 (best speed-to-ratio trade-off).
"""

from __future__ import annotations

import gzip
import hashlib
import shutil
from pathlib import Path
from typing import BinaryIO

import lz4.frame
import zstandard as zstd

from db_vault.core.exceptions import CompressionError
from db_vault.core.models import CompressionAlgorithm
from db_vault.logging import get_logger

log = get_logger(__name__)

# File extensions used for each algorithm
EXTENSIONS: dict[CompressionAlgorithm, str] = {
    CompressionAlgorithm.ZSTD: ".zst",
    CompressionAlgorithm.GZIP: ".gz",
    CompressionAlgorithm.LZ4: ".lz4",
    CompressionAlgorithm.NONE: "",
}

# Buffer size for streaming I/O (256 KB)
_CHUNK_SIZE = 256 * 1024


def get_extension(algorithm: CompressionAlgorithm) -> str:
    """Return the file extension for the given compression algorithm."""
    return EXTENSIONS[algorithm]


def detect_algorithm(file_path: Path) -> CompressionAlgorithm:
    """Detect compression algorithm from file extension."""
    suffix = file_path.suffix.lower()
    for algo, ext in EXTENSIONS.items():
        if ext and suffix == ext:
            return algo
    return CompressionAlgorithm.NONE


def compress_file(
        input_path: Path,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.ZSTD,
        level: int = 3,
        output_path: Path | None = None,
) -> Path:
    """Compress a file using the specified algorithm.

    Args:
        input_path: Path to the uncompressed source file.
        algorithm: Compression algorithm to use.
        level: Compression level (1-22, higher = better ratio, slower).
        output_path: Explicit output path. If None, appends the algorithm extension.

    Returns:
        Path to the compressed file.
    """
    if algorithm == CompressionAlgorithm.NONE:
        if output_path and output_path != input_path:
            shutil.copy2(input_path, output_path)
            return output_path
        return input_path

    if output_path is None:
        output_path = input_path.with_suffix(input_path.suffix + get_extension(algorithm))

    log.info(
        "compressing_file",
        input=str(input_path),
        output=str(output_path),
        algorithm=algorithm.value,
        level=level,
    )

    try:
        with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
            _compress_stream(fin, fout, algorithm, level)
    except Exception as exc:
        # Clean up partial output on failure
        output_path.unlink(missing_ok=True)
        raise CompressionError(f"Compression failed: {exc}") from exc

    original = input_path.stat().st_size
    compressed = output_path.stat().st_size
    ratio = compressed / original if original > 0 else 0
    log.info(
        "compression_complete",
        original_bytes=original,
        compressed_bytes=compressed,
        ratio=f"{ratio:.2%}",
    )
    return output_path


def decompress_file(
        input_path: Path,
        output_path: Path | None = None,
) -> Path:
    """Decompress a file, auto-detecting the algorithm from the extension.

    Args:
        input_path: Path to the compressed file.
        output_path: Explicit output path. If None, strips the compression extension.

    Returns:
        Path to the decompressed file.
    """
    algorithm = detect_algorithm(input_path)
    if algorithm == CompressionAlgorithm.NONE:
        return input_path

    if output_path is None:
        # Strip compression extension
        output_path = input_path.with_suffix("")

    log.info(
        "decompressing_file",
        input=str(input_path),
        output=str(output_path),
        algorithm=algorithm.value,
    )

    try:
        with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
            _decompress_stream(fin, fout, algorithm)
    except Exception as exc:
        output_path.unlink(missing_ok=True)
        raise CompressionError(f"Decompression failed: {exc}") from exc

    log.info("decompression_complete", output_bytes=output_path.stat().st_size)
    return output_path


def compute_checksum(file_path: Path) -> str:
    """Compute SHA-256 checksum of a file (streaming)."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# ──────────────────── Internal Stream Helpers ────────────


def _compress_stream(
        fin: BinaryIO, fout: BinaryIO, algorithm: CompressionAlgorithm, level: int
) -> None:
    """Compress from one stream to another."""
    if algorithm == CompressionAlgorithm.ZSTD:
        compressor = zstd.ZstdCompressor(level=level)
        with compressor.stream_writer(fout, closefd=False) as writer:
            for chunk in iter(lambda: fin.read(_CHUNK_SIZE), b""):
                writer.write(chunk)

    elif algorithm == CompressionAlgorithm.GZIP:
        with gzip.GzipFile(fileobj=fout, mode="wb", compresslevel=min(level, 9)) as gz:
            for chunk in iter(lambda: fin.read(_CHUNK_SIZE), b""):
                gz.write(chunk)

    elif algorithm == CompressionAlgorithm.LZ4:
        with lz4.frame.open(fout, mode="wb", compression_level=min(level, 16)) as lz:
            for chunk in iter(lambda: fin.read(_CHUNK_SIZE), b""):
                lz.write(chunk)

    else:
        raise CompressionError(f"Unsupported algorithm: {algorithm}")


def _decompress_stream(
        fin: BinaryIO, fout: BinaryIO, algorithm: CompressionAlgorithm
) -> None:
    """Decompress from one stream to another."""
    if algorithm == CompressionAlgorithm.ZSTD:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(fin, closefd=False) as reader:
            for chunk in iter(lambda: reader.read(_CHUNK_SIZE), b""):
                fout.write(chunk)

    elif algorithm == CompressionAlgorithm.GZIP:
        with gzip.GzipFile(fileobj=fin, mode="rb") as gz:
            for chunk in iter(lambda: gz.read(_CHUNK_SIZE), b""):
                fout.write(chunk)

    elif algorithm == CompressionAlgorithm.LZ4:
        with lz4.frame.open(fin, mode="rb") as lz:
            for chunk in iter(lambda: lz.read(_CHUNK_SIZE), b""):
                fout.write(chunk)

    else:
        raise CompressionError(f"Unsupported algorithm: {algorithm}")
