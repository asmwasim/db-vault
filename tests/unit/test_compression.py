"""Tests for compression module."""

from __future__ import annotations

from pathlib import Path

import pytest

from db_vault.compression.compressor import (
    compress_file,
    compute_checksum,
    decompress_file,
    detect_algorithm,
    get_extension,
)
from db_vault.core.models import CompressionAlgorithm


class TestGetExtension:
    def test_zstd(self) -> None:
        assert get_extension(CompressionAlgorithm.ZSTD) == ".zst"

    def test_gzip(self) -> None:
        assert get_extension(CompressionAlgorithm.GZIP) == ".gz"

    def test_lz4(self) -> None:
        assert get_extension(CompressionAlgorithm.LZ4) == ".lz4"

    def test_none(self) -> None:
        assert get_extension(CompressionAlgorithm.NONE) == ""


class TestDetectAlgorithm:
    def test_zstd(self) -> None:
        assert detect_algorithm(Path("backup.dump.zst")) == CompressionAlgorithm.ZSTD

    def test_gzip(self) -> None:
        assert detect_algorithm(Path("backup.sql.gz")) == CompressionAlgorithm.GZIP

    def test_lz4(self) -> None:
        assert detect_algorithm(Path("dump.lz4")) == CompressionAlgorithm.LZ4

    def test_unknown(self) -> None:
        assert detect_algorithm(Path("backup.dump")) == CompressionAlgorithm.NONE

    def test_txt(self) -> None:
        assert detect_algorithm(Path("data.txt")) == CompressionAlgorithm.NONE


class TestCompressDecompress:
    @pytest.mark.parametrize("algo", [
        CompressionAlgorithm.ZSTD,
        CompressionAlgorithm.GZIP,
        CompressionAlgorithm.LZ4,
    ])
    def test_roundtrip(self, sample_file: Path, tmp_dir: Path, algo: CompressionAlgorithm) -> None:
        """Compress then decompress and verify the content is identical."""
        original_data = sample_file.read_bytes()
        original_size = len(original_data)

        # Compress
        compressed = compress_file(sample_file, algorithm=algo, level=3)
        assert compressed.exists()
        assert compressed.stat().st_size > 0
        assert compressed.stat().st_size < original_size  # Should be smaller

        # Decompress
        decompressed = decompress_file(compressed)
        assert decompressed.exists()
        assert decompressed.read_bytes() == original_data

    def test_none_algorithm(self, sample_file: Path) -> None:
        """No compression should return the original file."""
        result = compress_file(sample_file, algorithm=CompressionAlgorithm.NONE)
        assert result == sample_file

    def test_none_decompress(self, sample_file: Path) -> None:
        """Decompressing an uncompressed file should return it as-is."""
        result = decompress_file(sample_file)
        assert result == sample_file

    def test_explicit_output_path(self, sample_file: Path, tmp_dir: Path) -> None:
        """Test compression with an explicit output path."""
        out = tmp_dir / "custom_output.zst"
        result = compress_file(sample_file, algorithm=CompressionAlgorithm.ZSTD, output_path=out)
        assert result == out
        assert out.exists()


class TestChecksum:
    def test_compute_checksum(self, sample_file: Path) -> None:
        """Checksum should be deterministic."""
        hash1 = compute_checksum(sample_file)
        hash2 = compute_checksum(sample_file)
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex digest

    def test_different_content_different_checksum(self, tmp_dir: Path) -> None:
        """Different files should produce different checksums."""
        f1 = tmp_dir / "a.txt"
        f2 = tmp_dir / "b.txt"
        f1.write_text("hello")
        f2.write_text("world")
        assert compute_checksum(f1) != compute_checksum(f2)
