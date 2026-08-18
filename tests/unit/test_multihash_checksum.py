"""Unit tests for multihash encoding of ``file:checksum`` (issue #654).

The STAC file extension defines ``file:checksum`` as a hex-encoded multihash,
not a bare digest; rashid's PTL-AST-004 rejects anything else.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from portolan_cli.sync.checksums import file_fields, file_fields_from, multihash_sha256

pytestmark = pytest.mark.unit


class TestMultihashSha256:
    def test_prefixes_the_sha2_256_code_and_length(self) -> None:
        digest = hashlib.sha256(b"portolan").hexdigest()

        encoded = multihash_sha256(digest)

        # 0x12 = sha2-256, 0x20 = 32-byte digest.
        assert encoded == f"1220{digest}"

    def test_decodes_as_a_well_formed_multihash(self) -> None:
        from rashid.api import decode_multihash

        decoded = decode_multihash(multihash_sha256(hashlib.sha256(b"x").hexdigest()))

        assert decoded is not None
        code, digest = decoded
        assert code == 0x12
        assert len(digest) == 32

    def test_rejects_a_non_sha256_digest(self) -> None:
        with pytest.raises(ValueError, match="64-character"):
            multihash_sha256("deadbeef")


class TestFileFields:
    """The paired ``file:size``/``file:checksum`` fields the file extension defines.

    Four call sites used to inline the same two lines. These cover the shared
    helpers they now share.
    """

    def test_reads_both_fields_from_a_file(self, tmp_path: Path) -> None:
        payload = b"portolan file fields"
        path = tmp_path / "asset.bin"
        path.write_bytes(payload)

        fields = file_fields(path)

        assert fields == {
            "file:size": len(payload),
            "file:checksum": f"1220{hashlib.sha256(payload).hexdigest()}",
        }

    def test_encodes_a_digest_that_was_measured_elsewhere(self) -> None:
        # FileGDB assets are directories: preparation.py computes the digest and
        # the size with compute_dir_checksum/compute_dir_size, then hands both here.
        digest = hashlib.sha256(b"filegdb").hexdigest()

        assert file_fields_from(digest, 4096) == {
            "file:size": 4096,
            "file:checksum": f"1220{digest}",
        }

    def test_size_is_an_int_so_ptl_ast_003_accepts_it(self, tmp_path: Path) -> None:
        # PTL-AST-003 rejects a bool or a non-int, and bool is a subclass of int.
        path = tmp_path / "asset.bin"
        path.write_bytes(b"xy")

        size = file_fields(path)["file:size"]

        assert type(size) is int
        assert size == 2

    def test_rejects_a_digest_that_is_not_sha256(self) -> None:
        with pytest.raises(ValueError, match="64-character"):
            file_fields_from("deadbeef", 1)

    def test_raises_when_the_file_is_missing(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            file_fields(tmp_path / "absent.bin")

    def test_rejects_a_directory(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            file_fields(tmp_path)
