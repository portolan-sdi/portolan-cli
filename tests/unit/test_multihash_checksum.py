"""Unit tests for multihash encoding of ``file:checksum`` (issue #654).

The STAC file extension defines ``file:checksum`` as a hex-encoded multihash,
not a bare digest; rashid's PTL-AST-004 rejects anything else.
"""

from __future__ import annotations

import hashlib

import pytest

from portolan_cli.sync.checksums import multihash_sha256

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
