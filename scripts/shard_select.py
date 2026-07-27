#!/usr/bin/env python3
"""Select one night's slice of source files for the rotating mutation sweep.

The full tree generates ~45k mutants, far more than one nightly window can test,
so each night mutates ``1/NUM_SHARDS`` of the files and the whole tree is covered
every ``NUM_SHARDS`` nights.

Files are assigned to a shard by a hash of their path, not by their index in the
sorted file list. Index assignment reshuffles every file's shard whenever a file
is added or removed, which makes a recorded per-shard kill rate
(``.mutation-shards.json``) meaningless the moment the tree changes. Hashing
moves only the added or removed file. See portolan-sdi/portolan-cli#612.

Assignment is BLAKE2b of the path under the package directory, modulo the shard
count: stable across runs, machines, and Python versions (unlike ``hash()``,
which is salted per process). A cryptographic digest rather than CRC-32 because
these paths share long prefixes — ``portolan_cli/extract/...`` — and CRC-32's
weak avalanche clustered them, leaving one shard with 17 files and another with
1. The key drops everything above the package, so where the checkout lives
cannot change which shard a file belongs to.

Paths are printed relative to the package for the same reason: they feed
``scripts/mutant_globs.py``, which dots a path into a module name, and an
absolute path would dot the filesystem prefix into a glob matching no mutant.

Usage:
    python scripts/shard_select.py --root portolan_cli --num-shards 25 --shard 8

Exit codes: 0 = selected paths written to stdout, one per line (possibly none);
1 = the root holds no Python files at all, which means a broken checkout rather
than an empty shard.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from hashlib import blake2b
from pathlib import Path, PurePosixPath


def shard_of(path: str | PurePosixPath, num_shards: int) -> int:
    """Return the shard index owning ``path``.

    Args:
        path: Source path, compared as POSIX text so Windows and Linux agree.
        num_shards: Total number of shards; must be positive.

    Raises:
        ValueError: ``num_shards`` is not positive.
    """
    if num_shards <= 0:
        raise ValueError(f"num_shards must be positive, got {num_shards}")
    posix = str(path).replace("\\", "/")
    digest = blake2b(posix.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % num_shards


def shard_key(root: Path, path: Path) -> str:
    """Return the hashed key for ``path``: its location under ``root``'s name.

    The key must not vary with how the caller spelled ``--root``. Hashing the
    path as walked made assignment depend on the working directory, so an
    absolute root (a tmp dir, a differently-checked-out CI runner) shuffled every
    file into a different shard and voided the recorded per-shard baselines.
    ``portolan_cli/sync/upload.py`` is the key whether the sweep ran from the
    repo root or anywhere else.
    """
    return f"{root.name}/{path.relative_to(root).as_posix()}"


def select(paths: Sequence[str], num_shards: int, shard: int) -> list[str]:
    """Return the subset of ``paths`` belonging to ``shard``, sorted.

    Raises:
        ValueError: ``shard`` is outside ``range(num_shards)``, or ``num_shards``
            is not positive.
    """
    if num_shards <= 0:
        raise ValueError(f"num_shards must be positive, got {num_shards}")
    if not 0 <= shard < num_shards:
        raise ValueError(f"shard {shard} is outside 0..{num_shards - 1}")
    return sorted(p for p in paths if shard_of(p, num_shards) == shard)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select a mutation-sweep shard.")
    parser.add_argument("--root", required=True, type=Path, help="package directory to walk")
    parser.add_argument("--num-shards", required=True, type=int, help="total shard count")
    parser.add_argument("--shard", required=True, type=int, help="shard index to emit")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _parse_args(argv)

    # Emit the root-relative key, not the path as walked. The caller feeds these
    # to mutant_globs.py, which turns a path into a dotted module name; an
    # absolute path would dot the whole filesystem prefix into the glob and match
    # no mutant at all.
    keys = [shard_key(args.root, p) for p in sorted(args.root.rglob("*.py"))]
    if not keys:
        print(
            f"::error::No Python files under {args.root} — refusing to report an "
            "empty sweep as a covered shard. See portolan-sdi/portolan-cli#612.",
            file=sys.stderr,
        )
        return 1

    try:
        selected = select(keys, args.num_shards, args.shard)
    except ValueError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1

    for key in selected:
        print(key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
