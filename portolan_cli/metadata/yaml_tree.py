"""Write and validate the .portolan/metadata.yaml files of a catalog tree.

``metadata init`` and ``metadata validate`` call these functions. Each one
walks the catalog and collection directories below a start directory, or acts
on the start directory alone, and returns a result whose ``to_dict()`` is the
JSON envelope ``data`` of its command.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.readme import resolve_catalog_dir

if TYPE_CHECKING:
    from collections.abc import Iterator

_METADATA_FILE = Path(".portolan") / "metadata.yaml"


def _is_stac_dir(dirpath: Path) -> bool:
    return (dirpath / "catalog.json").exists() or (dirpath / "collection.json").exists()


def _stac_dirs(catalog_root: Path, base_dir: Path) -> Iterator[Path]:
    """Yield the catalog and collection directories at and below ``base_dir``.

    The walk skips symlinks, hidden directories, and item directories.

    Raises:
        PermissionError: When the walk cannot list a directory.
    """
    if base_dir == catalog_root or _is_stac_dir(base_dir):
        yield base_dir
    try:
        candidates = sorted(base_dir.rglob("*"))
    except PermissionError as err:
        raise PermissionError(f"Permission denied during scan: {err}") from err
    for dirpath in candidates:
        if not dirpath.is_dir() or dirpath.is_symlink():
            continue
        if any(part.startswith(".") for part in dirpath.relative_to(catalog_root).parts):
            continue
        if (dirpath / "item.json").exists():
            continue
        if _is_stac_dir(dirpath):
            yield dirpath


@dataclass(frozen=True)
class MetadataInitResult:
    """The templates a ``metadata init`` run wrote, as catalog-relative paths.

    Attributes:
        recursive: True when the run walked the tree.
        created: Paths the run wrote.
        skipped: Paths that already held a metadata.yaml.
        permission_errors: Paths the run could not write.
        overwritten: True when a single-path run replaced an existing file.
    """

    recursive: bool
    created: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    permission_errors: list[str] = field(default_factory=list)
    overwritten: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``metadata init``."""
        if not self.recursive:
            return {"path": self.created[0], "overwritten": self.overwritten}
        return {
            "mode": "recursive",
            "created": self.created,
            "skipped": self.skipped,
            "permission_errors": self.permission_errors,
            "count": len(self.created),
        }


def init_metadata_templates(
    catalog_root: Path,
    path: str | None = None,
    *,
    force: bool = False,
    recursive: bool = True,
) -> MetadataInitResult:
    """Write a metadata.yaml template at each catalog and collection level.

    Args:
        catalog_root: The catalog root.
        path: The start directory, relative to the root. None selects the root.
        force: If True, overwrite an existing metadata.yaml.
        recursive: If False, write only at the start directory.

    Returns:
        The paths the run wrote, skipped, and could not write.

    Raises:
        InputValidationError: When ``path`` escapes the catalog root.
        PathNotFoundError: When a recursive run starts at a missing path.
        NotADirectoryError: When a recursive run starts at a file.
        FileExistsError: When a single-path run finds a file and ``force`` is off.
        PermissionError: When the recursive walk cannot list a directory.
    """
    from portolan_cli.metadata_yaml import generate_metadata_template

    base_dir = resolve_catalog_dir(catalog_root, path, must_exist=recursive)

    if not recursive:
        metadata_file = base_dir / _METADATA_FILE
        metadata_file.parent.mkdir(parents=True, exist_ok=True)
        existed = metadata_file.exists()
        if existed and not force:
            raise FileExistsError(
                f"metadata.yaml already exists at {metadata_file}. Use --force to overwrite."
            )
        metadata_file.write_text(generate_metadata_template(), encoding="utf-8")
        rel_path = str(metadata_file.relative_to(catalog_root))
        return MetadataInitResult(recursive=False, created=[rel_path], overwritten=existed)

    result = MetadataInitResult(recursive=True)
    for dirpath in _stac_dirs(catalog_root, base_dir):
        metadata_file = dirpath / _METADATA_FILE
        rel_path = str(metadata_file.relative_to(catalog_root))
        try:
            if metadata_file.exists() and not force:
                result.skipped.append(rel_path)
                continue
            metadata_file.parent.mkdir(parents=True, exist_ok=True)
            metadata_file.write_text(generate_metadata_template(), encoding="utf-8")
        except PermissionError:
            result.permission_errors.append(rel_path)
            continue
        result.created.append(rel_path)
    return result


@dataclass(frozen=True)
class MetadataValidation:
    """The validation errors of the merged metadata at one level.

    Attributes:
        path: The level, relative to the catalog root. "." is the root.
        errors: The validation errors. Empty when the metadata is valid.
    """

    path: str
    errors: list[str]

    @property
    def valid(self) -> bool:
        """True when the metadata has no errors."""
        return not self.errors


@dataclass(frozen=True)
class MetadataValidateResult:
    """The levels a ``metadata validate`` run checked.

    Attributes:
        recursive: True when the run walked the tree.
        validations: One entry for each level that the run checked.
    """

    recursive: bool
    validations: list[MetadataValidation]

    @property
    def valid(self) -> bool:
        """True when every level is valid."""
        return all(v.valid for v in self.validations)

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``metadata validate``."""
        if not self.recursive:
            only = self.validations[0]
            return {"valid": only.valid, "errors": only.errors, "path": only.path}
        valid_count = sum(1 for v in self.validations if v.valid)
        return {
            "mode": "recursive",
            "results": [
                {"path": v.path, "valid": v.valid, "errors": v.errors} for v in self.validations
            ],
            "summary": {
                "total": len(self.validations),
                "valid": valid_count,
                "invalid": len(self.validations) - valid_count,
            },
        }


def validate_metadata_files(
    catalog_root: Path,
    path: str | None = None,
    *,
    recursive: bool = True,
) -> MetadataValidateResult:
    """Validate the merged metadata.yaml at each catalog and collection level.

    A recursive run checks only the levels that hold a metadata.yaml, and it
    reports invalid YAML as a validation error of that level. A single-path run
    checks the merged metadata of the start directory.

    Args:
        catalog_root: The catalog root.
        path: The start directory, relative to the root. None selects the root.
        recursive: If False, validate only the start directory.

    Returns:
        The validation errors of each level.

    Raises:
        InputValidationError: When ``path`` escapes the catalog root.
        PathNotFoundError: When a recursive run starts at a missing path.
        NotADirectoryError: When a recursive run starts at a file.
        PermissionError: When the recursive walk cannot list a directory.
        ConfigInvalidStructureError: When a single-path run reads invalid YAML.
    """
    from portolan_cli.errors import ConfigInvalidStructureError
    from portolan_cli.metadata_yaml import load_and_validate_metadata

    base_dir = resolve_catalog_dir(catalog_root, path, must_exist=recursive)

    if not recursive:
        _metadata, errors = load_and_validate_metadata(base_dir, catalog_root)
        return MetadataValidateResult(False, [MetadataValidation(str(path or "."), errors)])

    validations: list[MetadataValidation] = []
    for dirpath in _stac_dirs(catalog_root, base_dir):
        if not (dirpath / _METADATA_FILE).exists():
            continue
        rel_path = str(dirpath.relative_to(catalog_root))
        try:
            _metadata, errors = load_and_validate_metadata(dirpath, catalog_root)
        except ConfigInvalidStructureError as err:
            errors = [f"Invalid YAML: {err}"]
        validations.append(MetadataValidation(rel_path, errors))
    return MetadataValidateResult(True, validations)
