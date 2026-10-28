"""GeoParquet metadata extraction.

Uses pyarrow to read GeoParquet metadata without loading full data.
Extracts bbox, CRS, schema, and geometry type from file metadata.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Protocol, TypeVar, overload

import pyarrow.parquet as pq
from pyproj import CRS
from pyproj.exceptions import CRSError

from portolan_cli.formats import parquet_footer, read_geo_metadata
from portolan_cli.models.schema import ColumnSchema, SchemaModel
from portolan_cli.trace import count

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pyarrow as pa

T = TypeVar("T")


class _Memo(Protocol):
    """The part of ``run_state.RunState`` this module uses.

    A Protocol, not an import: the ``viz`` layer imports this module, and
    ``run_state`` imports ``sync.checksums``, which ``viz`` must not reach.
    """

    def memo(self, kind: str, path: Path, loader: Callable[[Path], T]) -> T:
        """Load a value derived from one file once per run."""
        ...


# Footer keys the writer owns. geoparquet-io writes a fresh ``geo``. It drops
# ``ARROW:schema``, ``pandas`` and ``geo:overviews`` on purpose, because they
# describe the rows of the input, not of the output (gpio#1268). A rewrite
# keeps every other key.
_WRITER_OWNED_KEYS: frozenset[bytes] = frozenset(
    {b"geo", b"ARROW:schema", b"pandas", b"geo:overviews"}
)


@dataclass
class GeoParquetMetadata:
    """Metadata extracted from a GeoParquet file.

    Attributes:
        bbox: Bounding box as (minx, miny, maxx, maxy) or None.
        crs: CRS as EPSG code, WKT, or PROJJSON dict.
        geometry_type: Every declared geometry type (Point, Polygon, etc.), in
            footer order without duplicates, or None when the types are unknown.
        geometry_column: Name of the geometry column.
        feature_count: Number of features (rows).
        schema: Column names and types.
        schema_fingerprint: The freshness fingerprint of the Arrow schema (see
            :func:`schema_fingerprint`), read in the same footer read.
    """

    bbox: tuple[float, float, float, float] | None
    crs: str | dict[str, Any] | None
    geometry_type: list[str] | None
    geometry_column: str | None
    feature_count: int
    schema: dict[str, str]
    schema_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "bbox": list(self.bbox) if self.bbox else None,
            "crs": self.crs,
            "geometry_type": self.geometry_type,
            "geometry_column": self.geometry_column,
            "feature_count": self.feature_count,
            "schema": self.schema,
        }

    def _crs_to_epsg(self) -> int | None:
        """Resolve the CRS to an EPSG code, or None if unavailable.

        ``crs`` may be an EPSG/WKT/OGC-URN string or a PROJJSON dict. A PROJJSON
        dict that already carries ``id.code`` is read directly (mirroring
        ``_extract_crs``); otherwise pyproj resolves it.
        """
        crs = self.crs
        if crs is None:
            return None

        if isinstance(crs, dict):
            code = crs.get("id", {}).get("code")
            if code is not None:
                try:
                    return int(code)
                except (TypeError, ValueError):
                    pass

        try:
            return CRS.from_user_input(crs).to_epsg()
        except CRSError:
            return None  # Unknown CRS format, skip proj:epsg

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item properties format."""
        props: dict[str, Any] = {}

        # Derive the EPSG code from the file's real CRS (issue #488). The
        # proj:epsg key is the internal handoff to the STAC layer, which
        # publishes it as proj:code on the data asset (issue #654).
        epsg = self._crs_to_epsg()
        if epsg is not None:
            props["proj:epsg"] = epsg

        if self.geometry_type:
            props["geoparquet:geometry_type"] = self.geometry_type

        if self.feature_count is not None:
            props["geoparquet:feature_count"] = self.feature_count

        return props


def schema_fingerprint(schema: Any) -> str:
    """The freshness fingerprint of an Arrow schema: 16 hex digits of its SHA-256.

    versions.json stores this value, so the text it hashes must not change.
    """
    return hashlib.sha256(str(schema).encode()).hexdigest()[:16]


def extract_geoparquet_metadata(path: Path, *, state: _Memo | None = None) -> GeoParquetMetadata:
    """Extract metadata from a GeoParquet file.

    Uses pyarrow to read file metadata without loading all data.
    Parses GeoParquet geo metadata from file's custom metadata. One footer read
    gives every field, the schema fingerprint included. With ``state``, the run
    reads each footer once while the file stays unchanged.

    Args:
        path: Path to GeoParquet file.
        state: The run state, for one footer read per file and run.

    Returns:
        GeoParquetMetadata with extracted information.

    Raises:
        FileNotFoundError: If file doesn't exist.

    Note:
        For Parquet files without GeoParquet metadata, returns metadata with
        None/empty fields for geo-specific properties (bbox, crs, geometry_type).
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if state is not None:
        return state.memo("geoparquet.metadata", path, _read_geoparquet_metadata)
    return _read_geoparquet_metadata(path)


def _read_geoparquet_metadata(path: Path) -> GeoParquetMetadata:
    return geoparquet_metadata_of(parquet_footer(path))


def geoparquet_metadata_of(pf: pq.ParquetFile) -> GeoParquetMetadata:
    """Read the GeoParquet metadata from an open footer.

    A caller that has the footer open for another reason reads it once.
    """
    # Parse GeoParquet geo metadata
    geo_metadata = read_geo_metadata(pf) or {}

    # Extract schema
    schema = {field.name: str(field.type) for field in pf.schema_arrow}

    # Get feature count from row groups
    feature_count = pf.metadata.num_rows

    # Extract geometry column info
    geometry_column = geo_metadata.get("primary_column", "geometry")
    column_meta = geo_metadata.get("columns", {}).get(geometry_column, {})

    # Extract bbox
    bbox = _extract_bbox(column_meta)

    # Extract CRS
    crs = _extract_crs(column_meta)

    # Extract geometry type
    geometry_type = _extract_geometry_type(column_meta)

    return GeoParquetMetadata(
        bbox=bbox,
        crs=crs,
        geometry_type=geometry_type,
        geometry_column=geometry_column,
        feature_count=feature_count,
        schema=schema,
        schema_fingerprint=schema_fingerprint(pf.schema_arrow),
    )


class SpatialLayout(NamedTuple):
    """What one Parquet footer says about a file's spatial layout.

    Attributes:
        is_geoparquet: True when the file declares GeoParquet ``geo`` metadata.
        has_bbox_covering: True when the ``geo`` metadata names a
            ``covering.bbox`` entry whose root column exists in the schema.
    """

    is_geoparquet: bool
    has_bbox_covering: bool


def open_footer(path: Path) -> pq.ParquetFile | None:
    """Read the footer of a Parquet file, or None when it does not open.

    The file is closed, and it still answers the footer questions
    (:func:`formats.parquet_footer`). :func:`read_spatial_layout` and
    :func:`read_rewrite_fidelity` both take it, so a caller that needs both
    answers reads the footer once.
    """
    try:
        return parquet_footer(path)
    except Exception:  # noqa: BLE001 - an unreadable file has no footer to read
        return None


def _footer_or_open(source: Path | pq.ParquetFile) -> tuple[pq.ParquetFile | None, bool]:
    """The open file, and True when this call opened it and must close it."""
    if isinstance(source, pq.ParquetFile):
        return source, False
    return open_footer(source), True


def read_spatial_layout(source: Path | pq.ParquetFile) -> SpatialLayout:
    """Read a Parquet footer once and report both spatial-layout answers.

    Reads only the footer, so the cost does not grow with the file. Callers
    that need both answers must use this rather than calling
    ``formats.is_geoparquet`` and then reading the covering, which would open
    the file twice.

    rashid resolves both PTL-DAT-006 and PTL-DAT-007 through the covering
    column. It reads per-row-group extents from the column's min/max statistics
    for PTL-DAT-007, and it reads row order from the same column for
    PTL-DAT-006. A file without the column fails PTL-DAT-007 and leaves
    PTL-DAT-006 unevaluated, so Portolan rewrites such a file on ``add``
    (issue #805).

    The answer says nothing about row order. A file that carries the column but
    holds unordered rows still fails PTL-DAT-006, and ``add --force
    --reconvert`` is what rewrites it.

    Args:
        source: Path to a Parquet file, or a file the caller opened with
            :func:`open_footer`. A path is opened and closed here.

    Returns:
        The layout. Both fields are False for an unreadable file, and
        ``has_bbox_covering`` is False for a plain Parquet file.
    """
    parquet, owned = _footer_or_open(source)
    if parquet is None:
        # An unreadable file is not a conformant one.
        return SpatialLayout(is_geoparquet=False, has_bbox_covering=False)
    try:
        return _spatial_layout(parquet)
    finally:
        if owned:
            parquet.close()


def _spatial_layout(parquet: pq.ParquetFile) -> SpatialLayout:
    """The spatial layout of an open Parquet file. See :func:`read_spatial_layout`."""
    geo = read_geo_metadata(parquet) or {}
    if not geo:
        return SpatialLayout(is_geoparquet=False, has_bbox_covering=False)

    primary = geo.get("primary_column", "geometry")
    columns = geo.get("columns")
    column_meta = columns.get(primary, {}) if isinstance(columns, dict) else {}
    covering = column_meta.get("covering") if isinstance(column_meta, dict) else None
    bbox_paths = covering.get("bbox") if isinstance(covering, dict) else None
    if not isinstance(bbox_paths, dict):
        return SpatialLayout(is_geoparquet=True, has_bbox_covering=False)

    # The covering entry names the leaf paths of the struct. Its root must be a
    # real column, or a reader has nothing to prune on.
    names = set(parquet.schema_arrow.names)
    covers = all(
        isinstance(leaf, list) and leaf and leaf[0] in names for leaf in bbox_paths.values()
    )
    return SpatialLayout(is_geoparquet=True, has_bbox_covering=covers)


class RewriteFidelity(NamedTuple):
    """What a rewrite must not lose, read from one Parquet footer.

    Attributes:
        row_count: Number of rows in the file.
        crs: The primary geometry column's declared CRS, as written.
        columns: Every column name in the schema.
        extra_keys: The footer keys the writer does not own, such as a
            publisher's provenance key.
        geo_columns: The publisher fields of each geometry column in ``geo``:
            ``crs`` when the key is present, ``edges``, ``epoch``, and
            ``orientation``. None when the gate has nothing to compare.
        field_metadata: The field metadata of each column and nested child.
            Nested children use a tuple of the column name and child indexes.
            None when the gate has nothing to compare.
    """

    row_count: int
    crs: Any
    columns: frozenset[str]
    extra_keys: frozenset[bytes] = frozenset()
    geo_columns: dict[str, dict[str, Any]] | None = None
    field_metadata: dict[str | tuple[str, ...], dict[bytes, bytes]] | None = None


#: The fields of a ``geo`` column entry that a publisher sets and a rewrite
#: must keep. The writer recomputes ``bbox``, ``covering``, ``encoding``, and
#: ``geometry_types``.
_PUBLISHER_GEO_FIELDS = ("crs", "edges", "epoch", "orientation")


def read_rewrite_fidelity(source: Path | pq.ParquetFile) -> RewriteFidelity | None:
    """Read what an in-place rewrite must carry over unchanged.

    The rewrite replaces the operator's own file, so it needs a before-and-after
    comparison to prove it lost nothing. geoparquet-io drops the CRS on every
    write, which turns a projected GeoParquet into one that declares no CRS
    (issue #805). A malformed geometry makes it fail outright.

    Args:
        source: Path to a Parquet file, or a file the caller opened with
            :func:`open_footer`. A path is opened and closed here.

    Returns:
        The fidelity fields, or None when the file cannot be read.
    """
    parquet, owned = _footer_or_open(source)
    if parquet is None:
        # An unreadable file cannot be compared.
        return None
    try:
        return _rewrite_fidelity(parquet)
    finally:
        if owned:
            parquet.close()


def _rewrite_fidelity(parquet: pq.ParquetFile) -> RewriteFidelity:
    """The fidelity fields of an open Parquet file. See :func:`read_rewrite_fidelity`."""
    geo = read_geo_metadata(parquet) or {}
    primary = geo.get("primary_column", "geometry")
    columns = geo.get("columns")
    column_meta = columns.get(primary, {}) if isinstance(columns, dict) else {}
    crs = column_meta.get("crs") if isinstance(column_meta, dict) else None
    metadata = parquet.metadata.metadata or {}
    geo_columns = {
        name: {key: entry[key] for key in _PUBLISHER_GEO_FIELDS if key in entry}
        for name, entry in (columns.items() if isinstance(columns, dict) else ())
        if isinstance(entry, dict)
    }
    return RewriteFidelity(
        row_count=parquet.metadata.num_rows,
        crs=crs,
        columns=frozenset(parquet.schema_arrow.names),
        extra_keys=frozenset(metadata) - _WRITER_OWNED_KEYS,
        geo_columns=geo_columns,
        field_metadata=_raw_field_metadata(parquet, frozenset(geo_columns)),
    )


def _raw_field_metadata(
    parquet: pq.ParquetFile, geometry_columns: frozenset[str]
) -> dict[str | tuple[str, ...], dict[bytes, bytes]]:
    """Read extension declarations without consulting Arrow's global registry.

    Registered types consume the extension metadata and can normalize away
    publisher fields. Decode the stored IPC schema with its extension-name key
    masked by an equal-length, absent token. This preserves the IPC offsets and
    leaves the registry untouched, including during concurrent footer reads.
    """
    import pyarrow as pa

    stored = (parquet.metadata.metadata or {}).get(b"ARROW:schema")
    if stored is None:
        return _field_metadata(parquet.schema_arrow, geometry_columns)
    serialized = base64.b64decode(stored)
    key = b"ARROW:extension:name"
    token = b"_" * len(key)
    nonce = 0
    while token in serialized:
        nonce += 1
        token = f"{nonce:0{len(key)}d}".encode()
    schema = pa.ipc.read_schema(pa.BufferReader(serialized.replace(key, token)))
    return _field_metadata(schema, geometry_columns, masked_key=token)


def _field_metadata(
    schema: pa.Schema, geometry_columns: frozenset[str], *, masked_key: bytes | None = None
) -> dict[str | tuple[str, ...], dict[bytes, bytes]]:
    """Keep metadata at every depth, including semantic Arrow extension keys."""
    result: dict[str | tuple[str, ...], dict[bytes, bytes]] = {}

    def visit(field: pa.Field, path: tuple[str, ...]) -> None:
        metadata = dict(field.metadata or {})
        if masked_key is not None:
            # The byte substitution can also touch a publisher key or value
            # that quotes the extension-name key; restore those verbatim too.
            metadata = {
                key.replace(masked_key, b"ARROW:extension:name"): value.replace(
                    masked_key, b"ARROW:extension:name"
                )
                for key, value in metadata.items()
            }
        # The writer normalizes the GeoArrow WKB declaration. Its CRS and
        # edges also live in `geo`, where the gate compares their meaning.
        # Other extension declarations (and arbitrary ARROW:/PARQUET: keys)
        # carry semantics the writer must not silently drop.
        if (
            len(path) == 1
            and path[0] in geometry_columns
            and metadata.get(b"ARROW:extension:name") == b"geoarrow.wkb"
        ):
            try:
                declaration = json.loads(metadata.get(b"ARROW:extension:metadata", b"{}"))
            except ValueError:
                declaration = None  # An unreadable declaration must survive verbatim.
            # Registered GeoArrow types also emit crs_type=projjson. It names
            # the CRS representation already compared through `geo`, rather
            # than publisher metadata. Keep unknown representation tags raw.
            if (
                isinstance(declaration, dict)
                and declaration.keys() <= {"crs", "edges", "crs_type"}
                and declaration.get("crs_type", "projjson") == "projjson"
            ):
                metadata.pop(b"ARROW:extension:name", None)
                metadata.pop(b"ARROW:extension:metadata", None)
        if metadata:
            result[path[0] if len(path) == 1 else path] = metadata
        for index in range(field.type.num_fields):
            visit(field.type.field(index), (*path, str(index)))

    for field in schema:
        name = field.name
        if masked_key is not None:
            name = name.encode().replace(masked_key, b"ARROW:extension:name").decode()
        visit(field, (name,))
    return result


def _extract_bbox(column_meta: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """Extract bbox from column metadata.

    Validates bbox for finite values (issue #516). Does NOT check WGS84 range
    since source data may be in projected CRS.
    Returns None if bbox is missing, malformed, or contains inf/nan values.
    """
    import logging

    from portolan_cli.bbox import is_finite_bbox, to_2d_bbox

    logger = logging.getLogger(__name__)

    bbox = column_meta.get("bbox")
    if bbox and len(bbox) >= 4:
        try:
            bbox_list = to_2d_bbox([float(value) for value in bbox])
        except (ValueError, TypeError):
            # Non-numeric/malformed bbox entries - log and treat as missing
            logger.warning("Malformed bbox in GeoParquet metadata (non-numeric): %s", bbox)
            return None
        if is_finite_bbox(bbox_list):
            return (bbox_list[0], bbox_list[1], bbox_list[2], bbox_list[3])
        # Invalid bbox (inf/nan) - log and return None
        logger.warning("Invalid bbox in GeoParquet metadata (inf/nan): %s", bbox_list)
    return None


def _extract_crs(column_meta: dict[str, Any]) -> str | dict[str, Any] | None:
    """Extract CRS from column metadata."""
    crs = column_meta.get("crs")
    if crs is None:
        return None

    # CRS can be PROJJSON dict or WKT string
    if isinstance(crs, dict):
        # Try to extract EPSG code from PROJJSON
        epsg = crs.get("id", {}).get("code")
        if epsg:
            return f"EPSG:{epsg}"
        return crs
    return str(crs)


def _extract_geometry_type(column_meta: dict[str, Any]) -> list[str] | None:
    """Every geometry type a column declares, or None when they are unknown.

    The list keeps the footer order and drops duplicates. GeoParquet reads an
    empty ``geometry_types`` as unknown types, so it gives None, not a guess.
    Keeping only the first type hid the rest from the STAC metadata (#1010).
    """
    declared = column_meta.get("geometry_types")
    if declared is None:
        # Fallback to a single geometry_type field.
        declared = column_meta.get("geometry_type")
    if isinstance(declared, str):
        declared = [declared]
    if not isinstance(declared, list):
        return None
    types = list(dict.fromkeys(str(value) for value in declared))
    return types or None


# Overloaded signatures for extract_schema_from_geoparquet
@overload
def extract_schema_from_geoparquet(
    path: Path,
    *,
    return_warnings: Literal[False] = False,
) -> SchemaModel: ...


@overload
def extract_schema_from_geoparquet(
    path: Path,
    *,
    return_warnings: Literal[True],
) -> tuple[SchemaModel, list[str]]: ...


def extract_schema_from_geoparquet(
    path: Path,
    *,
    return_warnings: bool = False,
) -> SchemaModel | tuple[SchemaModel, list[str]]:
    """Extract SchemaModel from a GeoParquet file.

    Extracts column metadata including types, nullability, and geometry info.
    Returns a SchemaModel compatible with the Portolan metadata model.

    Args:
        path: Path to GeoParquet file.
        return_warnings: If True, return (SchemaModel, warnings) tuple.

    Returns:
        SchemaModel with column definitions, or (SchemaModel, warnings) tuple
        if return_warnings is True.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    warnings: list[str] = []

    # Open parquet file (metadata only)
    count("parquet.opens")
    pf = pq.ParquetFile(path)

    # Parse GeoParquet geo metadata
    geo_metadata = read_geo_metadata(pf) or {}

    # Get geometry column name
    geometry_column = geo_metadata.get("primary_column", "geometry")
    geo_columns = geo_metadata.get("columns", {})

    # Build ColumnSchema for each field
    columns: list[ColumnSchema] = []
    for field in pf.schema_arrow:
        # Check if this is a geometry column
        column_meta = geo_columns.get(field.name, {})
        # The schema model holds one string per column, so it lists every type.
        declared_types = _extract_geometry_type(column_meta)
        geometry_type = ", ".join(declared_types) if declared_types else None
        crs_raw = _extract_crs(column_meta)
        # Normalize CRS to string for ColumnSchema
        crs_str: str | None = None
        if isinstance(crs_raw, str):
            crs_str = crs_raw
        elif isinstance(crs_raw, dict):
            # Convert dict to string representation (json imported at module level)
            crs_str = json.dumps(crs_raw)

        # Track missing CRS warning for geometry columns
        if field.name == geometry_column and crs_str is None:
            warnings.append(
                f"Geometry column '{field.name}' has no CRS defined. "
                "Consider adding CRS metadata for proper coordinate reference."
            )

        column = ColumnSchema(
            name=field.name,
            type=str(field.type),
            nullable=field.nullable,
            geometry_type=geometry_type,
            crs=crs_str,
        )
        columns.append(column)

    schema = SchemaModel(
        schema_version="1.0.0",
        format="geoparquet",
        columns=list(columns),
    )

    if return_warnings:
        return schema, warnings
    return schema
