"""SQL text for the DuckDB session that writes an Iceberg table.

DuckDB binds no parameters in DDL, so every identifier and literal passes
through a quoting function here, and the session never formats SQL itself.

The column types come from DuckDB's own ``DESCRIBE`` of the GeoParquet
files, and ``create_table_sql`` copies them verbatim. That is how a geometry
column keeps its CRS: with the spatial extension loaded, ``read_parquet``
types a GeoParquet geometry as ``GEOMETRY('EPSG:25830')`` from the Parquet
logical type or the ``geo`` key, and a ``CREATE TABLE`` with that type lands
in the Iceberg schema as ``geometry(EPSG:25830)``. Verified 2026-10-29
against the Apache REST fixture 1.10.1 with DuckDB 1.5.6.

The table is created at format version 3 on purpose. DuckDB's default is
version 2, and the Java REST catalog refuses a geometry column there
("geometry is not supported until v3").
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

#: The Iceberg format version every table the backend creates has.
FORMAT_VERSION = 3


def quote_identifier(name: str) -> str:
    """Return ``name`` as a double-quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Return ``value`` as a single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def qualified_table(catalog: str, namespace: str, name: str) -> str:
    """Return the three-part name of a table in an attached catalog.

    The catalog alias is the one the session chose for ``ATTACH``, so it is
    not quoted. The namespace and the table name come from the user.
    """
    return f"{catalog}.{quote_identifier(namespace)}.{quote_identifier(name)}"


def read_parquet_sql(paths: Sequence[str], *, hive_partitioning: bool = False) -> str:
    """Return a ``read_parquet`` call over ``paths``, unioned by column name.

    ``union_by_name`` lets a collection whose files differ in column order,
    or in an optional column, read as one relation. ``hive_partitioning``
    reads the ``key=value`` directories of a partitioned collection as
    columns.

    Raises:
        ValueError: ``paths`` is empty.
    """
    if not paths:
        raise ValueError("read_parquet needs at least one file")
    files = ", ".join(quote_literal(path) for path in paths)
    hive = "true" if hive_partitioning else "false"
    return f"read_parquet([{files}], union_by_name = true, hive_partitioning = {hive})"


def describe_sql(source: str) -> str:
    """Return the ``DESCRIBE`` whose rows give each column's name and type."""
    # S608: the source is SQL this module built from quoted literals, not user text.
    return f"DESCRIBE SELECT * FROM {source}"  # noqa: S608


def create_table_sql(
    table: str, columns: Sequence[tuple[str, str]], *, format_version: int = FORMAT_VERSION
) -> str:
    """Return the ``CREATE TABLE`` for ``columns`` at ``format_version``.

    Args:
        table: The qualified, quoted table name from :func:`qualified_table`.
        columns: ``(name, type)`` pairs as ``DESCRIBE`` reports them.
        format_version: The Iceberg format version.

    Raises:
        ValueError: ``columns`` is empty.
    """
    if not columns:
        raise ValueError("a table needs at least one column")
    body = ", ".join(f"{quote_identifier(name)} {kind}" for name, kind in columns)
    return f"CREATE TABLE {table} ({body}) WITH ('format-version' = {format_version})"


def add_columns_sql(table: str, columns: Sequence[tuple[str, str]]) -> list[str]:
    """Return one ``ALTER TABLE ... ADD COLUMN`` per column."""
    return [
        f"ALTER TABLE {table} ADD COLUMN {quote_identifier(name)} {kind}" for name, kind in columns
    ]
