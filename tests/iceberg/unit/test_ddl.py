"""SQL text the DuckDB session runs against an attached Iceberg catalog.

DuckDB binds no parameters in DDL, so every identifier and literal goes
through a quoting function. The column types come from DuckDB's own
``DESCRIBE`` of the GeoParquet file, and the DDL copies them verbatim: that is
how a ``GEOMETRY('EPSG:25830')`` column keeps its CRS.
"""

from __future__ import annotations

import pytest

from portolan_cli.backends.iceberg.ddl import (
    add_columns_sql,
    create_table_sql,
    describe_sql,
    qualified_table,
    quote_identifier,
    quote_literal,
    read_parquet_sql,
)

pytestmark = pytest.mark.unit


def test_quote_identifier_doubles_embedded_quotes() -> None:
    assert quote_identifier("plain") == '"plain"'
    assert quote_identifier('say "hi"') == '"say ""hi"""'


def test_quote_literal_doubles_embedded_quotes() -> None:
    assert quote_literal("a'b") == "'a''b'"
    assert quote_literal("C:\\data\\x.parquet") == "'C:\\data\\x.parquet'"


def test_qualified_table_quotes_each_part() -> None:
    assert (
        qualified_table("lake", "portolake", "my-collection") == 'lake."portolake"."my-collection"'
    )


def test_read_parquet_lists_every_file_and_unions_by_name() -> None:
    sql = read_parquet_sql(["/a/one.parquet", "/a/two's.parquet"])

    assert sql == (
        "read_parquet(['/a/one.parquet', '/a/two''s.parquet'], "
        "union_by_name = true, hive_partitioning = false)"
    )


def test_read_parquet_can_read_hive_partitions() -> None:
    sql = read_parquet_sql(["/a/cell=1/x.parquet"], hive_partitioning=True)

    assert sql.endswith("hive_partitioning = true)")


def test_read_parquet_refuses_an_empty_list() -> None:
    with pytest.raises(ValueError, match="at least one"):
        read_parquet_sql([])


def test_describe_wraps_a_source() -> None:
    assert describe_sql("read_parquet(['x'])") == "DESCRIBE SELECT * FROM read_parquet(['x'])"


def test_create_table_copies_the_described_types_and_asks_for_version_3() -> None:
    columns = [
        ("id", "BIGINT"),
        ("name", "VARCHAR"),
        ("geometry", "GEOMETRY('EPSG:25830')"),
        ("bbox", "STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)"),
    ]

    sql = create_table_sql('lake."portolake"."roads"', columns)

    assert sql == (
        'CREATE TABLE lake."portolake"."roads" ('
        '"id" BIGINT, "name" VARCHAR, "geometry" GEOMETRY(\'EPSG:25830\'), '
        '"bbox" STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)'
        ") WITH ('format-version' = 3)"
    )


def test_create_table_refuses_no_columns() -> None:
    with pytest.raises(ValueError, match="at least one column"):
        create_table_sql('lake."ns"."t"', [])


def test_add_columns_gives_one_statement_per_column() -> None:
    statements = add_columns_sql('lake."ns"."t"', [("a", "INTEGER"), ("b c", "VARCHAR")])

    assert statements == [
        'ALTER TABLE lake."ns"."t" ADD COLUMN "a" INTEGER',
        'ALTER TABLE lake."ns"."t" ADD COLUMN "b c" VARCHAR',
    ]
