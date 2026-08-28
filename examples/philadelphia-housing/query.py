"""Summarize affordable housing production by City Council district."""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

import duckdb  # deptry: ignore[DEP003] - tutorial analysis, not CLI runtime code


def _asset(catalog: str, collection_id: str) -> str:
    """Return a local path or URL for one GeoParquet asset."""
    parsed = urlparse(catalog)
    relative = f"{collection_id}/{collection_id}.parquet"
    if parsed.scheme in {"http", "https"}:
        return f"{catalog.rstrip('/')}/{relative}"
    return str(Path(catalog) / relative)


def summarize(catalog: str) -> tuple[list[tuple[int, int, int]], tuple[int, int]]:
    """Return district totals and totals for projects without geometry."""
    affordable = _asset(catalog, "affordablehousingproduction")
    districts = _asset(catalog, "council_districts_2024")

    connection = duckdb.connect()
    try:
        connection.execute("INSTALL spatial")
        connection.execute("LOAD spatial")
        rows = connection.execute(
            """
            SELECT
                c.district_num AS district,
                count(*) AS projects,
                sum(a.total_units) AS units
            FROM read_parquet(?) AS c
            JOIN read_parquet(?) AS a
              ON ST_Intersects(c.geometry, a.geometry)
            GROUP BY c.district_num
            ORDER BY c.district_num
            """,
            [districts, affordable],
        ).fetchall()
        missing = connection.execute(
            """
            SELECT count(*), coalesce(sum(total_units), 0)
            FROM read_parquet(?)
            WHERE geometry IS NULL
            """,
            [affordable],
        ).fetchone()
    finally:
        connection.close()

    if missing is None:
        raise RuntimeError("DuckDB did not return the missing-geometry summary")
    return [(int(row[0]), int(row[1]), int(row[2])) for row in rows], (
        int(missing[0]),
        int(missing[1]),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize Philadelphia affordable housing by City Council district."
    )
    parser.add_argument("catalog", help="Local catalog path or public catalog URL")
    return parser.parse_args()


def main() -> None:
    """Run the spatial join and print stable, human-readable results."""
    args = _parse_args()
    rows, missing = summarize(args.catalog)

    print(f"{'district':>8}  {'projects':>8}  {'units':>5}")
    for district, projects, units in rows:
        print(f"{district:>8}  {projects:>8}  {units:>5}")

    print()
    print(f"Located projects: {sum(row[1] for row in rows):,}")
    print(f"Located units: {sum(row[2] for row in rows):,}")
    print(f"Projects without geometry: {missing[0]:,}")
    print(f"Units without geometry: {missing[1]:,}")


if __name__ == "__main__":
    main()
