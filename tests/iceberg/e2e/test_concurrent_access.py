"""E2E tests for concurrent access via REST catalog.

These tests verify that Iceberg's optimistic concurrency control works
correctly when multiple threads publish simultaneously.

Marked as e2e_slow -- runs only in nightly CI.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from tests.iceberg.e2e.conftest import write_test_parquet


@pytest.mark.e2e
@pytest.mark.e2e_slow
def test_concurrent_publish_different_collections(rest_iceberg_backend, tmp_path):
    """Two threads publishing to different collections should both succeed."""
    results = {}

    def publish_to(collection_name: str, index: int):
        asset = write_test_parquet(tmp_path / f"{collection_name}.parquet", rows=index + 1)
        return rest_iceberg_backend.publish(
            collection=collection_name,
            assets={f"{collection_name}.parquet": str(asset)},
            schema={"columns": ["id", "val"], "types": {}, "hash": f"h{index}"},
            breaking=False,
            message=f"Concurrent write to {collection_name}",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {
            pool.submit(publish_to, f"concurrent-a-{id(tmp_path)}", 0): "a",
            pool.submit(publish_to, f"concurrent-b-{id(tmp_path)}", 1): "b",
        }
        for future in as_completed(futures):
            label = futures[future]
            results[label] = future.result()

    assert results["a"].version == "1.0.0"
    assert results["b"].version == "1.0.0"


@pytest.mark.e2e
@pytest.mark.e2e_slow
def test_concurrent_read_during_write(rest_iceberg_backend, tmp_path):
    """Reader should see consistent state during concurrent write."""
    collection = f"read-write-{id(tmp_path)}"

    # Seed with v1
    asset = write_test_parquet(tmp_path / "seed.parquet")
    rest_iceberg_backend.publish(
        collection=collection,
        assets={"seed.parquet": str(asset)},
        schema={"columns": ["id", "val"], "types": {}, "hash": "h0"},
        breaking=False,
        message="Seed",
    )

    read_results = []

    def writer():
        a = write_test_parquet(tmp_path / "w.parquet", rows=5)
        return rest_iceberg_backend.publish(
            collection=collection,
            assets={"w.parquet": str(a)},
            schema={"columns": ["id", "val"], "types": {}, "hash": "h1"},
            breaking=False,
            message="Write during read",
        )

    def reader():
        versions = rest_iceberg_backend.list_versions(collection)
        read_results.append(len(versions))
        return versions

    with ThreadPoolExecutor(max_workers=2) as pool:
        fw = pool.submit(writer)
        fr = pool.submit(reader)
        fw.result()
        fr.result()

    # Reader should see either 1 (pre-write) or 2 (post-write), never 0
    assert read_results[0] in (1, 2), f"Expected 1 or 2 versions, reader saw {read_results[0]}"
