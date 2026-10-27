"""Failure-boundary regressions for the deferred add commit."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from portolan_cli.add import add_files
from portolan_cli.cli import cli
from portolan_cli.extract.common.report import ExtractionReport, ExtractionSummary, LayerResult
from portolan_cli.versions import read_versions

if TYPE_CHECKING:
    from pathlib import Path
    from typing import IO

    from portolan_cli.query import ItemInfo

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("entry_point", ["api", "cli"])
@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
@pytest.mark.parametrize("seam", ["_enrich_deferred_documents", "_enrich_saved_documents"])
def test_add_failure_after_finalize_keeps_version(
    conformant_catalog: Path,
    valid_points_parquet: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_point: str,
    failure: type[BaseException],
    seam: str,
) -> None:
    """A failure inside add_files must retain versions of collections already saved."""
    import portolan_cli.add as add_module
    import portolan_cli.finalization as finalization

    root = conformant_catalog
    collection = root / "roads"
    collection.mkdir()
    source = collection / "roads.parquet"
    source.write_bytes(valid_points_parquet.read_bytes())

    def fail_after_finalize(*_args: object, **_kwargs: object) -> None:
        raise failure("interrupted after finalize")

    owner = add_module if seam == "_enrich_deferred_documents" else finalization
    monkeypatch.setattr(owner, seam, fail_after_finalize)
    if entry_point == "api":
        with pytest.raises(failure, match="interrupted after finalize"):
            add_files(paths=[source], catalog_root=root)
    else:
        result = CliRunner().invoke(cli, ["add", "--portolan-dir", str(root), str(source)])
        assert result.exit_code == 1

    assert (collection / "collection.json").exists()
    assert (collection / "versions.json").exists(), "saved collection has no version"
    versions = read_versions(collection / "versions.json")
    assert versions.current_version == "1.0.0"
    assert set(versions.versions[-1].assets) == {"roads.parquet"}
    index = json.loads((root / "versions.json").read_text())["collections"]
    assert index["roads"]["current_version"] == "1.0.0"


@pytest.mark.parametrize("entry_point", ["api", "cli"])
def test_later_collection_failure_keeps_completed_version(
    conformant_catalog: Path,
    valid_points_parquet: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_point: str,
) -> None:
    """A later finalization error must not discard an earlier collection's stage."""
    import portolan_cli.finalization as finalization

    root = conformant_catalog
    sources = []
    for collection_id in ("roads", "trails"):
        collection = root / collection_id
        collection.mkdir()
        source = collection / f"{collection_id}.parquet"
        source.write_bytes(valid_points_parquet.read_bytes())
        sources.append(source)
    finalize = finalization._finalize_collection

    def fail_second(
        catalog_root: Path, collection_id: str, *args: Any, **kwargs: Any
    ) -> list[ItemInfo]:
        if collection_id == "trails":
            raise RuntimeError("second collection failed")
        return finalize(catalog_root, collection_id, *args, **kwargs)

    monkeypatch.setattr(finalization, "_finalize_collection", fail_second)
    if entry_point == "api":
        with pytest.raises(RuntimeError, match="second collection failed"):
            add_files(paths=sources, catalog_root=root)
    else:
        result = CliRunner().invoke(
            cli, ["add", "--portolan-dir", str(root), *(str(source) for source in sources)]
        )
        assert result.exit_code == 1
    assert (root / "roads" / "collection.json").exists()
    assert (root / "roads" / "versions.json").exists(), "completed stage was discarded"
    assert read_versions(root / "roads" / "versions.json").current_version == "1.0.0"
    assert not (root / "trails" / "versions.json").exists()


def _extraction_report() -> ExtractionReport:
    return ExtractionReport(
        extraction_date="2026-10-27T00:00:00+00:00",
        source_url="https://example.com/wfs",
        portolan_version="test",
        gpio_version="test",
        metadata_extracted=None,
        layers=[LayerResult(0, "roads", "success", 1, 1, 0.0, "roads/roads.parquet", [], None, 1)],
        summary=ExtractionSummary(1, 1, 0, 0, 0, 1, 1, 0.0),
    )


def test_extract_failure_while_adding_keeps_version(
    conformant_catalog: Path,
    valid_points_parquet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The extraction commit guard covers the add, which can save then raise."""
    from portolan_cli.extract.common.source import Discovery, LayerRef, build_catalog

    root = conformant_catalog
    collection = root / "roads"
    collection.mkdir()
    (collection / "roads.parquet").write_bytes(valid_points_parquet.read_bytes())

    def fail_after_finalize(*_args: object, **_kwargs: object) -> None:
        raise KeyboardInterrupt("extraction add interrupted")

    monkeypatch.setattr("portolan_cli.add._enrich_deferred_documents", fail_after_finalize)
    source = MagicMock()
    source.name = "fake"
    discovery = Discovery(source_url="https://example.com/wfs", layers=[LayerRef(0, "roads")])
    with pytest.raises(KeyboardInterrupt, match="extraction add interrupted"):
        build_catalog(source, discovery, root, _extraction_report())

    assert (collection / "collection.json").exists()
    assert (collection / "versions.json").exists(), "extraction discarded the add's stage"
    assert read_versions(collection / "versions.json").current_version == "1.0.0"


def test_extract_post_init_and_tabular_add_share_footer(conformant_catalog: Path) -> None:
    """The post-init format probe and deferred table metadata share one actual open."""
    from portolan_cli.config import set_setting
    from portolan_cli.extract.common.orchestrator_base import init_extracted_catalog
    from portolan_cli.formats import is_geoparquet
    from portolan_cli.run_state import RunState
    from portolan_cli.version_ops import commit_run

    root = conformant_catalog
    collection = root / "roads"
    collection.mkdir()
    source = collection / "roads.parquet"
    pq.write_table(pa.table({"name": ["Main Street"]}), source)
    set_setting(root, "tabular.enabled", True)
    state = RunState.open(root)

    def probe(_root: Path, files: list[Path], _fresh: bool) -> None:
        assert files == [source]
        assert is_geoparquet(source) is False

    with patch("pyarrow.parquet.ParquetFile", wraps=pq.ParquetFile) as opens:
        added = init_extracted_catalog(
            root, _extraction_report(), title=None, description=None, post_init=probe, state=state
        )
    assert added == ([source], {"roads"})
    assert opens.call_count == 1, f"one file opened {opens.call_count} times"
    commit_run(state)
    stac = json.loads((collection / "collection.json").read_text())
    assert stac["table:row_count"] == 1


@pytest.mark.parametrize("paths_kind", ["csv", "directory"])
def test_unchanged_item_companion_does_not_write(
    conformant_catalog: Path,
    valid_rgb_cog: Path,
    paths_kind: str,
) -> None:
    """An unchanged CSV in a raster item uses its owner's version before preparation."""
    root = conformant_catalog
    item = root / "imagery" / "scene"
    item.mkdir(parents=True)
    raster = item / "scene.tif"
    raster.write_bytes(valid_rgb_cog.read_bytes())
    companion = item / "attributes.csv"
    companion.write_text("id,name\n1,Scene\n")
    added, _skipped, failures = add_files(paths=[item], catalog_root=root)
    assert failures == []
    assert [info.collection_id for info in added] == ["imagery"]
    before = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}

    added, skipped, failures = add_files(
        paths=[companion if paths_kind == "csv" else item], catalog_root=root
    )

    assert (added, failures) == ([], [])
    assert companion in skipped
    after = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}
    assert after == before, "unchanged item companion rewrote catalog files"


def test_thumbnail_registration_and_version_share_one_hash(
    conformant_catalog: Path,
    valid_points_parquet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registering and staging an adopted thumbnail read its bytes just once."""
    from pathlib import Path

    from portolan_cli.collection_thumbnail import ensure_collection_thumbnails
    from portolan_cli.run_state import RunState

    root = conformant_catalog
    collection = root / "roads"
    collection.mkdir()
    data = collection / "roads.parquet"
    data.write_bytes(valid_points_parquet.read_bytes())
    assert add_files(paths=[data], catalog_root=root)[2] == []
    thumbnail = collection / "thumbnail.png"
    thumbnail.write_bytes(b"thumbnail supplied by the user")
    state = RunState.open(root)
    read_paths = []
    open_file = Path.open

    def record_open(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> IO[Any]:
        if path == thumbnail and mode == "rb":
            read_paths.append(path)
        return open_file(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", record_open)
    ensure_collection_thumbnails(root, {"roads"}, generate=True, state=state)

    assert read_paths == [thumbnail], "thumbnail bytes were hashed more than once"
    assert "thumbnail.png" in state.staged()["roads"].assets


@pytest.mark.parametrize("format_name", ["vector", "raster"])
def test_force_noop_does_not_write_an_empty_version(
    conformant_catalog: Path,
    valid_points_parquet: Path,
    valid_rgb_cog: Path,
    format_name: str,
) -> None:
    """Forced metadata refreshes do not append a version when the asset bytes agree."""
    root = conformant_catalog
    if format_name == "vector":
        data = root / "roads" / "roads.parquet"
        fixture = valid_points_parquet
    else:
        data = root / "imagery" / "scene" / "scene.tif"
        fixture = valid_rgb_cog
    data.parent.mkdir(parents=True)
    data.write_bytes(fixture.read_bytes())
    args = ["add", "--no-thumbnails", "--portolan-dir", str(root), str(data)]
    runner = CliRunner()
    first = runner.invoke(cli, args, catch_exceptions=False)
    assert first.exit_code == 0, first.output
    before = {path: path.read_bytes() for path in root.rglob("versions.json")}
    before_mtime = {path: path.stat().st_mtime_ns for path in before}

    second = runner.invoke(cli, [*args, "--force"], catch_exceptions=False)

    assert second.exit_code == 0, second.output
    assert {path: path.read_bytes() for path in before} == before
    assert {path: path.stat().st_mtime_ns for path in before} == before_mtime


def test_phase_two_pool_continues_after_one_failed_file(
    conformant_catalog: Path,
    valid_points_parquet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One worker's preparation error leaves both successful assets in one version."""
    import portolan_cli.add as add_module

    root = conformant_catalog
    collection = root / "roads"
    collection.mkdir()
    paths = [collection / f"{name}.parquet" for name in ("a", "b", "c")]
    for path in paths:
        path.write_bytes(valid_points_parquet.read_bytes())
    prepare = add_module.prepare_item

    def fail_middle(*args: Any, **kwargs: Any) -> Any:
        if kwargs["path"] == paths[1]:
            raise ValueError("middle preparation failed")
        return prepare(*args, **kwargs)

    monkeypatch.setattr(add_module, "prepare_item", fail_middle)
    added, skipped, failures = add_files(paths=paths, catalog_root=root, workers=4)

    assert skipped == []
    assert {info.item_id for info in added} == {"a", "c"}
    assert [(failure.path, failure.error) for failure in failures] == [
        (paths[1], "middle preparation failed")
    ]
    versions = read_versions(collection / "versions.json")
    assert len(versions.versions) == 1
    assert set(versions.versions[-1].assets) == {"a.parquet", "c.parquet"}
