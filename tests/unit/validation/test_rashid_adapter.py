"""The rashid adapter: config translation, pass routing, and validator injection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from rashid.model import Severity

from portolan_cli.validation import run_check
from portolan_cli.validation.config import load_public_url, load_rules_config

pytestmark = pytest.mark.unit


def _write_config(root: Path, body: dict[str, Any]) -> None:
    portolan = root / ".portolan"
    portolan.mkdir(parents=True, exist_ok=True)
    (portolan / "config.yaml").write_text(yaml.dump(body), encoding="utf-8")


class TestLoadRulesConfig:
    def test_no_config_yields_empty_rules_config(self, tmp_path: Path) -> None:
        config = load_rules_config(tmp_path)
        assert config.disabled == frozenset()
        assert config.severity_overrides == {}

    def test_disabled_ids_are_read(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"check": {"disabled": ["PTL-TTL-002", "PTL-VIZ-001"]}})
        assert load_rules_config(tmp_path).disabled == frozenset({"PTL-TTL-002", "PTL-VIZ-001"})

    def test_severity_overrides_are_read(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"check": {"severity": {"PTL-VIZ-001": "error"}}})
        assert load_rules_config(tmp_path).severity_overrides == {"PTL-VIZ-001": Severity.ERROR}

    def test_unrelated_config_keys_are_ignored(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"conversion": {"vector": {"enabled": True}}})
        assert load_rules_config(tmp_path).disabled == frozenset()

    def test_legacy_stac_lint_block_warns(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"stac_lint": {"severity": {"check_thumbnail": "error"}}})
        with pytest.warns(DeprecationWarning, match="stac_lint"):
            config = load_rules_config(tmp_path)
        # Disjoint namespaces: the old keys name native rules that no longer
        # exist, so nothing is translated.
        assert config.severity_overrides == {}

    def test_check_block_alongside_stac_lint_still_applies(self, tmp_path: Path) -> None:
        _write_config(
            tmp_path,
            {
                "stac_lint": {"severity": {"check_thumbnail": "error"}},
                "check": {"disabled": ["PTL-TMP-001"]},
            },
        )
        with pytest.warns(DeprecationWarning):
            config = load_rules_config(tmp_path)
        assert config.disabled == frozenset({"PTL-TMP-001"})

    def test_invalid_severity_value_is_rejected(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"check": {"severity": {"PTL-VIZ-001": "catastrophe"}}})
        with pytest.raises(ValueError, match="catastrophe"):
            load_rules_config(tmp_path)


class TestLoadPublicUrl:
    def test_absent_by_default(self, tmp_path: Path) -> None:
        assert load_public_url(tmp_path) is None

    def test_read_from_publish_block(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"publish": {"public_url": "https://data.example.org/catalog/"}})
        assert load_public_url(tmp_path) == "https://data.example.org/catalog/"

    def test_non_string_value_is_ignored(self, tmp_path: Path) -> None:
        _write_config(tmp_path, {"publish": {"public_url": 42}})
        assert load_public_url(tmp_path) is None


def _minimal_catalog(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "demo",
                "description": "Demo catalog.",
                "links": [],
            }
        ),
        encoding="utf-8",
    )


class TestRunCheckRouting:
    def test_optional_passes_are_off_unless_requested(self, tmp_path: Path) -> None:
        """schema is opt-in; a validator handed in must not run when schema=False."""
        _minimal_catalog(tmp_path)
        calls: list[dict[str, Any]] = []

        def schema_validator(doc: dict[str, Any]) -> list[Any]:
            calls.append(doc)
            return []

        run_check(tmp_path, data=False, geo_assets=False, schema_validator=schema_validator)
        assert calls == []

    def test_schema_validator_runs_when_schema_requested(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        calls: list[dict[str, Any]] = []

        def schema_validator(doc: dict[str, Any]) -> list[Any]:
            calls.append(doc)
            return []

        run_check(
            tmp_path,
            data=False,
            geo_assets=False,
            schema=True,
            schema_validator=schema_validator,
        )
        assert len(calls) == 1

    def test_structural_validator_is_injected(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        calls: list[dict[str, Any]] = []

        def structural_validator(doc: dict[str, Any]) -> list[Any]:
            calls.append(doc)
            return []

        run_check(
            tmp_path,
            data=False,
            geo_assets=False,
            structural_validator=structural_validator,
        )
        assert len(calls) == 1

    def test_live_prober_only_runs_with_live(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        probed: list[str] = []

        def prober(url: str) -> Any:
            probed.append(url)
            raise AssertionError("no assets to probe in this catalog")

        run_check(tmp_path, data=False, geo_assets=False, live_prober=prober)
        assert probed == []

    def test_geo_assets_off_leaves_format_report_none(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        outcome = run_check(tmp_path, data=False, geo_assets=False)
        assert outcome.format_report is None

    def test_geo_assets_on_produces_a_format_report(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        outcome = run_check(tmp_path, data=False, geo_assets=True)
        assert outcome.format_report is not None
        assert outcome.format_report.total == 0

    def test_metadata_off_leaves_report_none(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        outcome = run_check(tmp_path, metadata=False, geo_assets=True)
        assert outcome.report is None

    def test_config_disabled_rules_are_honored(self, tmp_path: Path) -> None:
        """A rule the config disables produces no findings."""
        _minimal_catalog(tmp_path)
        before = run_check(tmp_path, data=False, structural=False, geo_assets=False)
        assert before.report is not None
        fired = {f.rule_id for f in before.report.findings}
        assert "PTL-CNF-001" in fired

        _write_config(tmp_path, {"check": {"disabled": ["PTL-CNF-001"]}})
        after = run_check(tmp_path, data=False, structural=False, geo_assets=False)
        assert after.report is not None
        assert "PTL-CNF-001" not in {f.rule_id for f in after.report.findings}

    def test_root_is_resolved_from_a_subdirectory(self, tmp_path: Path) -> None:
        """Checking a nested path validates the catalog it belongs to."""
        _minimal_catalog(tmp_path)
        nested = tmp_path / "roads"
        nested.mkdir()
        outcome = run_check(nested, data=False, structural=False, geo_assets=False)
        assert outcome.report is not None
        assert outcome.report.files_checked == 1


class TestDataScope:
    """``data_scope`` selects the rashid reader the data pass uses."""

    def test_scope_all_uses_the_default_reader(self, tmp_path: Path, monkeypatch: Any) -> None:
        _minimal_catalog(tmp_path)
        captured: dict[str, Any] = {}

        def fake_validate(root: Path, **kwargs: Any) -> Any:
            captured.update(kwargs)
            from rashid.model import Report

            return Report(findings=[], files_checked=0)

        monkeypatch.setattr("portolan_cli.validation.runner.validate", fake_validate)
        run_check(tmp_path, data=True, geo_assets=False)
        assert captured["data_reader_factory"] is None

    def test_scope_local_uses_the_local_only_reader(self, tmp_path: Path, monkeypatch: Any) -> None:
        from rashid.data.reader import LocalOnlyReader

        _minimal_catalog(tmp_path)
        captured: dict[str, Any] = {}

        def fake_validate(root: Path, **kwargs: Any) -> Any:
            captured.update(kwargs)
            from rashid.model import Report

            return Report(findings=[], files_checked=0)

        monkeypatch.setattr("portolan_cli.validation.runner.validate", fake_validate)
        run_check(tmp_path, data=True, data_scope="local", geo_assets=False)
        assert captured["data_reader_factory"] is LocalOnlyReader


class TestLiveHint:
    def test_hint_when_published_and_live_not_requested(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        _write_config(tmp_path, {"publish": {"public_url": "https://data.example.org/c/"}})
        outcome = run_check(tmp_path, data=False, structural=False, geo_assets=False)
        assert outcome.live_hint is not None
        assert outcome.live_hint.base_url == "https://data.example.org/c/"

    def test_no_hint_when_live_was_requested(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        _write_config(tmp_path, {"publish": {"public_url": "https://data.example.org/c/"}})
        outcome = run_check(
            tmp_path,
            data=False,
            structural=False,
            geo_assets=False,
            live=True,
            live_prober=lambda url: None,
        )
        assert outcome.live_hint is None

    def test_no_hint_when_unpublished(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        outcome = run_check(tmp_path, data=False, structural=False, geo_assets=False)
        assert outcome.live_hint is None

    def test_explicit_url_overrides_config(self, tmp_path: Path) -> None:
        _minimal_catalog(tmp_path)
        _write_config(tmp_path, {"publish": {"public_url": "https://config.example.org/"}})
        outcome = run_check(
            tmp_path,
            data=False,
            structural=False,
            geo_assets=False,
            public_url="https://flag.example.org/",
        )
        assert outcome.live_hint is not None
        assert outcome.live_hint.base_url == "https://flag.example.org/"


class TestMissingRoot:
    def test_no_catalog_json_reports_the_generic_error(self, tmp_path: Path) -> None:
        outcome = run_check(tmp_path, data=False, structural=False, geo_assets=False)
        assert outcome.report is not None
        assert [f.rule_id for f in outcome.report.findings] == ["PTL-GEN-000"]
