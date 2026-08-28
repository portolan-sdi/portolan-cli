"""Unit tests for own-over-inherited metadata precedence (issue #755).

``portolan add`` used to overwrite a collection's title, license, and providers
with values merged down from the catalog root. ``apply_human_metadata`` fixes
that: a value the collection owns wins, and an inherited value only fills a field
the collection still lacks.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from portolan_cli.finalization import apply_human_metadata
from portolan_cli.stac import DEFAULT_LICENSE, create_collection

pytestmark = pytest.mark.unit


def _write_metadata(directory: Path, data: dict[str, object]) -> None:
    portolan = directory / ".portolan"
    portolan.mkdir(parents=True, exist_ok=True)
    (portolan / "metadata.yaml").write_text(yaml.dump(data), encoding="utf-8")


def _collection_dir(catalog_root: Path, collection_id: str = "roads") -> Path:
    collection_dir = catalog_root / collection_id
    collection_dir.mkdir(parents=True, exist_ok=True)
    return collection_dir


class TestTitlePrecedence:
    def test_inherited_title_never_overwrites_a_collection_title(self, tmp_path: Path) -> None:
        """The catalog title must not become the collection title (issue #755)."""
        _write_metadata(tmp_path, {"title": "Root Catalog"})
        collection_dir = _collection_dir(tmp_path)
        collection = create_collection(collection_id="roads", description="Roads", title="Roads")

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.title == "Roads"

    def test_own_title_wins(self, tmp_path: Path) -> None:
        _write_metadata(tmp_path, {"title": "Root Catalog"})
        collection_dir = _collection_dir(tmp_path)
        _write_metadata(collection_dir, {"title": "City Roads"})
        collection = create_collection(collection_id="roads", description="Roads", title="Roads")

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.title == "City Roads"


class TestDescriptionPrecedence:
    def test_inherited_description_never_overwrites_a_collection_description(
        self, tmp_path: Path
    ) -> None:
        """An ancestor description must not become the collection description (issue #755)."""
        _write_metadata(tmp_path, {"description": "Root catalog description"})
        collection_dir = _collection_dir(tmp_path)
        collection = create_collection(
            collection_id="roads", description="Road network of the city"
        )

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.description == "Road network of the city"

    def test_own_description_wins(self, tmp_path: Path) -> None:
        _write_metadata(tmp_path, {"description": "Root catalog description"})
        collection_dir = _collection_dir(tmp_path)
        _write_metadata(collection_dir, {"description": "City road network"})
        collection = create_collection(collection_id="roads", description="Roads")

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.description == "City road network"


class TestLicensePrecedence:
    def test_inherited_license_fills_placeholder(self, tmp_path: Path) -> None:
        _write_metadata(tmp_path, {"license": "CC-BY-4.0"})
        collection_dir = _collection_dir(tmp_path)
        collection = create_collection(collection_id="roads", description="Roads")
        assert collection.license == DEFAULT_LICENSE

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.license == "CC-BY-4.0"

    def test_inherited_license_does_not_overwrite_a_real_license(self, tmp_path: Path) -> None:
        _write_metadata(tmp_path, {"license": "CC-BY-4.0"})
        collection_dir = _collection_dir(tmp_path)
        collection = create_collection(collection_id="roads", description="Roads", license="MIT")

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.license == "MIT"

    def test_own_license_wins_over_inherited(self, tmp_path: Path) -> None:
        _write_metadata(tmp_path, {"license": "CC-BY-4.0"})
        collection_dir = _collection_dir(tmp_path)
        _write_metadata(collection_dir, {"license": "ODbL-1.0"})
        collection = create_collection(collection_id="roads", description="Roads", license="MIT")

        apply_human_metadata(collection, collection_dir, tmp_path)

        assert collection.license == "ODbL-1.0"


class TestProvidersPrecedence:
    def test_inherited_providers_fill_missing_providers(self, tmp_path: Path) -> None:
        _write_metadata(
            tmp_path,
            {
                "providers": [{"name": "City GIS", "roles": ["producer", "host"]}],
            },
        )
        collection_dir = _collection_dir(tmp_path)
        collection = create_collection(collection_id="roads", description="Roads")
        assert not collection.providers

        apply_human_metadata(collection, collection_dir, tmp_path)

        names = [provider.name for provider in collection.providers or []]
        assert names == ["City GIS"]

    def test_own_contact_regenerates_providers(self, tmp_path: Path) -> None:
        """An owned contact rebuilds providers, even over a hand-edited array.

        A collection's own metadata.yaml is authoritative. A contact it declares
        seeds the host provider, so the resolved providers replace the array.
        """
        collection_dir = _collection_dir(tmp_path)
        _write_metadata(
            collection_dir,
            {"contact": {"name": "City GIS", "email": "gis@city.example"}},
        )
        collection = create_collection(collection_id="roads", description="Roads")
        import pystac

        collection.providers = [pystac.Provider(name="Hand Edited", roles=["producer", "host"])]

        apply_human_metadata(collection, collection_dir, tmp_path)

        names = [provider.name for provider in collection.providers or []]
        assert names == ["City GIS"]

    def test_inherited_providers_do_not_overwrite_existing(self, tmp_path: Path) -> None:
        """A maintainer's hand-edited providers survive a re-add (issue #755)."""
        _write_metadata(
            tmp_path,
            {"providers": [{"name": "Root Org", "roles": ["producer", "host"]}]},
        )
        collection_dir = _collection_dir(tmp_path)
        collection = create_collection(collection_id="roads", description="Roads")
        # The collection already carries providers a human wrote by hand.
        import pystac

        collection.providers = [pystac.Provider(name="Hand Edited", roles=["producer", "host"])]

        apply_human_metadata(collection, collection_dir, tmp_path)

        names = [provider.name for provider in collection.providers or []]
        assert names == ["Hand Edited"]

    def test_own_providers_win_over_existing(self, tmp_path: Path) -> None:
        collection_dir = _collection_dir(tmp_path)
        _write_metadata(
            collection_dir,
            {"providers": [{"name": "Own Org", "roles": ["producer", "host"]}]},
        )
        collection = create_collection(collection_id="roads", description="Roads")
        import pystac

        collection.providers = [pystac.Provider(name="Stale", roles=["producer", "host"])]

        apply_human_metadata(collection, collection_dir, tmp_path)

        names = [provider.name for provider in collection.providers or []]
        assert names == ["Own Org"]
