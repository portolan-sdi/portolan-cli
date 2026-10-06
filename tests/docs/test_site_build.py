"""Tests for the built documentation site.

Zensical builds this site. `zensical build --strict` reports a dead
link and a dead anchor, and it stops the build. It does not report two
other failures. A dropped plugin and an empty snippet include both
produce a clean build and a wrong page. These tests close that gap.
"""

from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# The pymdownx.snippets directive. A built page must never show it.
SNIPPET_DIRECTIVE = "--8<--"

# The sync in portolan-ops owns this file. It declares every brand
# token on `:root`.
BRAND_TOKENS = PROJECT_ROOT / "docs/assets/stylesheets/_brand-vars.css"

# This repository owns this file. It maps the brand tokens onto the
# Material surface.
SITE_STYLES = PROJECT_ROOT / "docs/assets/stylesheets/extra.css"

# A literal color value. The brand kit is the one home for these.
LITERAL_COLOR = re.compile(r"#[0-9a-fA-F]{3,8}\b|rgba?\(")


@pytest.fixture(scope="session")
def built_site() -> Path:
    """Build the site once and return the output directory.

    Remove the output directory first. `zensical build --clean` clears
    the cache and keeps the output. A page whose source is gone stays
    on disk, and a test that reads it passes against stale HTML.
    """
    site = PROJECT_ROOT / "site"
    shutil.rmtree(site, ignore_errors=True)

    result = subprocess.run(
        ["uv", "run", "zensical", "build", "--strict", "--clean"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"Build failed:\n{result.stdout}\n{result.stderr}"
    assert site.is_dir(), f"Build reported success but {site} does not exist."
    return site


def _page(site: Path, relative: str) -> str:
    """Read one built page."""
    path = site / relative
    assert path.is_file(), f"The build wrote no {relative}."
    return path.read_text(encoding="utf-8")


def _declared_plugin_names() -> list[str]:
    """Return every plugin name that mkdocs.yml declares."""
    # SafeLoader rejects the `!!python/name:` tags in markdown_extensions,
    # so read the plugins block on its own.
    source = (PROJECT_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    block = re.search(r"^plugins:\n(?:[ \t#].*\n|\n)*", source, re.MULTILINE)
    assert block is not None, "mkdocs.yml declares no plugins block."

    entries = yaml.safe_load(block.group(0))["plugins"]
    # A plugin entry is either a bare name or a single-key mapping of
    # the name to its options.
    return [entry if isinstance(entry, str) else next(iter(entry)) for entry in entries]


def _first_prose_line(relative: str) -> str:
    """Return the first prose line of a source Markdown file."""
    for line in (PROJECT_ROOT / relative).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith(("#", "[", "!", "<", "-")):
            return stripped
    raise AssertionError(f"{relative} holds no prose line.")


def test_every_declared_plugin_survives_the_config_parse() -> None:
    """Zensical keeps each plugin that mkdocs.yml declares.

    `_convert_plugins` returns early for a name it does not support. The
    drop is silent and the build still succeeds.
    """
    from zensical.config import parse_config

    parsed = parse_config(str(PROJECT_ROOT / "mkdocs.yml"))
    supported = parsed["plugins"]

    for name in _declared_plugin_names():
        assert name in supported, f"Zensical dropped the {name} plugin."


def test_the_mkdocstrings_handler_options_survive_the_config_parse() -> None:
    """The parsed config keeps the python handler options from mkdocs.yml."""
    from zensical.config import parse_config

    parsed = parse_config(str(PROJECT_ROOT / "mkdocs.yml"))
    options = parsed["plugins"]["mkdocstrings"]["config"]["handlers"]["python"]["options"]

    assert options["docstring_style"] == "google"
    assert options["show_source"] is False


def test_the_python_reference_renders_real_signatures(built_site: Path) -> None:
    """mkdocstrings runs under Zensical and emits object markup.

    A configured handler is not a handler that ran. This page is the one
    place the handler does real work.
    """
    page = _page(built_site, "reference/python/index.html")

    assert "portolan_cli.Catalog" in page
    assert 'class="doc doc-object doc-class"' in page
    assert 'class="doc doc-object doc-function"' in page
    assert "doc-section-title" in page


def test_the_readme_include_renders_its_source(built_site: Path) -> None:
    """docs/index.md includes README.md from outside docs_dir.

    The include depends on `base_path: .`. An empty include produces a
    page that builds and says nothing.
    """
    page = _page(built_site, "index.html")

    assert html.escape(_first_prose_line("README.md"), quote=False) in page
    assert SNIPPET_DIRECTIVE not in page


def test_the_example_include_renders_its_source(built_site: Path) -> None:
    """docs/examples.md includes the example README from outside docs_dir."""
    source = "examples/philadelphia-housing/README.md"
    page = _page(built_site, "examples/index.html")

    assert html.escape(_first_prose_line(source), quote=False) in page
    assert SNIPPET_DIRECTIVE not in page


def test_the_changelog_symlink_reaches_the_built_site(built_site: Path) -> None:
    """docs/changelog.md is a symlink to CHANGELOG.md.

    Zensical walks the docs directory in Rust. A walker that skips a
    symlink drops this page from the published site.
    """
    page = _page(built_site, "changelog/index.html")

    # The template chrome alone is over 16 kB, so a size floor proves
    # nothing. Read the newest release heading from the source instead.
    heading = next(
        line[3:].strip()
        for line in (PROJECT_ROOT / "CHANGELOG.md").read_text(encoding="utf-8").splitlines()
        if line.startswith("## ")
    )

    assert html.escape(heading, quote=False) in page


def test_the_search_index_covers_every_page(built_site: Path) -> None:
    """Zensical writes its search index to site/search.json.

    The path and the shape both differ from the MkDocs search plugin.
    MkDocs writes site/search/search_index.json with a `docs` key.
    """
    index = json.loads((built_site / "search.json").read_text(encoding="utf-8"))

    assert len(index["items"]) >= 6


def test_the_built_pages_carry_no_dark_mode(built_site: Path) -> None:
    """The brand kit carries no dark theme and no toggle.

    mkdocs.yml states light mode only. A palette toggle added by mistake
    emits the slate scheme into the markup.
    """
    page = _page(built_site, "index.html")

    assert 'data-md-color-scheme="slate"' not in page


def _declared_brand_tokens() -> set[str]:
    """Return every token that the synced brand stylesheet declares."""
    source = BRAND_TOKENS.read_text(encoding="utf-8")
    return set(re.findall(r"^\s*(--[a-z0-9-]+):", source, re.MULTILINE))


def _brand_tokens_the_stylesheet_reads() -> set[str]:
    """Return every brand token that extra.css reads."""
    source = SITE_STYLES.read_text(encoding="utf-8")
    return set(re.findall(r"var\((--(?:palette|color)-[a-z0-9-]+)\)", source))


def test_the_stylesheet_restates_no_brand_value() -> None:
    """extra.css holds no hex value and no rgba() call.

    The brand kit in portolan-ops is the one home for a color. A restated
    value drifts from its source and nothing reports the drift.
    """
    found = LITERAL_COLOR.findall(SITE_STYLES.read_text(encoding="utf-8"))

    assert found == [], f"extra.css restates a color: {found}"


def test_every_brand_token_the_stylesheet_reads_exists() -> None:
    """Each brand token that extra.css reads exists in the synced file.

    CSS resolves an undefined custom property to nothing. A rename in
    portolan-ops therefore removes a color and the build stays green.
    """
    missing = _brand_tokens_the_stylesheet_reads() - _declared_brand_tokens()

    assert missing == set(), f"The brand kit declares no {sorted(missing)}."


def test_the_pages_load_the_brand_tokens_before_the_overrides(
    built_site: Path,
) -> None:
    """The brand stylesheet loads before extra.css.

    `_brand-vars.css` declares the tokens and extra.css reads them. The
    reverse order leaves the Material overrides unresolved.
    """
    page = _page(built_site, "index.html")

    tokens = page.find("assets/stylesheets/_brand-vars.css")
    overrides = page.find("assets/stylesheets/extra.css")

    assert tokens != -1, "The page loads no _brand-vars.css."
    assert overrides != -1, "The page loads no extra.css."
    assert tokens < overrides, "extra.css loads before the brand tokens."


def test_the_pages_request_the_brand_typefaces(built_site: Path) -> None:
    """The page requests Hanken Grotesk and JetBrains Mono.

    Material fills an absent `theme.font` with Roboto. The site then
    renders in a typeface that the brand kit does not name.
    """
    page = _page(built_site, "index.html")

    assert "Hanken+Grotesk" in page
    assert "JetBrains+Mono" in page
    assert "Roboto" not in page


def test_the_mark_and_the_favicon_come_from_the_sync(built_site: Path) -> None:
    """The header mark and the favicon point at the synced files.

    The header is a solid blue band, so the mark on it is cream. The
    favicon sits on a browser tab, so it is blue.
    """
    page = _page(built_site, "index.html")

    assert "assets/images/portolan-logomark-fcfcfa.svg" in page
    assert "assets/images/portolan-logomark-4163cc.svg" in page

    for gone in ("icon-white.svg", "icon.svg", "favicon.ico", "logo.svg"):
        assert gone not in page, f"The page still points at {gone}."
