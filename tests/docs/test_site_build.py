"""Check the built site for failures that ``zensical build --strict`` does not report."""

from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
import sys
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


def _copy_working_tree(dest: Path) -> None:
    """Copy each tracked or untracked, not ignored, file of the working tree to ``dest``."""
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        check=True,
    ).stdout.decode("utf-8")
    for relative in filter(None, listed.split("\0")):
        source = PROJECT_ROOT / relative
        if not source.is_symlink() and not source.exists():
            continue  # deleted in the working tree
        target = dest / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target, follow_symlinks=False)


@pytest.fixture(scope="session")
def built_site(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a copy of the tree once per xdist worker and return its output directory.
    Zensical has no output flag, so workers that share ``site/`` delete it under each other.
    """
    root = tmp_path_factory.mktemp("site-build")
    _copy_working_tree(root)
    result = subprocess.run(
        [sys.executable, "-m", "zensical", "build", "--strict", "--clean"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"Build failed:\n{result.stdout}\n{result.stderr}"
    site = root / "site"
    assert site.is_dir(), f"Build reported success but {site} does not exist."
    return site


def test_the_site_builds_outside_the_repository(built_site: Path) -> None:
    """Workers that share one output directory delete each other's pages."""
    assert PROJECT_ROOT not in built_site.parents
    assert (built_site / "index.html").is_file()


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
    """Zensical drops an unsupported plugin silently and the build still succeeds."""
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
    """mkdocstrings runs under Zensical and emits object markup."""
    page = _page(built_site, "reference/python/index.html")

    assert "portolan_cli.Catalog" in page
    assert 'class="doc doc-object doc-class"' in page
    assert 'class="doc doc-object doc-function"' in page
    assert "doc-section-title" in page


def test_the_python_reference_renders_the_plugin_surface(built_site: Path) -> None:
    """griffe follows the lazy portolan_cli.api names to their source objects."""
    page = _page(built_site, "reference/python/index.html")

    for anchor in (
        "portolan_cli.api.FormatHandler",
        "portolan_cli.api.LayerSource",
        "portolan_cli.api.extractor_from",
        "portolan_cli.hookspecs.portolan_backends",
        "portolan_cli.testing.check_backend",
    ):
        assert f'id="{anchor}"' in page, anchor
    assert "A data source that serves named layers." in page


def test_the_plugin_guide_includes_the_example_plugin(built_site: Path) -> None:
    """docs/guides/plugins.md takes its code from the example plugin that CI runs."""
    page = _page(built_site, "guides/plugins/index.html")

    for fragment in (
        "portolan_example.hooks",
        "portolan_enrich_item",
        "portolan_extractors",
        "GeoJSONDirectory",
        "check_backend",
    ):
        assert fragment in page, fragment
    assert SNIPPET_DIRECTIVE not in page
    assert "[start:" not in page


def test_the_readme_include_renders_its_source(built_site: Path) -> None:
    """docs/index.md includes README.md from outside docs_dir with ``base_path: .``."""
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
    """docs/changelog.md is a symlink to CHANGELOG.md, and the walker must follow it."""
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
    """Zensical writes its search index to site/search.json, not the MkDocs path."""
    index = json.loads((built_site / "search.json").read_text(encoding="utf-8"))

    assert len(index["items"]) >= 6


def test_the_built_pages_carry_no_dark_mode(built_site: Path) -> None:
    """The brand kit carries no dark theme and no toggle."""
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
    """The brand kit in portolan-ops is the one home for a color."""
    found = LITERAL_COLOR.findall(SITE_STYLES.read_text(encoding="utf-8"))

    assert found == [], f"extra.css restates a color: {found}"


def test_every_brand_token_the_stylesheet_reads_exists() -> None:
    """CSS resolves an undefined custom property to nothing, so a rename drops a color."""
    missing = _brand_tokens_the_stylesheet_reads() - _declared_brand_tokens()

    assert missing == set(), f"The brand kit declares no {sorted(missing)}."


def test_the_pages_load_the_brand_tokens_before_the_overrides(
    built_site: Path,
) -> None:
    page = _page(built_site, "index.html")

    tokens = page.find("assets/stylesheets/_brand-vars.css")
    overrides = page.find("assets/stylesheets/extra.css")

    assert tokens != -1, "The page loads no _brand-vars.css."
    assert overrides != -1, "The page loads no extra.css."
    assert tokens < overrides, "extra.css loads before the brand tokens."


def test_the_pages_request_the_brand_typefaces(built_site: Path) -> None:
    """Material fills an absent ``theme.font`` with Roboto."""
    page = _page(built_site, "index.html")

    assert "Hanken+Grotesk" in page
    assert "JetBrains+Mono" in page
    assert "Roboto" not in page


def test_the_mark_and_the_favicon_come_from_the_sync(built_site: Path) -> None:
    """The header band is blue, so its mark is cream; the favicon is blue."""
    page = _page(built_site, "index.html")

    assert "assets/images/portolan-logomark-fcfcfa.svg" in page
    assert "assets/images/portolan-logomark-4163cc.svg" in page

    for gone in ("icon-white.svg", "icon.svg", "favicon.ico", "logo.svg"):
        assert gone not in page, f"The page still points at {gone}."
