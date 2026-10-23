"""Generate the public CLI, Python API, and configuration reference wrappers.

The generated pages expose only Portolan's supported public API. Run this
script after changing the Click tree, an export list of ``portolan_cli``,
``portolan_cli.api``, or ``portolan_cli.testing``, a hookspec, or the
conversion settings dataclasses.
"""

from __future__ import annotations

import argparse
import inspect
import re
import sys
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING

import click

import portolan_cli
import portolan_cli.api
import portolan_cli.hookspecs
import portolan_cli.testing
from portolan_cli.conversion_config import (
    VALID_SORT_METHODS,
    VALID_SPATIAL_INDEXES,
    VectorSettings,
)
from portolan_cli.plugins.commands import PluginGroup

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

# This module renders the Click tree itself. It replaces a third-party MkDocs
# directive for two reasons. The directive dropped a line only when the line
# held the backspace character, so the 47 two-character markers in this repo's
# docstrings reached the built page. The directive also read the terminal width
# at render time, which a checked-in page cannot depend on.

# A pipe ends a table cell, so every cell escapes it.
_HTML_PIPE = "&#x7C;"

# Click marks a no-rewrap paragraph with a line that holds this marker alone.
# A docstring written with a raw string carries the two-character form.
_VERBATIM_MARKERS = frozenset({"\\b", "\b"})

# The heading level of the root command. `portolan` renders as `###`.
_CLI_DEPTH = 2

# Click's own width is `min(terminal_columns, 80) - 2`. Pin both numbers so the
# page does not change with the size of the window that generated it.
_HELP_WIDTH = 78
_HELP_MAX_WIDTH = 80

# A memory address or an absolute path makes a default machine-specific.
_OPAQUE_DEFAULT = re.compile(r"0x[0-9a-f]+|^/")


def _slug(text: str) -> str:
    """Return the anchor that Python-Markdown derives from a heading.

    This reproduces ``markdown.extensions.toc.slugify(text, "-")`` without the
    import, so the generator needs Click alone. A test asserts that the two
    agree for every command in the shipped tree.
    """
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    stripped = re.sub(r"[^\w\s-]", "", ascii_text).strip().lower()
    return re.sub(r"[-\s]+", "-", stripped)


def _walk(
    command: click.Command,
    parent_ctx: click.Context | None = None,
    prog_name: str = "portolan",
    depth: int = _CLI_DEPTH,
) -> Iterator[tuple[click.Context, int]]:
    """Yield a context and a heading depth for every visible command.

    The bullet list and the full reference both read this walk. One walk means
    the two cannot disagree about which commands ship.
    """
    if command.hidden:
        return

    ctx = command.context_class(
        command, info_name=prog_name, parent=parent_ctx, **command.context_settings
    )
    yield ctx, depth

    if not isinstance(command, click.Group):
        return

    # Read the shipped commands rather than `Group.list_commands(ctx)`, which
    # also lists commands from installed plugins. The reference documents what
    # portolan-cli ships, including the commands it declares as entry points.
    subcommands = (
        command.shipped_commands(ctx) if isinstance(command, PluginGroup) else command.commands
    )
    for name in sorted(subcommands):
        yield from _walk(subcommands[name], parent_ctx=ctx, prog_name=name, depth=depth + 1)


def _command_names() -> list[str]:
    """Return the complete command tree in deterministic display order."""
    paths = [ctx.command_path for ctx, _ in _walk(portolan_cli.cli)]
    # The first entry is the root group, which the bullet list does not repeat.
    return [path.removeprefix("portolan ") for path in paths[1:]]


def _cell(text: str) -> str:
    """Return text that is safe inside a Markdown table cell.

    A newline ends the row and a pipe ends the cell. This removes both.
    """
    return " ".join(text.split()).replace("|", _HTML_PIPE)


def _fenced(block: list[str]) -> list[str]:
    """Return a verbatim help block as a fenced code block."""
    if not block:
        return []
    return ["```text", *block, "```"]


def _command_title(ctx: click.Context, depth: int) -> list[str]:
    """Return the heading for one command.

    The heading text is the full command path, so every anchor is unique. The
    table of contents shows the leaf name, because the tree already shows the
    hierarchy.
    """
    attributes = f"#{_slug(ctx.command_path)} data-toc-label='{ctx.info_name}'"
    return [f"{'#' * (depth + 1)} {ctx.command_path} {{ {attributes} }}", ""]


def _command_description(ctx: click.Context) -> list[str]:
    """Return the command help text, with each no-rewrap block fenced.

    Click marks a preformatted paragraph with a marker on its own line.
    Markdown has no equivalent, so the paragraph becomes a fenced block and the
    marker line never reaches the page.
    """
    help_text = ctx.command.help or ctx.command.short_help
    if not help_text:
        return []

    lines: list[str] = []
    verbatim: list[str] = []
    in_verbatim = False
    for line in inspect.cleandoc(help_text).splitlines():
        if line.strip() in _VERBATIM_MARKERS:
            lines.extend(_fenced(verbatim))
            verbatim = []
            in_verbatim = True
            continue
        if in_verbatim:
            if line.strip():
                verbatim.append(line)
                continue
            lines.extend(_fenced(verbatim))
            verbatim = []
            in_verbatim = False
        lines.append(line)
    lines.extend(_fenced(verbatim))
    return [*lines, ""]


def _command_usage(ctx: click.Context) -> list[str]:
    """Return the usage line for one command.

    This builds the formatter rather than call ``ctx.make_formatter()``, which
    reads the terminal size. A checked-in page must not change with the window
    that generated it.
    """
    formatter = click.HelpFormatter(width=_HELP_WIDTH, max_width=_HELP_MAX_WIDTH)
    formatter.write_usage(
        ctx.command_path, " ".join(ctx.command.collect_usage_pieces(ctx)), prefix=""
    )
    return ["**Usage:**", "", "```text", formatter.getvalue().strip(), "```", ""]


def _range_bounds(param_type: click.IntRange | click.FloatRange) -> str:
    """Return the readable bounds of a numeric range type."""
    if param_type.min is not None and param_type.max is not None:
        return f"between `{param_type.min}` and `{param_type.max}`"
    if param_type.min is not None:
        return f"`{param_type.min}` and above"
    return f"`{param_type.max}` and below"


def _option_type(option: click.Option) -> str:
    """Return a readable type name for one option."""
    type_name = option.type.name
    if isinstance(option.type, click.Choice):
        choices = f" {_HTML_PIPE} ".join(f"`{choice}`" for choice in option.type.choices)
        return f"{type_name} ({choices})"
    if isinstance(option.type, click.DateTime):
        formats = f" {_HTML_PIPE} ".join(f"`{fmt}`" for fmt in option.type.formats)
        return f"{type_name} ({formats})"
    if isinstance(option.type, click.IntRange | click.FloatRange):
        return f"{type_name} ({_range_bounds(option.type)})"
    return type_name


def _reject_opaque_default(ctx: click.Context, option: click.Option) -> None:
    """Stop the generator when a default would differ between two machines.

    A memory address or an absolute path ties the checked-in page to the
    machine that wrote it. ``--check`` then fails in CI for no real reason.
    """
    if option.default is None:
        return
    if _OPAQUE_DEFAULT.search(str(option.default)):
        message = (
            f"{ctx.command_path} {option.opts}: the default {option.default!r} names a "
            "memory address or an absolute path. Give the option a stable default, "
            "or mark it hidden."
        )
        raise ValueError(message)


def _option_row(option: click.Option) -> str:
    """Return one Markdown table row for an option."""
    names = ", ".join(f"`{opt}`" for opt in option.opts)
    if option.secondary_opts:
        names += " / " + ", ".join(f"`{opt}`" for opt in option.secondary_opts)

    description = _cell(option.help) if option.help else "N/A"
    if option.default is None:
        default = "_required" if option.required else "None"
    else:
        default = f"`{_cell(str(option.default))}`"
    return f"| {names} | {_option_type(option)} | {description} | {default} |"


def _command_options(ctx: click.Context) -> list[str]:
    """Return the options table for one command."""
    options = [
        param
        for param in ctx.command.get_params(ctx)
        if isinstance(param, click.Option) and not param.hidden
    ]
    if not options:
        return []

    for option in options:
        _reject_opaque_default(ctx, option)

    return [
        "**Options:**",
        "",
        "| Name | Type | Description | Default |",
        "| ---- | ---- | ----------- | ------- |",
        *[_option_row(option) for option in options],
        "",
    ]


def _command_arguments(ctx: click.Context) -> list[str]:
    """Return the arguments table for one command.

    Click holds no help text for an argument. ``Argument.get_help_record``
    returns ``None`` by design. This table therefore gives the name, the type,
    and whether the argument is required. Each command's own help text states
    what the argument means.
    """
    arguments = [
        param for param in ctx.command.get_params(ctx) if isinstance(param, click.Argument)
    ]
    if not arguments:
        return []

    rows = [
        f"| `{_cell(param.make_metavar(ctx))}` | {param.type.name} |"
        f" {'yes' if param.required else 'no'} |"
        for param in arguments
    ]
    return [
        "**Arguments:**",
        "",
        "| Name | Type | Required |",
        "| ---- | ---- | -------- |",
        *rows,
        "",
    ]


def _command_section(ctx: click.Context, depth: int) -> list[str]:
    """Return every Markdown line for one command."""
    return [
        *_command_title(ctx, depth),
        *_command_description(ctx),
        *_command_usage(ctx),
        *_command_arguments(ctx),
        *_command_options(ctx),
    ]


def _cli_reference() -> str:
    """Build the CLI reference page from the shipped Click tree."""
    commands = "\n".join(f"- `portolan {name}`" for name in _command_names())
    sections: list[str] = []
    for ctx, depth in _walk(portolan_cli.cli):
        sections.extend(_command_section(ctx, depth))
    body = "\n".join(sections).rstrip()
    return f"""# CLI Reference

<!-- Generated by scripts/generate_reference_docs.py. Do not edit. -->

This page is generated from the shipped Click command tree.

## Commands

{commands}

## Full Reference

{body}
"""


def _api_groups() -> list[tuple[str, list[str]]]:
    """Group the ``portolan_cli.api`` names as its module docstring does.

    Each ``Groups:`` bullet names its members in double backticks. One bullet
    may name no member. It receives every name that the other bullets leave
    out, in ``__all__`` order. A name that no bullet places stops the build.
    """
    doc = inspect.getdoc(portolan_cli.api) or ""
    _, _, listing = doc.partition("Groups:\n")
    bullets: list[str] = []
    for line in listing.splitlines():
        if line.startswith("- "):
            bullets.append(line[2:])
        elif line.startswith("  ") and bullets:
            bullets[-1] += " " + line.strip()

    exported = set(portolan_cli.api.__all__)
    placed: set[str] = set()
    groups: list[tuple[str, list[str]]] = []
    rest_index: int | None = None
    for bullet in bullets:
        title, _, body = bullet.partition(": ")
        names = [
            name
            for name in dict.fromkeys(re.findall(r"``(\w+)``", body))
            if name in exported and name not in placed
        ]
        if not names:
            if rest_index is not None:
                raise ValueError("Two groups in the portolan_cli.api docstring name no member.")
            rest_index = len(groups)
        placed.update(names)
        groups.append((title, names))

    rest = [name for name in portolan_cli.api.__all__ if name not in placed]
    if rest and rest_index is None:
        raise ValueError(f"No group in the portolan_cli.api docstring places {rest}.")
    if rest_index is not None:
        groups[rest_index] = (groups[rest_index][0], rest)
    return groups


def _hook_names() -> list[str]:
    """Return the hooks that pluggy reads from ``portolan_cli.hookspecs``."""
    marker = f"{portolan_cli.hookspecs.PROJECT_NAME}_spec"
    return [name for name, value in vars(portolan_cli.hookspecs).items() if hasattr(value, marker)]


def _members(module: str, names: Sequence[str], heading_level: int | None = None) -> str:
    """Return one mkdocstrings directive per name, optionally below a subheading."""
    options = f"    options:\n      heading_level: {heading_level}\n" if heading_level else ""
    return "\n".join(f"::: {module}.{name}\n{options}" for name in names)


def _api_reference() -> str:
    """Build the MkDocs wrapper for the explicit supported Python exports."""
    top_level = _members("portolan_cli", portolan_cli.__all__)
    plugin_api = "\n".join(
        f"### {title}\n\n{_members('portolan_cli.api', names, heading_level=4)}"
        for title, names in _api_groups()
    )
    hooks = _members("portolan_cli.hookspecs", _hook_names())
    testing = _members("portolan_cli.testing", portolan_cli.testing.__all__)
    return f"""# Python API Reference

<!-- Generated by scripts/generate_reference_docs.py. Do not edit. -->

This page documents the supported Python API. Internal modules are not a
supported interface. To extend Portolan, follow the
[plugin guide](../guides/plugins.md) and use the APIs on this page.

## Top-level exports

{top_level}
## Plugin API

Import these names from `portolan_cli.api`. The module follows semantic
versioning.

{plugin_api}
## Plugin hooks

Use `@hookimpl` to implement these hooks from `portolan_cli.hookspecs`.

{hooks}
## Plugin testing

Call these checks from `portolan_cli.testing` in your plugin tests.

{testing}"""


def _reference_index() -> str:
    """Build the generated Reference navigation landing page."""
    return """# Reference

<!-- Generated by scripts/generate_reference_docs.py. Do not edit. -->

- [CLI Reference](cli.md)
- [Python API Reference](python.md)
- [Configuration Reference](configuration.md)
"""


def _configuration_reference() -> str:
    """Build the conversion configuration reference from the shipped defaults.

    Every default is read from :class:`VectorSettings` rather than written out,
    so a change to the dataclass cannot leave the page stale (issue #805).
    """
    defaults = VectorSettings()
    # A literal pipe would end the table cell, so the alternatives are escaped.
    indexes = r" \| ".join(sorted(VALID_SPATIAL_INDEXES))
    sorts = r" \| ".join(sorted(VALID_SORT_METHODS))
    rows = "\n".join(
        [
            f"| `spatial_index` | {indexes} | `{defaults.spatial_index}` |"
            " Spatial index column to add. |",
            f"| `resolution` | auto \\| integer | `{defaults.resolution}` |"
            " Index resolution. `auto` uses the geoparquet-io defaults. |",
            f"| `sort` | {sorts} | `{defaults.sort}` | Row ordering method. |",
            f"| `add_bbox` | true \\| false | `{str(defaults.add_bbox).lower()}` |"
            " Add a bbox covering column. |",
            f"| `partition` | true \\| false | `{str(defaults.partition).lower()}` |"
            " Produce hive-partitioned output. Requires a spatial index. |",
        ]
    )
    return f"""# Configuration Reference

<!-- Generated by scripts/generate_reference_docs.py. Do not edit. -->

Portolan reads catalog settings from `.portolan/config.yaml`.

## Vector Conversion

The `conversion.vector` block controls how Portolan writes GeoParquet.

| Setting | Values | Default | Meaning |
|---------|--------|---------|---------|
{rows}

```yaml
conversion:
  vector:
    sort: {defaults.sort}
    add_bbox: {str(defaults.add_bbox).lower()}
```

## Conforming Output by Default

`sort` and `add_bbox` carry the values the Portolan GeoParquet profile
requires. rashid reads both `PTL-DAT-006` and `PTL-DAT-007` through the bbox
covering column, so a file without that column fails `portolan check`. Set
`sort: none` or `add_bbox: false` to turn either off.

These settings apply to `portolan add`, `portolan convert`, and `portolan
extract arcgis`. `extract arcgis --raw` writes no catalog, so it uses the
defaults.

`add` also rewrites a GeoParquet you hand it when that file carries no
covering column. A file that already has one is copied untouched, so a large
conformant file costs nothing. `--force` applies the same test.

The rewrite is a full read, sort, and write, not a copy. `add` reports the
file size before it starts, and it needs free space for a second copy of the
file while it runs. Above 1GB `add` also prints a warning, because the read
and the sort take long enough to look like a stall. Every schema metadata key
the file carried is restored afterwards, `pandas` and publisher keys included.

## The Rewrite Never Loses Data

`add` compares the rewritten file against the source before it swaps them in.
It checks the row count, the column set, and the declared CRS. It keeps your
file and prints the reason when any of those would be lost. The file then
stays non-conformant, and `check` reports it. A projected GeoParquet keeps its
CRS through the rewrite:

```console
$ portolan add roads/data.parquet
→ Rewriting data.parquet (11.1KB): it carries no bbox covering column
✓ Added 1 file to 1 collection
```

`add` does not inspect row order. To reorder a file that has the column but is
not sorted, run `portolan add <path> --force --reconvert`. That is also the
way to apply `sort` to an existing file when you set `add_bbox: false`,
because the footer then shows nothing `add` can act on.
"""


def generated_pages(project_root: Path) -> dict[Path, str]:
    """Return generated reference pages, keyed by project-relative path."""
    _ = project_root
    return {
        Path("docs/reference/cli.md"): _cli_reference(),
        Path("docs/reference/python.md"): _api_reference(),
        Path("docs/reference/configuration.md"): _configuration_reference(),
        Path("docs/reference/index.md"): _reference_index(),
    }


def _write_pages(project_root: Path) -> None:
    """Write every generated page to the repository."""
    for relative_path, content in generated_pages(project_root).items():
        target = project_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def _check_pages(project_root: Path) -> bool:
    """Return whether every checked-in generated page is current."""
    is_current = True
    for relative_path, expected in generated_pages(project_root).items():
        target = project_root / relative_path
        actual = target.read_text(encoding="utf-8") if target.exists() else ""
        if actual != expected:
            print(f"Generated reference is stale: {relative_path}", file=sys.stderr)
            is_current = False
    return is_current


def main(argv: Sequence[str] | None = None) -> int:
    """Generate reference wrappers or report stale checked-in output."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated reference pages differ from checked-in files.",
    )
    arguments = parser.parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]

    if arguments.check:
        return 0 if _check_pages(project_root) else 1

    _write_pages(project_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
