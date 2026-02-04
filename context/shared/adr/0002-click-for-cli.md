# ADR-0002: Click for CLI Framework

## Status
Accepted

## Context

portolan-cli needs a CLI framework to provide a user-friendly command-line interface. The framework should:

- Support nested commands and command groups
- Handle argument parsing and validation
- Provide help text generation
- Be well-documented and stable
- Work well with type hints and mypy

We have prior experience with Click from geoparquet-io, where it proved effective.

## Decision

We use **Click** as the CLI framework for portolan-cli.

```python
import click

@click.group()
def cli():
    """Portolan CLI - manage cloud-native geospatial data."""
    pass

@cli.command()
@click.argument("input_path")
@click.option("--output", "-o", help="Output path")
def convert(input_path: str, output: str | None) -> None:
    """Convert geospatial data between formats."""
    ...
```

## Consequences

### What becomes easier

- **Proven pattern** — We already know Click works well for geospatial CLIs (geoparquet-io)
- **Composability** — Commands can be organized into groups naturally
- **Documentation** — Help text is auto-generated from docstrings
- **Testing** — Click's `CliRunner` makes CLI testing straightforward
- **Ecosystem** — Rich extensions (click-plugins, click-completion, etc.)

### What becomes harder

- **Type hints** — Click's decorators can be tricky with strict mypy (requires careful annotation)
- **Async** — Click is synchronous; async commands require workarounds

### Trade-offs

- We accept Click's decorator-heavy style for its proven reliability
- We accept synchronous architecture (can wrap async internals if needed)

## Alternatives Considered

### 1. Typer
**Considered but rejected:** Typer is built on Click and adds type-hint-based argument parsing. While attractive, we prefer Click's explicit decorator style and have more experience with it. Typer's magic can make debugging harder.

### 2. argparse (stdlib)
**Rejected:** Too verbose for complex CLIs. No built-in support for command groups. Help text formatting is limited.

### 3. Fire
**Rejected:** Too magical — auto-generates CLI from function signatures. Makes it hard to control the exact CLI interface and documentation.

### 4. Cyclopts
**Rejected:** Newer library, less ecosystem support and documentation. Higher risk for a foundational choice.
