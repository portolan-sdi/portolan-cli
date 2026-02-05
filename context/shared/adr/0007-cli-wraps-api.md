# ADR-0007: CLI Wraps Python API

## Status
Accepted

## Context

Portolan needs both a CLI (for humans and scripts) and a Python API (for programmatic use, testing, and AI integration). Two approaches:

1. **CLI-first:** Logic lives in CLI commands, Python API shells out or duplicates
2. **API-first:** Logic lives in Python library, CLI is a thin wrapper

## Decision

**CLI wraps the API.** All logic lives in the Python library layer. The CLI is a thin layer of Click decorators that parse arguments and call library functions.

### Architecture

```
┌─────────────────────────────────────────┐
│              CLI Layer                  │
│  (Click decorators, argument parsing,   │
│   output formatting, error handling)    │
└─────────────────┬───────────────────────┘
                  │ calls
                  ▼
┌─────────────────────────────────────────┐
│           Python API Layer              │
│  (Catalog class, format handlers,       │
│   sync logic, validation, versioning)   │
└─────────────────────────────────────────┘
```

### Example

```python
# portolan_cli/api/catalog.py
class Catalog:
    def add(self, path: Path, *, title: str | None = None, auto: bool = False) -> Dataset:
        """Add a dataset to the catalog."""
        # All logic here
        ...

# portolan_cli/cli/dataset.py
@click.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--title", help="Dataset title")
@click.option("--auto", is_flag=True, help="Use smart defaults")
def add(path: str, title: str | None, auto: bool) -> None:
    """Add a dataset to the catalog."""
    catalog = Catalog.load()
    dataset = catalog.add(Path(path), title=title, auto=auto)
    success(f"Added {dataset.name}")
```

### What the CLI layer handles
- Argument parsing and validation
- Output formatting (colors, tables, progress bars)
- Error message presentation
- Exit codes

### What the API layer handles
- All business logic
- File I/O
- Format conversion dispatch
- Sync operations
- Validation rules

## Consequences

### What becomes easier
- **Testing** — Unit test the API without CLI overhead
- **Programmatic use** — Import and call directly from Python
- **AI integration** — Agents call API, no shell subprocess needed
- **REPL exploration** — `from portolan import Catalog; c = Catalog.load()`
- **Composition** — Build higher-level tools on the API

### What becomes harder
- **CLI-specific features** — Progress bars, interactive prompts need careful API design
- **Streaming output** — API must support callbacks or iterators for progress

### Trade-offs
- We accept slightly more complex API design for much better testability and reuse
- We accept that some CLI affordances (colors, spinners) don't translate to API

## Alternatives Considered

### 1. CLI-first with Python bindings
**Rejected:** Testing requires subprocess calls, AI integration requires shell execution, logic duplication risk.

### 2. Separate CLI and library packages
**Considered:** Clean separation, but adds packaging complexity. Single package with clear internal boundaries achieves same goal.

### 3. Auto-generate CLI from API (like Typer)
**Rejected:** Magic makes debugging harder. Explicit Click decorators are more predictable and maintainable.
