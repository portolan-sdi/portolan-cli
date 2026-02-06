# ADR-0009: Dry-Run and Verbose Modes in Output Functions

## Status
Accepted

## Context

Portolan CLI performs destructive operations like file deletion (`prune`), remote sync (`sync`), and data transformation (`dataset add`). Users need confidence before executing these operations, especially in production environments or when operating on cloud storage.

Additionally, debugging and troubleshooting benefit from detailed technical output showing internal decisions, file paths, and library calls. However, this detail should not clutter normal usage.

**Problem:** How do we let users preview operations before executing them, and provide technical details when debugging, without complicating the API or fragmenting the codebase?

**Forces at play:**
- Operations that modify state need safe preview mechanisms
- AI agents need to inspect what tools would do before executing them
- Debugging requires technical details that would overwhelm normal users
- Every output function needs consistent behavior across dry-run and verbose modes
- The implementation must be maintainable and testable against mutations
- Future enhancement should be possible without breaking existing code

## Decision

Add **per-call** `dry_run` and `verbose` parameters to all output functions (`success()`, `error()`, `info()`, `warn()`, `detail()`).

### Dry-Run Mode (`dry_run=True`)

**Behavior:** Prefix all messages with `[DRY RUN]` to indicate simulation mode.

**Semantics:**
- Shows what *would* happen without executing side effects
- File writes, network calls, and state changes are suppressed by the calling code
- All validation and error checking still runs
- Output functions merely prefix messages; actual operation suppression happens in business logic

**Rationale:**
- **AI-friendly:** Agents can run commands with `--dry-run` to understand behavior (per [Henrik Warne 2026](https://henrikwarne.com/2026/01/31/in-praise-of-dry-run/))
- **Maintainable:** Using dry-run output as input to execute path prevents drift (per [G-Research best practices](https://www.gresearch.com/news/in-praise-of-dry-run/))
- **Safe:** Users build muscle memory to preview before executing destructive operations

### Verbose Mode (`verbose=True`)

**Behavior:** Reserved for future enhancement. Currently has same behavior as default.

**Future semantics:**
- Show technical details: file paths, sizes, checksums, timestamps
- Expose internal steps: which libraries are called, conversion progress, decision rationale
- Include debug context: "Detected CRS: EPSG:4326 from .prj file"

**Rationale:**
- **Progressive disclosure:** Normal users see clean output; developers get details when needed
- **GNU standard:** `--verbose` is the conventional name per [GNU Coding Standards](https://www.gnu.org/prep/standards/html_node/Command_002dLine-Interfaces.html)
- **Future-proof:** Reserving the parameter now allows enhancement without API changes

### Parameter Location

**Decision:** Parameters are **per-call**, not global state or context managers.

**Rationale:**
- Matches the "CLI wraps API" architecture (ADR-0007): CLI passes flags down to library layer
- Explicit over implicit: call sites clearly show when dry-run/verbose is active
- Testable: Each call can be tested independently without managing global state
- Thread-safe: No shared state means no concurrency issues

### Command Coverage

Dry-run mode applies to commands that modify state:

| Command | `--dry-run` | `--verbose` |
|---------|-------------|-------------|
| `dataset add` | ✅ | ✅ |
| `dataset remove` | ✅ | ✅ |
| `sync` | ✅ | ✅ |
| `repair` | ✅ | ✅ |
| `prune` | ✅ | ✅ |
| `init` | ✅ | ✅ |
| `check` | ❌ | ✅ |
| `check --remote` | ❌ | ✅ |
| `dataset list` | ❌ | ✅ |
| `dataset info` | ❌ | ✅ |
| `remote add` | ❌ | ✅ |
| `remote list` | ❌ | ✅ |

**Rule:** If it modifies filesystem or network state → support `--dry-run`.

## Consequences

### What becomes easier

- **Safe exploration:** Users can preview `prune`, `sync`, and `repair` before executing
- **AI integration:** Agents can run tools with `--dry-run` to understand behavior before committing
- **Debugging:** Future verbose mode will expose technical details without cluttering normal output
- **Testing:** Per-call parameters are straightforward to test with mutation testing
- **Consistency:** All output functions have identical signatures and behavior

### What becomes harder

- **Parameter propagation:** Every call to output functions in business logic needs to accept and pass down `dry_run` and `verbose`
- **Dry-run implementation:** Each command must correctly suppress side effects when `dry_run=True`
- **Documentation maintenance:** Users need to know which commands support which modes

### Trade-offs accepted

- **Verbose mode is a no-op initially:** We reserve the parameter now but defer implementation until needed
- **Manual parameter threading:** No context manager or global state means parameters must be passed explicitly through call chains
- **Output-only dry-run:** The output module only prefixes messages; suppressing actual operations is the caller's responsibility

## Alternatives considered

### Alternative 1: Global state via context manager

```python
with output.dry_run_mode():
    success("Would write file")
```

**Rejected because:**
- Implicit behavior is harder to reason about
- Thread-unsafe without complex locking
- Doesn't match "CLI wraps API" pattern (ADR-0007)
- Mutation testing would have harder time verifying context manager behavior

### Alternative 2: Separate dry-run output functions

```python
dry_success("Would write file")
verbose_info("Reading file with details")
```

**Rejected because:**
- Doubles the API surface (10 functions instead of 5)
- Harder to maintain consistency
- Unclear how to combine both modes
- Mutation testing would need to verify 10 functions instead of 5

### Alternative 3: Module-level configuration

```python
output.set_dry_run(True)
success("Would write file")
```

**Rejected because:**
- Same thread-safety issues as context manager
- Spooky action at a distance: configuration far from usage
- Hard to test: must reset state after each test
- Doesn't match explicit API design philosophy

## Implementation notes

- All functions now accept `dry_run=False` and `verbose=False` parameters
- `_output()` internal helper handles dry-run prefix logic centrally
- Tests use property-based testing (Hypothesis) to verify message preservation
- Mutation testing ensures tests actually verify dry-run prefix presence/absence
- 100% branch coverage on output.py

## References

- [In Praise of –dry-run (Henrik Warne, 2026)](https://henrikwarne.com/2026/01/31/in-praise-of-dry-run/)
- [Enhancing Software Tools With --dry-run (G-Research)](https://www.gresearch.com/news/in-praise-of-dry-run/)
- [GNU Coding Standards - Command-Line Interfaces](https://www.gnu.org/prep/standards/html_node/Command_002dLine-Interfaces.html)
- [Command Line Interface Guidelines](https://clig.dev/)
- ADR-0007: CLI wraps Python API
