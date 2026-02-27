# Documentation Alignment Strategy for Portolan CLI

**Issue:** [#112 - Documentation Drift](https://github.com/portolan-sdi/portolan-cli/issues/112)
**Goal:** Zero tolerance for staleness — automate enforcement or eliminate the artifact

## Problem Statement

At 5-10k LoC/day velocity, manual documentation sync is impossible. The codebase has multiple documentation layers:

| Artifact | Audience | Current Problem |
|----------|----------|-----------------|
| `README.md` | Humans (first impression) | Duplicates `docs/index.md`, examples drift |
| `docs/` | Humans (users) | Quick-start examples break silently |
| `ROADMAP.md` | Everyone | Manual ✓ checkmarks, perpetually stale |
| `context/architecture.md` | AI + humans | Prose descriptions drift from code |
| `CLAUDE.md` | AI agents | **Critical:** stale docs → bad AI code → compounds |
| `SKILL.md` (planned) | AI agents | Will have same problem as CLAUDE.md |

## Strategy: Three Pillars

### Pillar 1: Eliminate What Can't Be Automated
- **ROADMAP.md → GitHub Issues + Milestones** (full deprecation)
- **README.md ↔ docs/index.md** → README.md is source of truth, include in docs via mkdocs snippets

### Pillar 2: Enforce via CI (Fail Build on Drift)
- **architecture.yaml** + import-linter (replace prose with rules)
- **CLI reference** → mkdocs-click auto-generation + snapshot validation
- **Doc examples** → test-verified (extract and run in CI)
- **CLAUDE.md** → expanded validation (CLI commands, module paths, test markers)

### Pillar 3: Auto-Generate from Code
- CLI reference docs from Click introspection
- ADR index from directory listing (already partially done)
- Test markers table from pyproject.toml
- SKILL.md designed from day 1 to be mostly generated

---

## Implementation Phases

### Phase 1: Quick Wins (Day 1)

**1.1 Deprecate ROADMAP.md**
- [ ] Create GitHub labels: `roadmap:mvp`, `roadmap:v0.5`, `roadmap:future`
- [ ] Create milestones matching roadmap versions
- [ ] Convert ROADMAP.md items to issues with labels
- [ ] Replace ROADMAP.md with link to Issues view
- [ ] Update docs/roadmap.md symlink → Issues URL or generate from API

**Files:** `ROADMAP.md`, `docs/roadmap.md`, `.github/`

**1.2 Dedupe README.md ↔ docs/index.md**
- [ ] Keep `README.md` as single source of truth (GitHub landing page)
- [ ] Configure mkdocs `snippets` extension to include README sections in docs/index.md
- [ ] Structure README.md with snippet markers for clean inclusion:
  ```markdown
  <!-- --8<-- [start:quickstart] -->
  ## Quick Start
  ...
  <!-- --8<-- [end:quickstart] -->
  ```
- [ ] Update docs/index.md to include snippets:
  ```markdown
  --8<-- "README.md:quickstart"
  ```

**Files:** `README.md`, `docs/index.md`, `mkdocs.yml`

---

### Phase 2: Architecture Enforcement (Day 2-3)

**2.1 Add import-linter**
```toml
# pyproject.toml additions
[project.optional-dependencies]
dev = ["import-linter>=2.0", ...]

[tool.importlinter]
root_packages = ["portolan_cli"]

[[tool.importlinter.contracts]]
id = "layered-architecture"
name = "Enforce layered architecture"
type = "layers"
layers = [
    "portolan_cli.cli",
    "portolan_cli.catalog | portolan_cli.dataset | portolan_cli.push | portolan_cli.pull | portolan_cli.sync | portolan_cli.check",
    "portolan_cli.scan | portolan_cli.convert | portolan_cli.collection | portolan_cli.item",
    "portolan_cli.metadata | portolan_cli.validation | portolan_cli.schema | portolan_cli.versions",
    "portolan_cli.models | portolan_cli.backends",
    "portolan_cli.output | portolan_cli.errors | portolan_cli.constants",
]

[[tool.importlinter.contracts]]
id = "cli-no-storage"
name = "CLI cannot import storage directly"
type = "forbidden"
source_modules = ["portolan_cli.cli"]
forbidden_modules = ["portolan_cli.backends"]
```

**2.2 Deprecate architecture.md**
- [ ] Convert key invariants to import-linter contracts
- [ ] Delete or archive `context/architecture.md`
- [ ] Create `ADR-0025-architecture-as-code.md`

**2.3 Pre-commit + CI Integration**
```yaml
# .pre-commit-config.yaml addition
- repo: local
  hooks:
    - id: import-linter
      name: Check architecture rules
      entry: uv run lint-imports
      language: system
      types: [python]
      pass_filenames: false
```

**Files:** `pyproject.toml`, `.pre-commit-config.yaml`, `.github/workflows/ci.yml`, `context/architecture.md`, `context/shared/adr/`

---

### Phase 3: Test-Verified Documentation (Day 3-4)

**3.1 Create documentation example tests**
```
tests/
└── docs/
    ├── conftest.py           # Fixtures for temp catalogs
    └── test_readme_examples.py
```

**3.2 Test implementation pattern:**
```python
# tests/docs/test_readme_examples.py
class TestReadmeQuickStart:
    """Test README quick-start workflow executes without error."""

    @pytest.mark.integration
    def test_quickstart_workflow(self, tmp_path: Path) -> None:
        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Create fixture data
            (Path.cwd() / "demographics").mkdir()
            (Path.cwd() / "demographics/sample.geojson").write_text(MINIMAL_GEOJSON)

            # Step 1: portolan init
            assert runner.invoke(cli, ["init", "--auto"]).exit_code == 0
            # Step 2: portolan scan demographics/
            assert runner.invoke(cli, ["scan", "demographics/"]).exit_code == 0
            # Step 3: portolan add demographics/
            assert runner.invoke(cli, ["add", "demographics/"]).exit_code == 0
```

**3.3 Mark non-testable examples:**
```markdown
```bash
# notest - installation command
pipx install portolan-cli
```

**3.4 CI integration:**
```yaml
# Add to ci.yml
- name: Test documentation examples
  run: uv run pytest tests/docs/ -v --tb=short
```

**Files:** `tests/docs/`, `.github/workflows/ci.yml`, `README.md` (add notest markers)

---

### Phase 4: CLI Reference Auto-Generation (Day 4-5)

**4.1 Add mkdocs-click**
```toml
# pyproject.toml
docs = ["mkdocs-click>=0.8.0", ...]
```

```yaml
# mkdocs.yml
markdown_extensions:
  - mkdocs-click

nav:
  - Reference:
    - CLI: reference/cli.md
```

**4.2 Create reference page:**
```markdown
# docs/reference/cli.md
# CLI Reference

::: mkdocs-click
    :module: portolan_cli
    :command: cli
    :prog_name: portolan
    :style: table
```

**4.3 Add CLI snapshot validation:**
```python
# scripts/validate_cli_docs.py
"""Detect when CLI changes but docs aren't updated."""

def extract_cli_structure(cli_module) -> dict:
    """Extract command names, options, help text."""
    ...

def validate():
    current = extract_cli_structure(cli)
    committed = json.load(open("docs/.cli-snapshot.json"))
    if hash(current) != committed["hash"]:
        fail("CLI changed - run: uv run python scripts/validate_cli_docs.py --update")
```

**Files:** `pyproject.toml`, `mkdocs.yml`, `docs/reference/cli.md`, `scripts/validate_cli_docs.py`, `docs/.cli-snapshot.json`

---

### Phase 5: Expanded CLAUDE.md Validation (Day 5-6)

**5.1 Add validators to existing script:**

| Validator | Extracts | Validates Against |
|-----------|----------|-------------------|
| `FilePathValidator` | `` `path/to/file.py` `` | `Path.exists()` |
| `CLICommandValidator` | `` `portolan <cmd>` `` | `cli.py` decorators |
| `TestMarkerValidator` | `@pytest.mark.X` | `pyproject.toml` markers |
| `CodeExampleValidator` | `from portolan_cli.X import Y` | Module exports |

**5.2 Implementation:**
```python
# scripts/validate_claude_md.py (extended)

def validate_file_paths(claude_md: str, root: Path) -> list[str]:
    """Check that all referenced paths exist."""
    errors = []
    pattern = r"`([a-zA-Z_/]+\.(?:py|md|yaml|json))`"
    for match in re.finditer(pattern, claude_md):
        path = root / match.group(1)
        if not path.exists() and not is_example_path(match.group(1)):
            errors.append(f"Path not found: {match.group(1)}")
    return errors

def validate_cli_commands(claude_md: str, root: Path) -> list[str]:
    """Check that CLI commands exist."""
    registered = extract_click_commands(root / "portolan_cli/cli.py")
    errors = []
    for match in re.finditer(r"`portolan\s+(\w+)`", claude_md):
        if match.group(1) not in registered:
            errors.append(f"CLI command not found: portolan {match.group(1)}")
    return errors

def validate_test_markers(claude_md: str, root: Path) -> list[str]:
    """Check markers in CLAUDE.md match pyproject.toml."""
    documented = set(re.findall(r"@pytest\.mark\.(\w+)", claude_md))
    registered = parse_pytest_markers(root / "pyproject.toml")
    phantom = documented - registered
    if phantom:
        return [f"Test markers not in pyproject.toml: {phantom}"]
    return []
```

**5.3 Auto-generated sections:**
```markdown
<!-- BEGIN GENERATED: cli-commands -->
| Command | Description |
|---------|-------------|
| `portolan init` | Initialize a new catalog |
...
<!-- END GENERATED: cli-commands -->
```

With a generator script that fails CI if sections are stale.

**5.4 Freshness tracking for prose:**
Add freshness markers to prose sections that describe code behavior:
```markdown
<!-- freshness: last-verified: 2026-02-27 -->
## Design Principles
...
<!-- /freshness -->
```

Validator checks freshness and warns (doesn't fail) when sections are >30 days stale.

**Files:** `scripts/validate_claude_md.py`, `scripts/generate_claude_md_sections.py`, `CLAUDE.md`

---

### Phase 6: SKILL.md Design (Day 6-7)

**Key principle:** SKILL.md is for **AI agents helping USERS** (how to USE Portolan).
CLAUDE.md is for **AI agents doing DEVELOPMENT** (how to MODIFY Portolan).

| Doc | Audience | Content Focus |
|-----|----------|---------------|
| `SKILL.md` | AI helping users | CLI workflows, common tasks, troubleshooting, Python API examples |
| `CLAUDE.md` | AI doing dev work | Conventions, TDD rules, architecture, ADRs, test markers |

**6.1 Structure:**
```markdown
# portolan-cli SKILL.md

<!-- BEGIN GENERATED: catalog-overview -->
## What is Portolan?
[Generated from package docstring]
<!-- END GENERATED -->

<!-- BEGIN GENERATED: cli-commands -->
## CLI Commands
[Generated from Click introspection]
<!-- END GENERATED -->

<!-- BEGIN GENERATED: python-api -->
## Python API
[Generated from public module docstrings]
<!-- END GENERATED -->

## Common Workflows

<!-- freshness: last-verified: 2026-02-27 -->
### Publishing a New Catalog
1. `portolan init` - create catalog structure
2. `portolan scan <dir>` - discover files, fix issues
3. `portolan add <dir>` - track files in collection
4. `portolan push <remote>` - sync to cloud
<!-- /freshness -->

<!-- anchor: sync.py:sync_catalog -->
### Sync Workflow
...
<!-- /anchor -->

## Troubleshooting
[Common error messages and solutions]
```

**6.2 Generation + validation:**
- `scripts/generate_skill_md.py` - generates sections from code
- `scripts/validate_skill_md.py` - validates anchors, checks freshness
- Pre-commit hook fails if SKILL.md stale

**6.3 Freshness tracking:**
```python
# In validate_skill_md.py
def check_freshness(content: str, max_days: int = 30) -> list[str]:
    """Warn on sections not verified within max_days."""
    warnings = []
    pattern = r"<!-- freshness: last-verified: (\d{4}-\d{2}-\d{2}) -->"
    for match in re.finditer(pattern, content):
        verified = datetime.strptime(match.group(1), "%Y-%m-%d")
        if (datetime.now() - verified).days > max_days:
            warnings.append(f"Section last verified {match.group(1)} (>{max_days} days ago)")
    return warnings
```

CI warns (but doesn't fail) on stale prose sections.

**Files:** `SKILL.md`, `scripts/generate_skill_md.py`, `scripts/validate_skill_md.py`

---

## Verification Steps

After each phase, verify:

| Phase | Verification Command |
|-------|---------------------|
| 1 | `gh issue list --label roadmap:mvp` returns issues |
| 2 | `uv run lint-imports` passes |
| 3 | `uv run pytest tests/docs/` passes |
| 4 | `uv run mkdocs build --strict` + snapshot check passes |
| 5 | `uv run python scripts/validate_claude_md.py` passes |
| 6 | `uv run python scripts/validate_skill_md.py` passes |

---

## Summary: Before/After

| Artifact | Before | After |
|----------|--------|-------|
| `ROADMAP.md` | Manual prose, checkmarks | **Deleted** → GitHub Issues + Milestones |
| `README.md` ↔ `docs/` | Duplicate, drift | **README.md is source**, included via snippets |
| `architecture.md` | Prose, stale | **Deleted** → `import-linter` contracts |
| CLI reference | Manual or none | **Auto-generated** via mkdocs-click |
| Doc examples | Untested | **Test-verified** in CI |
| `CLAUDE.md` | Partial validation | **Full validation** (paths, commands, markers) + freshness |
| `SKILL.md` | N/A | **Auto-generated** (user workflows) + freshness tracking |

## The Sanity Guarantee

After implementation, these invariants hold:

1. **If it compiles, the architecture is valid** (import-linter)
2. **If CI passes, the docs are accurate** (test-verified examples)
3. **If pre-commit passes, CLAUDE.md references exist** (expanded validation)
4. **If CLI changes, you're forced to update** (snapshot validation)
5. **If prose is stale, you're warned** (freshness tracking)

No more "the docs say X but the code does Y."

---

## Estimated Timeline

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: Quick wins | 2-3 hours | None |
| Phase 2: Architecture | 3-4 hours | Phase 1 (for clean state) |
| Phase 3: Test-verified docs | 3-4 hours | None |
| Phase 4: CLI auto-gen | 3-4 hours | None |
| Phase 5: CLAUDE.md validation | 4-5 hours | None |
| Phase 6: SKILL.md | 3-4 hours | Phase 5 (same patterns) |

**Total:** ~20-24 hours of implementation

Phases 1-2 and 3-4 can be parallelized.

---

## Decisions Made

1. **README vs docs/index.md:** README.md is source of truth, include in docs via mkdocs snippets
2. **SKILL.md scope:** User-focused workflows (how to USE) vs CLAUDE.md dev conventions (how to MODIFY)
3. **Freshness tracking:** Yes — `<!-- freshness: last-verified: DATE -->` markers for prose sections, CI warns when >30 days stale

---

## Parallelization Strategy

Given your velocity, execute phases in parallel where possible:

```
Week 1:
├── Agent 1: Phase 1 (ROADMAP deprecation, README dedup)
├── Agent 2: Phase 2 (import-linter)
└── Agent 3: Phase 3 (test-verified docs)

Week 2:
├── Agent 1: Phase 4 (CLI auto-gen)
├── Agent 2: Phase 5 (CLAUDE.md validation)
└── Agent 3: Phase 6 (SKILL.md)
```

Phases 1-3 have no dependencies. Phases 4-6 can run in parallel after Phase 3 (test infrastructure exists).
