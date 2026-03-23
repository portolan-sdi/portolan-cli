# menard audit

Analyze documentation for menard trackability and suggest improvements.

## When to Use

Use this skill when:
- Onboarding a new repo to menard (`menard init` → `menard audit`)
- Periodic health checks on documentation coverage
- After adding new docs that need links.toml entries
- When docs feel "messy" and need restructuring for trackability

## What This Skill Does

Score docs on **deterministic verifiability** — how well menard can track and enforce them.

### Scoring Signals

**Good structure (increase score):**
- Tables with file paths, commands, or config values (machine-parseable)
- Code blocks with actual commands or config snippets
- Sections with clear single-file scope (heading maps to one code file)
- Explicit source-of-truth pointers ("see X for canonical version")
- Short, factual assertions ("CLI entry point: `foo` → `bar:baz`")
- Already covered by `links.toml` entries
- Protected by `donttouch` rules where appropriate

**Poor structure (decrease score):**
- Long prose blocks with no code references or tables
- Sections referencing many code files without clear boundaries
- Implicit file references (mentions `auth.py` in prose but not in links.toml)
- Assertions that could be checked against code but aren't linked
- No heading structure (flat wall of text)
- Terminology inconsistencies

## Workflow

### Step 1: Gather Context

```bash
# Check current menard configuration
cat pyproject.toml | grep -A 20 "\[tool.menard\]"

# See existing links
cat .menard/links.toml

# See existing protections
cat .menard/donttouch 2>/dev/null || echo "No donttouch file"

# Run coverage to see current state
menard coverage
```

### Step 2: Scan Documentation

Read all docs matching `doc_paths` from config (typically `docs/**/*.md`, `README.md`).

For each doc file, analyze:
1. **Structure**: Headings, tables, code blocks, prose ratio
2. **File references**: Backtick-quoted paths like `src/foo.py`
3. **Section scope**: Does each section map to identifiable code?
4. **Protected content**: License blocks, version pins, critical terminology

### Step 3: Generate Report

Output per-file, per-section scores with specific issues:

```
# docs/api.md
  Overall: 6/10 (partially trackable)

  ## Authentication (8/10)
    ✓ Contains code examples
    ✓ References src/auth.py
    ⚠ src/auth.py not in links.toml — SUGGEST ADD

  ## Data Pipeline (3/10)
    ✗ 400 words of prose, no tables or code blocks
    ✗ References 7 code files, none in links.toml
    ✗ No clear single-file scope — consider splitting

  ## License (9/10)
    ✓ Short, assertable content
    ⚠ Not in donttouch — SUGGEST PROTECT
```

### Step 4: Generate Suggestions

#### links.toml suggestions

Extract file path mentions from prose and suggest entries:

```toml
# SUGGESTED: Add to .menard/links.toml

[[link]]
code = "src/auth.py"
docs = ["docs/api.md#Authentication"]

[[link]]
code = "src/pipeline.py"
docs = ["docs/api.md#Data Pipeline"]
```

#### donttouch suggestions

Detect protected content patterns:

```
# SUGGESTED: Add to .menard/donttouch

# License section should not change
README.md#License

# License identifier must exist
"Apache-2.0"

# Version pins
pyproject.toml: "python >= 3.10"
```

#### Restructuring suggestions

For low-scoring sections, suggest concrete improvements:

```
## Data Pipeline (score: 3/10)

SUGGEST: Split into per-file sections
This section references: src/pipeline.py, src/transform.py, src/loader.py

Proposed structure:
  ## Pipeline Overview (keep as brief intro)
  ## Transform Step → links to src/transform.py
  ## Loading Step → links to src/loader.py

SUGGEST: Convert prose to table
Current (78 words): "The pipeline supports several output formats..."
Proposed:
  | Format | Type | Handler |
  |--------|------|---------|
  | JSON | Output | `src/formats/json.py` |
  | CSV | Output | `src/formats/csv.py` |
```

### Step 5: Apply (Interactive)

**Safe to auto-apply:**
- `links.toml` additions (purely additive)
- `donttouch` additions (purely additive)

**Require confirmation:**
- Restructuring changes (modify doc content)
- Section splits
- Prose-to-table conversions

## Output Formats

### Human-readable (default)
```bash
menard audit
```

### JSON (for programmatic use)
```bash
menard audit --format json
```

### Suggestions only
```bash
menard audit --suggest
```

### Apply safe changes
```bash
menard audit --apply
```

## Key Principles

1. **Heuristic, not AI-powered scoring** — Pattern matching: count tables, count prose length, grep for file paths, check links.toml coverage. Deterministic and fast.

2. **AI for restructuring suggestions** — The skill (Claude) adds value by proposing how to restructure prose into tables, how to split sections, etc.

3. **links.toml and donttouch suggestions are deterministic** — Inferred from file path mentions, heading structure, content patterns.

4. **Two-audience awareness** — If repo has both `docs/` and `CLAUDE.md`/`context/`, score them differently. AI-oriented docs should be denser, more structured.

## Integration with menard init

Ideal onboarding flow:
```bash
menard init                    # Creates config, .menard/ directory
menard audit --suggest         # "Here's what your docs look like"
menard audit --apply           # Auto-generate links.toml + donttouch
menard bootstrap               # Fill in convention-based links
menard install-hook            # Start enforcing
```
