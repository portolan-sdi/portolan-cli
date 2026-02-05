# ADR-0008: pipx for Global Installation, uv for Development

## Status
Accepted

## Context

Users need a clear, reliable way to install Portolan CLI, while developers need efficient tools for local development. Python package installation has historically been fragmented, with several competing approaches:

1. **pip** — The traditional package installer, but installs into global or user site-packages, risking dependency conflicts
2. **pipx** — Installs CLI tools in isolated environments, preventing conflicts while making commands globally available
3. **uv** — Modern, Rust-based package manager with faster dependency resolution and better lockfile support
4. **brew** — Platform package manager (macOS/Linux), but Linux support is more complex and requires additional repositories

The goal is to recommend tools that:
- Prevent dependency conflicts for end users
- Provide a smooth installation experience across platforms
- Support both casual users and power users/developers
- Align with modern Python packaging best practices

## Decision

We will recommend **pipx for global installation** (end users) and **uv for local development** (contributors).

### For End Users (Global Installation)

Recommend `pipx install portolan-cli` as the primary installation method:

```bash
pipx install portolan-cli
```

**Rationale:**
- Isolates Portolan's dependencies from other Python tools
- Makes `portolan` command globally available
- Now recommended by major Python tools (black, ruff, mypy, etc.)
- Cross-platform support (macOS, Linux, Windows)
- Handles PATH configuration automatically

We will still document `pip install portolan-cli` as a fallback for users who prefer it, but mark pipx as recommended.

### For Developers (Local Development)

Use `uv` for all development workflows:

```bash
uv sync --all-extras        # Install dependencies
uv run pytest               # Run tests
uv run portolan --help      # Run CLI locally
```

**Rationale:**
- Significantly faster dependency resolution than pip/pip-tools
- Reproducible builds with `uv.lock`
- Built-in virtual environment management
- Better error messages and conflict resolution
- Already adopted for this project

### Not Recommended (For Now)

**Homebrew:** While technically feasible for macOS, Linux support would require additional tap repositories and maintenance overhead. We may revisit this in the future, but it's not a priority for initial release.

**System pip:** Not recommended due to potential dependency conflicts and security implications of modifying system Python packages.

## Consequences

### Positive
- **For users:** Clear, conflict-free installation path
- **For developers:** Fast, reproducible development environment
- **For maintainers:** Aligns with modern Python packaging trends, reduces support burden
- **For documentation:** Single recommended path reduces confusion

### Negative
- **User learning curve:** Users unfamiliar with pipx need to install it first (one extra step)
- **Platform variance:** Some users may encounter PATH issues with pipx (though less common than with pip)
- **Two tools:** Developers need both uv (development) and pipx (testing global install)

### Mitigations
- Clearly document pipx installation in README and docs
- Provide fallback instructions for pip
- Link to pipx troubleshooting guide for PATH issues
- Document how to install pipx itself (`python3 -m pip install --user pipx`)

## Alternatives Considered

### Alternative 1: pip as Primary Recommendation
**Rejected** because:
- No dependency isolation — conflicts are common
- Python packaging ecosystem is moving away from global pip installs for CLI tools
- Doesn't align with best practices for distributing CLI applications

### Alternative 2: Homebrew
**Deferred** because:
- Linux support requires maintaining a tap repository
- Additional maintenance overhead for formula updates
- Doesn't solve Windows installation
- Can revisit later if demand warrants it

### Alternative 3: uv for Both Users and Developers
**Rejected** because:
- `uv tool install` is relatively new and less widely adopted than pipx
- pipx is more established for CLI tool installation
- User documentation would need to explain uv's broader capabilities when users only need tool installation
- However, this may become viable in the future as uv matures

### Alternative 4: Docker/Container
**Out of scope** because:
- Overkill for a CLI tool
- Doesn't match user expectations for Python CLI tools
- Adds complexity for simple use cases
