# Contributing to portolan-cli

Thank you for your interest in contributing to portolan-cli!

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/portolan-sdi/portolan-cli.git
   cd portolan-cli
   ```

2. **Install uv** (if not already installed)
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Install dependencies**
   ```bash
   uv sync --all-extras
   ```

4. **Install pre-commit hooks**
   ```bash
   uv run pre-commit install
   ```

5. **Verify setup**
   ```bash
   uv run pytest
   uv run portolan --help
   ```

## Making Changes

### Branch Naming

- Feature: `feature/description`
- Bug fix: `fix/description`
- Documentation: `docs/description`
- Refactor: `refactor/description`

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) enforced by commitizen:

```
feat(scope): add new feature
fix(scope): fix bug
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
```

Use `uv run cz commit` for interactive commit creation.

### Pull Request Process

1. Create a branch from `main`
2. Write tests first, then implement (TDD is required — see Testing section)
3. Run `uv run pre-commit run --all-files` to check everything locally
4. Push and open a PR — CI runs automatically
5. Fill in the PR template and link related issues

### What CI Checks

All PRs must pass:

- **ruff** — Linting and formatting
- **mypy** — Type checking (strict mode)
- **bandit** — Security scanning
- **pip-audit** — Dependency vulnerabilities
- **vulture** — Dead code detection
- **xenon** — Complexity limits
- **pytest** — Full test suite with coverage
- **mkdocs** — Documentation build

All checks are strict — none allow failures.

## Testing

Tests use pytest with markers to categorize test types:

| Marker | Description |
|--------|-------------|
| `@pytest.mark.unit` | Fast, isolated, no I/O (< 100ms) |
| `@pytest.mark.integration` | Multi-component, may touch filesystem |
| `@pytest.mark.network` | Requires network (mocked locally) |
| `@pytest.mark.slow` | Takes > 5 seconds |

```bash
# All tests
uv run pytest

# Only unit tests
uv run pytest -m unit

# With coverage report
uv run pytest --cov=portolan_cli --cov-report=html
```

**Test-driven development is required.** Write tests before implementation. Tests must fail before the implementation exists and pass after. This is not optional.

## Release Process

Portolan uses a tag-based release workflow driven by conventional commits:

| Commit type | Version bump |
|-------------|--------------|
| `feat:` | Minor (0.x.0) |
| `fix:` | Patch (0.0.x) |
| `BREAKING CHANGE:` | Major (x.0.0) |
| `docs:`, `refactor:`, `test:`, `chore:` | No release |

To cut a release, create a PR that runs:
```bash
uv run cz bump --changelog
```

Merging that PR triggers the release workflow: it creates a git tag, builds the package, publishes to PyPI, and creates a GitHub Release.

## Code Standards

- All code requires type annotations (`mypy --strict`)
- Use `portolan_cli/output.py` for all user-facing terminal messages
- Non-obvious design decisions require an ADR in `context/shared/adr/`

## AI-Assisted Development (Optional)

We use AI assistants (Claude Code, etc.) extensively. Useful tools:

| Tool | Purpose | Install |
|------|---------|---------|
| [Speckit](https://github.com/speckit/speckit) | Specification-driven development | Claude Code plugin |
| [Context7](https://context7.io) | Up-to-date library docs via MCP | MCP server config |
| [Gitingest](https://github.com/cyclotruc/gitingest) | Source code exploration | `pipx install gitingest` |
| [grepai](https://yoanbernabeu.github.io/grepai/) | Semantic code search via MCP | `pipx install grepai` |

The project includes `.mcp.json` for automatic MCP registration. grepai must be installed locally (`pipx install grepai`) and initialized with `grepai init` in the project directory. The `.grepai/` index is gitignored — it's regenerated per-developer.

## Questions?

- **Bug reports / feature requests:** Open an issue
- **Questions:** Use GitHub Discussions

## Code of Conduct

Be respectful and constructive. Help create a welcoming environment.

## License

By contributing, you agree your contributions will be licensed under Apache 2.0.
