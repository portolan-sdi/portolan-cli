# Contributing to portolan-cli

Thank you for your interest in contributing to portolan-cli!

> **For AI agents and detailed development guidelines:** See `CLAUDE.md` in the project root. This document is for human contributors and covers workflow, not implementation details.

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

- Feature: `feature/description` (e.g., `feature/add-streaming-support`)
- Bug fix: `fix/description` (e.g., `fix/bbox-metadata-issue`)
- Documentation: `docs/description` (e.g., `docs/update-readme`)

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) enforced by commitizen. See `CLAUDE.md` for the full format specification.

**Quick reference:**
```
feat(scope): add new feature
fix(scope): fix bug
docs(scope): update documentation
refactor(scope): restructure code
test(scope): add tests
```

### Pull Request Process

1. **Create a new branch** from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** — Write code, add tests, update docs

3. **Run pre-commit checks** (happens automatically on commit)
   ```bash
   uv run pre-commit run --all-files
   ```

4. **Run tests**
   ```bash
   uv run pytest
   ```

5. **Commit and push**
   ```bash
   git add .
   git commit -m "feat(scope): description"
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** on GitHub
   - Fill in the PR template
   - Link any related issues
   - CI will run automatically

### What CI Checks

All PRs must pass these automated checks (see `context/shared/documentation/ci.md` for details):

- **Linting & formatting** — ruff
- **Type checking** — mypy
- **Security scanning** — bandit, pip-audit
- **Tests** — pytest with coverage
- **Dead code detection** — vulture
- **Complexity limits** — xenon
- **Documentation build** — mkdocs

## Testing

Tests use pytest. See `CLAUDE.md` for:
- Test-Driven Development requirements
- Available pytest markers (`@pytest.mark.unit`, etc.)
- Test fixture conventions

**Running tests:**
```bash
# All tests (except slow/network/benchmark)
uv run pytest

# Specific marker
uv run pytest -m unit

# With coverage report
uv run pytest --cov=portolan_cli --cov-report=html
```

## Code Review

### For Contributors

- Respond to feedback promptly
- Be open to suggestions
- Update your PR based on feedback
- Keep discussions focused and professional

### For Reviewers

- Be respectful and constructive
- Explain the reasoning behind suggestions
- Help contributors improve their submissions

## Release Process

Portolan uses a **tag-based release workflow**:

1. **Accumulate changes** — Merge PRs to `main` as normal using conventional commits
2. **Prepare a release** — When ready to release, create a PR that bumps the version:
   ```bash
   uv run cz bump --changelog
   git push
   ```
3. **Merge the bump PR** — The release workflow detects the bump commit and:
   - Creates a git tag (e.g., `v0.3.0`)
   - Builds the package
   - Publishes to PyPI
   - Creates a GitHub Release

The version is determined by commitizen based on conventional commits since the last release:

| Commit type | Version bump |
|-------------|--------------|
| `feat:` | Minor (0.x.0) |
| `fix:` | Patch (0.0.x) |
| `BREAKING CHANGE:` | Major (x.0.0) |
| `docs:`, `refactor:`, `test:`, `chore:` | No release |

## AI-Assisted Development (Optional)

We use AI assistants (Claude Code, etc.) extensively for development. If you do too, here are tools we find helpful:

| Tool | Purpose | Install |
|------|---------|---------|
| [Context7](https://context7.io) | Up-to-date library docs via MCP | MCP server config |
| [Gitingest](https://github.com/cyclotruc/gitingest) | Source code exploration | `pipx install gitingest` |
| Distill | Token compression for large outputs | MCP server config |

These are **entirely optional**—the project works fine without them. We document our AI workflows in `CLAUDE.md` and `context/shared/documentation/context-guide.md` for those who want to use similar approaches.

> **Note**: This space evolves rapidly. Use whatever tools work for you.

## Questions?

- **Bug reports / feature requests:** Open an issue
- **Questions:** Use GitHub Discussions
- **Check existing issues** before creating new ones

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help create a welcoming environment
- Report unacceptable behavior to maintainers

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
