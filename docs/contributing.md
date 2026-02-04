# Contributing to portolan-cli

Thank you for your interest in contributing to portolan-cli!

> **For AI agents and detailed development guidelines:** See [`CLAUDE.md`](../CLAUDE.md) in the project root. This document is for human contributors and covers workflow, not implementation details.

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

Releases are **fully automated** via commitizen. When PRs are merged to `main`:

1. Commits are analyzed for conventional commit types
2. Version is bumped automatically (major/minor/patch)
3. Changelog is generated
4. Package is published to PyPI
5. GitHub Release is created

No manual intervention needed. Just merge PRs with proper commit messages.

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
