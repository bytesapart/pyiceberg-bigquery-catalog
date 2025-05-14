# Contributing to PyIceberg BigQuery Catalog

Thank you for your interest in contributing to the PyIceberg BigQuery Catalog! This document outlines the process for contributing to this project.

## Development Setup

1. Fork the repository and clone your fork:
   ```bash
   git clone https://github.com/bytesapart/pyiceberg-bigquery-catalog.git
   cd pyiceberg-bigquery-catalog
   ```

2. Install Poetry (if not already installed):
   ```bash
   pip install poetry
   ```

3. Install dependencies and pre-commit hooks:
   ```bash
   poetry install
   poetry run pre-commit install
   ```

## Code Style and Quality Checks

This project uses several tools to maintain code quality, which are enforced through pre-commit hooks:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Style guide enforcement
- **mypy**: Type checking
- **ruff**: Additional linting
- **bandit**: Security checks

### Pre-commit Hooks

Pre-commit hooks are **automatically run on every commit** to ensure code quality. They will:
- Format your code
- Check for style violations
- Run type checking
- Validate YAML, JSON, and TOML files
- Check for merge conflicts and large files

If pre-commit fails, it will show you what needs to be fixed. In many cases, it will automatically fix issues (like formatting).

To manually run pre-commit hooks:
```bash
# Run on all files
poetry run pre-commit run --all-files

# Run on staged files only
poetry run pre-commit run
```

### CI Enforcement

All pull requests must pass pre-commit checks before merging. The CI pipeline will:
- Run pre-commit hooks on all files
- Run tests on multiple Python versions
- Check that poetry.lock is up to date

## Making Changes

1. Create a new branch for your feature or fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and ensure tests pass:
   ```bash
   poetry run pytest
   ```

3. The pre-commit hooks will run automatically when you commit:
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```

   If pre-commit fails, fix the issues and commit again:
   ```bash
   # Pre-commit may have made automatic fixes
   git add .
   git commit -m "feat: add new feature"
   ```

4. Push your changes to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

## Commit Messages

Use conventional commits for your commit messages:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Test additions or changes
- `build`: Build system changes
- `ci`: CI configuration changes
- `chore`: Other changes

Examples:
```bash
git commit -m "feat: add support for custom table properties"
git commit -m "fix: handle empty dataset names correctly"
git commit -m "docs: update installation instructions"
```

## Testing

- Write tests for new features and bug fixes
- Ensure all tests pass before submitting a PR
- Aim for good test coverage

Run tests with:
```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest -v --cov=pyiceberg_bigquery_catalog --cov-report=term-missing

# Run specific test
poetry run pytest tests/test_catalog.py::TestTableOperations::test_create_table
```

## Submitting a Pull Request

1. Push your changes to your fork
2. Create a pull request on GitHub
3. Ensure all CI checks pass (including pre-commit)
4. Wait for code review and address any feedback

## Code Review Process

- All submissions require review before merging
- We aim to review PRs within 48 hours
- Be open to feedback and suggestions
- Keep discussions focused and professional

## Troubleshooting

### Pre-commit Issues

If pre-commit hooks are causing issues:

1. Update pre-commit hooks:
   ```bash
   poetry run pre-commit autoupdate
   ```

2. Clear pre-commit cache:
   ```bash
   poetry run pre-commit clean
   ```

3. Reinstall hooks:
   ```bash
   poetry run pre-commit uninstall
   poetry run pre-commit install
   ```

### Poetry Issues

If you encounter Poetry-related issues:

1. Update Poetry:
   ```bash
   pip install --upgrade poetry
   ```

2. Clear Poetry cache:
   ```bash
   poetry cache clear --all .
   ```

3. Reinstall dependencies:
   ```bash
   poetry install --no-cache
   ```

## Questions?

If you have any questions, please:
- Open an issue for bugs or feature requests
- Start a discussion for general questions
- Check existing issues and discussions first

Thank you for contributing!
