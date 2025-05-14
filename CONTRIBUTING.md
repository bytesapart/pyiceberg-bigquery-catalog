# Contributing to PyIceberg BigQuery Catalog

Thank you for your interest in contributing to the PyIceberg BigQuery Catalog! This document outlines the process for contributing to this project.

## Development Setup

1. Fork the repository and clone your fork:
   ```bash
   git clone https://github.com/yourusername/pyiceberg-bigquery-catalog.git
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

## Code Style

This project uses several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Style guide enforcement
- **mypy**: Type checking
- **pre-commit**: Git hooks for code quality

All of these are automatically run via pre-commit hooks.

## Making Changes

1. Create a new branch for your feature or fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and ensure tests pass:
   ```bash
   poetry run pytest
   ```

3. Run the linters:
   ```bash
   poetry run pre-commit run --all-files
   ```

4. Commit your changes using conventional commits:
   ```bash
   git commit -m "feat: add new feature"
   ```

   Commit types:
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

## Testing

- Write tests for new features and bug fixes
- Ensure all tests pass before submitting a PR
- Aim for good test coverage

Run tests with:
```bash
poetry run pytest -v --cov=pyiceberg_bigquery_catalog --cov-report=term-missing
```

## Submitting a Pull Request

1. Push your changes to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Create a pull request on GitHub

3. Ensure all CI checks pass

4. Wait for code review and address any feedback

## Code Review Process

- All submissions require review before merging
- We aim to review PRs within 48 hours
- Be open to feedback and suggestions
- Keep discussions focused and professional

## Questions?

If you have any questions, please:
- Open an issue for bugs or feature requests
- Start a discussion for general questions
- Check existing issues and discussions first

Thank you for contributing!