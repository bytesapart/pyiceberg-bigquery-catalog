# Makefile
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make install        Install dependencies and pre-commit hooks"
	@echo "  make test          Run tests"
	@echo "  make lint          Run all linters"
	@echo "  make format        Format code"
	@echo "  make clean         Clean up generated files"
	@echo "  make build         Build the package"
	@echo "  make publish       Publish to PyPI"

.PHONY: install
install:
	poetry install
	poetry run pre-commit install

.PHONY: test
test:
	poetry run pytest tests/ -v --cov=pyiceberg_bigquery_catalog --cov-report=term-missing

.PHONY: lint
lint:
	poetry run pre-commit run --all-files

.PHONY: format
format:
	poetry run black .
	poetry run isort .

.PHONY: type-check
type-check:
	poetry run mypy pyiceberg_bigquery_catalog

.PHONY: clean
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf .ruff_cache
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

.PHONY: build
build: clean
	poetry build

.PHONY: publish
publish: build
	poetry publish

.PHONY: publish-test
publish-test: build
	poetry publish -r testpypi