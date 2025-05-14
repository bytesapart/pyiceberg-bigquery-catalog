# Installation and Usage Guide

This guide shows you how to install the `pyiceberg-bigquery-catalog` package and use it to work with Iceberg tables in BigQuery.

## Prerequisites

1. **Python 3.8+**
2. **Google Cloud Project** with BigQuery enabled
3. **Google Cloud Storage bucket** for storing table data
4. **Authentication** - One of:
   - `gcloud` CLI configured
   - Service account key file
   - Application Default Credentials

## Step 1: Installation

Since this package is not yet available on PyPI, you can install it from GitHub:

### Install from GitHub

Using pip:

```bash
# Install from the main branch
pip install git+https://github.com/bytesapart/pyiceberg-bigquery-catalog.git

# Install a specific version/tag
pip install git+https://github.com/bytesapart/pyiceberg-bigquery-catalog.git@v0.1.0

# Install a specific commit
pip install git+https://github.com/bytesapart/pyiceberg-bigquery-catalog.git@abcdef123456
```

Using poetry:

```bash
# Add to your project
poetry add git+https://github.com/bytesapart/pyiceberg-bigquery-catalog.git

# Or add to pyproject.toml
[tool.poetry.dependencies]
pyiceberg-bigquery-catalog = { git = "https://github.com/bytesapart/pyiceberg-bigquery-catalog.git", branch = "main" }
```

### Install from source

```bash
git clone https://github.com/bytesapart/pyiceberg-bigquery-catalog.git
cd pyiceberg-bigquery-catalog
pip install .

# Or using poetry
poetry install
```
