# PyIceberg BigQuery Catalog

[![PyPI version](https://badge.fury.io/py/pyiceberg-bigquery-catalog.svg)](https://badge.fury.io/py/pyiceberg-bigquery-catalog)
[![Python](https://img.shields.io/pypi/pyversions/pyiceberg-bigquery-catalog.svg)](https://pypi.org/project/pyiceberg-bigquery-catalog/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A BigQuery catalog implementation for [PyIceberg](https://py.iceberg.apache.org/). This catalog enables you to use Google BigQuery as a metadata store for Apache Iceberg tables.

## Features

- Store Iceberg table metadata in BigQuery
- Create, drop, and manage Iceberg tables using BigQuery as the catalog
- Seamlessly integrate with PyIceberg's API
- Support for table operations including schema evolution and time travel
- Works with Google Cloud Storage (GCS) as the data warehouse

## Installation

```bash
pip install pyiceberg-bigquery-catalog
```

## Quick Start

### 1. Configure the Catalog

Create a `.pyiceberg.yaml` configuration file:

```yaml
catalog:
  bigquery:
    py-catalog-impl: pyiceberg_bigquery_catalog.BigQueryCatalog
    project_id: my-gcp-project
    dataset_id: my_dataset
    gcp_location: us-central1
    warehouse: gs://my-bucket/warehouse
```

### 2. Use the Catalog

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType

# Load the BigQuery catalog
catalog = load_catalog("bigquery")

# The dataset is already configured, no need to create namespace
# Define a schema
schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=True),
)

# Create a table in the configured dataset
table = catalog.create_table(
    identifier="my_table",  # Just the table name, dataset is pre-configured
    schema=schema,
)

# Write data using PyArrow
import pyarrow as pa

df = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
})

table.append(df)
```

## Authentication

The BigQuery catalog uses Google Cloud authentication. You can authenticate using:

1. **Application Default Credentials (recommended)**:
   ```bash
   gcloud auth application-default login
   ```

2. **Service Account Key**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
   ```

3. **Workload Identity** (for GKE deployments)

## Configuration Options

| Property | Description | Default | Required |
|----------|-------------|---------|----------|
| `project_id` | GCP project ID | - | Yes |
| `dataset_id` | BigQuery dataset ID | - | Yes |
| `gcp_location` | BigQuery dataset location | `us` | No |
| `warehouse` | GCS path for table data | - | No |
| `filter_unsupported_tables` | Only show Iceberg tables when listing | `false` | No |
| `bq_connection` | BigQuery connection for external data | - | No |

## Required Permissions

The service account needs these BigQuery IAM permissions:
- `bigquery.datasets.create`
- `bigquery.datasets.get`
- `bigquery.datasets.update`
- `bigquery.datasets.delete`
- `bigquery.tables.create`
- `bigquery.tables.get`
- `bigquery.tables.update`
- `bigquery.tables.delete`

For GCS (data warehouse):
- `storage.objects.create`
- `storage.objects.get`
- `storage.objects.delete`

## Advanced Usage

### Using a BigQuery Connection

For tables with data in GCS, you can specify a BigQuery connection:

```python
table = catalog.create_table(
    identifier="my_table",  # Just table name, dataset is pre-configured
    schema=schema,
    properties={
        "bq_connection": "projects/my-project/locations/us/connections/my-connection"
    }
)
```

### Schema Evolution

```python
# Add a new column
with table.update_schema() as update:
    update.add_column(
        path="email",
        field_type=StringType(),
    )
```

### Time Travel

```python
# List snapshots
for snapshot in table.snapshots():
    print(f"Snapshot {snapshot.snapshot_id} at {snapshot.timestamp_ms}")

# Read from a specific snapshot
df = table.scan(snapshot_id=specific_snapshot_id).to_pandas()
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/pyiceberg-bigquery-catalog.git
cd pyiceberg-bigquery-catalog

# Install poetry
pip install poetry

# Install dependencies
poetry install

# Install pre-commit hooks
poetry run pre-commit install

# Run tests
poetry run pytest

# Format code
poetry run black .
poetry run isort .

# Type checking
poetry run mypy .
```

### Running Tests

```bash
poetry run pytest tests/
```

### Code Quality

This project uses pre-commit hooks to maintain code quality. After installing with `poetry install`, set up the hooks:

```bash
# Install pre-commit hooks
poetry run pre-commit install

# Run hooks manually on all files
poetry run pre-commit run --all-files

# Update hook versions
poetry run pre-commit autoupdate
```

The pre-commit hooks will:
- Format code with Black and isort
- Check for common Python issues with flake8 and ruff
- Run type checking with mypy
- Check for security issues with bandit
- Validate YAML, JSON, and TOML files
- Check for large files and merge conflicts
- Ensure consistent line endings
- Validate commit messages with commitizen`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -am 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [PyIceberg](https://py.iceberg.apache.org/) - The Python implementation of Apache Iceberg
- [Apache Iceberg](https://iceberg.apache.org/) - An open table format for huge analytic datasets
- [Google BigQuery](https://cloud.google.com/bigquery) - Serverless data warehouse
