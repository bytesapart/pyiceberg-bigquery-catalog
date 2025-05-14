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

Install the package from PyPI:

```bash
pip install pyiceberg-bigquery-catalog
```

Or install from source:

```bash
git clone https://github.com/yourusername/pyiceberg-bigquery-catalog.git
cd pyiceberg-bigquery-catalog
pip install .
```

## Step 2: Set Up Authentication

### Option A: Using gcloud (Recommended for development)

```bash
gcloud auth application-default login
gcloud config set project your-gcp-project
```

### Option B: Using Service Account

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

## Step 3: Configure the Catalog

### Option A: Configuration File

Create a `.pyiceberg.yaml` file in your project directory:

```yaml
catalog:
  my_bigquery_catalog:
    py-catalog-impl: pyiceberg_bigquery_catalog.BigQueryCatalog
    project_id: your-gcp-project
    dataset_id: your_dataset
    gcp_location: us-central1
    warehouse: gs://your-bucket/iceberg-warehouse
```

### Option B: Environment Variables

```bash
export PYICEBERG_CATALOG__MY_BIGQUERY_CATALOG__PY_CATALOG_IMPL=pyiceberg_bigquery_catalog.BigQueryCatalog
export PYICEBERG_CATALOG__MY_BIGQUERY_CATALOG__PROJECT_ID=your-gcp-project
export PYICEBERG_CATALOG__MY_BIGQUERY_CATALOG__DATASET_ID=your_dataset
export PYICEBERG_CATALOG__MY_BIGQUERY_CATALOG__GCP_LOCATION=us-central1
export PYICEBERG_CATALOG__MY_BIGQUERY_CATALOG__WAREHOUSE=gs://your-bucket/iceberg-warehouse
```

### Option C: Programmatic Configuration

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    name="my_bigquery_catalog",
    py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
    project_id="your-gcp-project",
    dataset_id="your_dataset",
    gcp_location="us-central1",
    warehouse="gs://your-bucket/iceberg-warehouse",
)
```

## Step 4: Basic Usage Example

```python
from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestampType

# Load the catalog
catalog = load_catalog("my_bigquery_catalog")

# No need to create namespace - dataset is pre-configured

# Define a schema
schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "timestamp", TimestampType(), required=True),
)

# Create a table (just table name, no dataset prefix)
table = catalog.create_table(
    identifier="users",
    schema=schema,
    properties={
        "format-version": "2",
    }
)

# Write some data
data = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "timestamp": pa.array([
        datetime.now(),
        datetime.now(),
        datetime.now(),
    ], type=pa.timestamp('us'))
})

table.append(data)

# Read the data
df = table.scan().to_pandas()
print(df)
```

## Step 5: Advanced Features

### Schema Evolution

```python
# Add a new column
with table.update_schema() as update:
    update.add_column(path="email", field_type=StringType())

# Write data with new schema
new_data = pa.table({
    "id": [4, 5],
    "name": ["David", "Eve"],
    "timestamp": pa.array([datetime.now(), datetime.now()], type=pa.timestamp('us')),
    "email": ["david@example.com", "eve@example.com"],
})

table.append(new_data)
```

### Time Travel

```python
# List all snapshots
for snapshot in table.snapshots():
    print(f"Snapshot {snapshot.snapshot_id} at {snapshot.timestamp_ms}")

# Read from a specific snapshot
snapshot_id = table.current_snapshot().snapshot_id
df_snapshot = table.scan(snapshot_id=snapshot_id).to_pandas()
```

### Partitioned Tables

```python
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

# Create a partitioned table
partition_spec = PartitionSpec(
    PartitionField(
        source_id=3,
        field_id=1000,
        transform=DayTransform(),
        name="timestamp_day"
    )
)

partitioned_table = catalog.create_table(
    identifier="iceberg_demo.events",
    schema=schema,
    partition_spec=partition_spec,
)
```

## Step 6: BigQuery Integration

Once tables are created, you can query them directly in BigQuery:

```sql
-- Query the Iceberg table from BigQuery
SELECT * 
FROM `your-gcp-project.iceberg_demo.users`
WHERE timestamp > CURRENT_TIMESTAMP() - INTERVAL 1 DAY;
```

## Troubleshooting

### Authentication Issues

If you encounter authentication errors:

1. Verify your credentials:
   ```bash
   gcloud auth list
   gcloud config get-value project
   ```

2. Check service account permissions:
   - `bigquery.datasets.*`
   - `bigquery.tables.*`
   - `storage.objects.*` (for GCS warehouse)

### Table Not Found

If BigQuery can't find your table:

1. Ensure the dataset exists:
   ```python
   namespaces = catalog.list_namespaces()
   print(namespaces)
   ```

2. Check table metadata:
   ```python
   tables = catalog.list_tables("iceberg_demo")
   print(tables)
   ```

### Permission Errors

Required IAM roles:
- `roles/bigquery.dataEditor`
- `roles/storage.objectAdmin` (for GCS warehouse)

## Best Practices

1. **Use format version 2** for better performance:
   ```python
   properties={"format-version": "2"}
   ```

2. **Set appropriate table properties**:
   ```python
   properties={
       "write.parquet.compression-codec": "snappy",
       "write.parquet.row-group-limit": "100000",
   }
   ```

3. **Use BigQuery connections** for external data:
   ```python
   properties={
       "bq_connection": "projects/my-project/locations/us/connections/my-connection"
   }
   ```

4. **Monitor table metadata**:
   ```python
   print(f"Table location: {table.location()}")
   print(f"Metadata location: {table.metadata_location}")
   ```

## Complete Example Script

```python
#!/usr/bin/env python3
"""
Complete example using PyIceberg with BigQuery catalog.
"""

import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    TimestampType,
    DoubleType
)

def main():
    # Configure catalog
    catalog = load_catalog(
        name="bigquery",
        py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
        gcp_project=os.getenv("GCP_PROJECT", "your-gcp-project"),
        gcp_location="us-central1",
        warehouse=os.getenv("GCS_WAREHOUSE", "gs://your-bucket/iceberg-warehouse"),
    )
    
    # Create namespace
    namespace = "analytics"
    catalog.create_namespace_if_not_exists(namespace)
    
    # Define schema
    schema = Schema(
        NestedField(1, "event_id", LongType(), required=True),
        NestedField(2, "user_id", LongType(), required=True),
        NestedField(3, "event_type", StringType(), required=True),
        NestedField(4, "timestamp", TimestampType(), required=True),
        NestedField(5, "value", DoubleType(), required=False),
    )
    
    # Create or load table
    table_id = f"{namespace}.user_events"
    
    if catalog.table_exists(table_id):
        table = catalog.load_table(table_id)
        print(f"Loaded existing table: {table_id}")
    else:
        table = catalog.create_table(
            identifier=table_id,
            schema=schema,
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "snappy",
            }
        )
        print(f"Created new table: {table_id}")
    
    # Generate sample data
    num_events = 1000
    data = pa.table({
        "event_id": range(num_events),
        "user_id": [i % 100 for i in range(num_events)],
        "event_type": ["click", "view", "purchase"][i % 3] for i in range(num_events)],
        "timestamp": pa.array([
            datetime.now() for _ in range(num_events)
        ], type=pa.timestamp('us')),
        "value": [i * 1.5 if i % 3 == 2 else None for i in range(num_events)],
    })
    
    # Write data
    print(f"Writing {num_events} events...")
    table.append(data)
    
    # Read and analyze data
    print("\nReading data...")
    df = table.scan().to_pandas()
    
    print(f"Total events: {len(df)}")
    print(f"Event types: {df['event_type'].value_counts().to_dict()}")
    print(f"Average purchase value: ${df[df['event_type'] == 'purchase']['value'].mean():.2f}")
    
    # Show sample data
    print("\nSample data:")
    print(df.head())
    
    # Show table info
    print("\nTable information:")
    print(f"  Location: {table.location()}")
    print(f"  Metadata: {table.metadata_location}")
    print(f"  Snapshots: {len(list(table.snapshots()))}")
    
    print("\nSetup complete! You can now query this table in BigQuery:")
    print(f"  SELECT * FROM `{catalog.gcp_project}.{table_id}` LIMIT 10")

if __name__ == "__main__":
    main()
```

## Next Steps

1. Explore the [PyIceberg documentation](https://py.iceberg.apache.org/)
2. Learn about [Iceberg table maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
3. Set up [table optimization](https://iceberg.apache.org/docs/latest/spark-procedures/#optimize)
4. Configure [data compaction](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files)

## Support

- Report issues: [GitHub Issues](https://github.com/yourusername/pyiceberg-bigquery-catalog/issues)
- Documentation: [PyIceberg Docs](https://py.iceberg.apache.org/)
- Community: [Apache Iceberg Slack](https://iceberg.apache.org/community/)
