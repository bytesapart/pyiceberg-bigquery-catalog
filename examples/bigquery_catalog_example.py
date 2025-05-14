# examples/bigquery_catalog_example.py
"""
Example of using PyIceberg with BigQuery Catalog.

This example demonstrates:
1. Setting up the catalog
2. Creating tables
3. Writing data
4. Reading data
5. Schema evolution
6. Time travel
"""

import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


def setup_catalog():
    """Set up the BigQuery catalog with configuration."""
    # You can configure the catalog via environment variables
    os.environ["PYICEBERG_CATALOG__BIGQUERY__PY-CATALOG-IMPL"] = "pyiceberg_bigquery_catalog.BigQueryCatalog"
    os.environ["PYICEBERG_CATALOG__BIGQUERY__PROJECT_ID"] = "your-gcp-project"
    os.environ["PYICEBERG_CATALOG__BIGQUERY__DATASET_ID"] = "your_dataset"
    os.environ["PYICEBERG_CATALOG__BIGQUERY__GCP_LOCATION"] = "us-central1"
    os.environ["PYICEBERG_CATALOG__BIGQUERY__WAREHOUSE"] = "gs://your-bucket/iceberg-warehouse"

    # Or use a configuration dictionary
    catalog = load_catalog(
        name="bigquery",
        **{
            "py-catalog-impl": "pyiceberg_bigquery_catalog.BigQueryCatalog",
            "project_id": "your-gcp-project",
            "dataset_id": "your_dataset",
            "gcp_location": "us-central1",
            "warehouse": "gs://your-bucket/iceberg-warehouse",
        }
    )

    return catalog


def create_sample_table(catalog):
    """Create a sample table with schema."""
    # Define schema
    schema = Schema(
        NestedField(1, "customer_id", LongType(), required=True),
        NestedField(2, "customer_name", StringType(), required=True),
        NestedField(3, "order_id", LongType(), required=True),
        NestedField(4, "order_date", TimestampType(), required=True),
        NestedField(5, "product", StringType(), required=True),
        NestedField(6, "quantity", LongType(), required=True),
        NestedField(7, "price", DoubleType(), required=True),
    )

    # Create table
    table_name = "sales_orders"

    if catalog.table_exists(table_name):
        print(f"Table {table_name} already exists, loading it...")
        table = catalog.load_table(table_name)
    else:
        print(f"Creating table {table_name}...")
        table = catalog.create_table(
            identifier=table_name,
            schema=schema,
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "snappy",
            }
        )
        print("Note: BigQuery external table will be created after first data write")

    return table


def write_sample_data(table):
    """Write sample data to the table."""
    # Create sample data with proper nullability settings
    data = pa.table({
        "customer_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "customer_name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"], type=pa.string()),
        "order_id": pa.array([1001, 1002, 1003, 1004, 1005], type=pa.int64()),
        "order_date": pa.array([
            datetime.now(),
            datetime.now(),
            datetime.now(),
            datetime.now(),
            datetime.now(),
        ], type=pa.timestamp('us')),
        "product": pa.array(["Widget A", "Widget B", "Widget C", "Widget A", "Widget B"], type=pa.string()),
        "quantity": pa.array([2, 1, 3, 1, 2], type=pa.int64()),
        "price": pa.array([29.99, 49.99, 19.99, 29.99, 49.99], type=pa.float64()),
    })

    # Create schema with required fields (not nullable)
    schema = pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("customer_name", pa.string(), nullable=False),
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("order_date", pa.timestamp('us'), nullable=False),
        pa.field("product", pa.string(), nullable=False),
        pa.field("quantity", pa.int64(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
    ])

    # Apply schema to data
    data = pa.table({
        column: data[column] for column in data.column_names
    }, schema=schema)

    print("\nWriting data to table...")
    table.append(data)
    print(f"Written {len(data)} rows")
    print("BigQuery external table should now be created with the first snapshot")


def read_and_analyze_data(table):
    """Read data from the table and perform simple analysis."""
    print("\nReading data from table...")

    # Scan all data
    df = table.scan().to_pandas()
    print(f"Total rows: {len(df)}")
    print("\nFirst 5 rows:")
    print(df.head())

    # Perform aggregation
    print("\nSales by product:")
    sales_by_product = df.groupby('product').agg({
        'quantity': 'sum',
        'price': lambda x: (x * df.loc[x.index, 'quantity']).sum()
    }).rename(columns={'price': 'total_revenue'})
    print(sales_by_product)

    # Filter data
    print("\nHigh-value orders (quantity * price > 50):")
    df['order_value'] = df['quantity'] * df['price']
    high_value_orders = df[df['order_value'] > 50]
    print(high_value_orders[['order_id', 'customer_name', 'product', 'order_value']])


def demonstrate_schema_evolution(table):
    """Demonstrate schema evolution capabilities."""
    print("\nDemonstrating schema evolution...")

    # Check if column already exists
    if 'discount_percentage' in [field.name for field in table.schema().fields]:
        print("'discount_percentage' column already exists")
    else:
        # Add a new column
        with table.update_schema() as update:
            update.add_column(
                path="discount_percentage",
                field_type=DoubleType(),
            )
        print("Added 'discount_percentage' column")

    # Write data with the new column
    data = pa.table({
        "customer_id": pa.array([6, 7], type=pa.int64()),
        "customer_name": pa.array(["Frank", "Grace"], type=pa.string()),
        "order_id": pa.array([1006, 1007], type=pa.int64()),
        "order_date": pa.array([
            datetime.now(),
            datetime.now(),
        ], type=pa.timestamp('us')),
        "product": pa.array(["Widget C", "Widget A"], type=pa.string()),
        "quantity": pa.array([1, 2], type=pa.int64()),
        "price": pa.array([19.99, 29.99], type=pa.float64()),
        "discount_percentage": pa.array([10.0, 5.0], type=pa.float64()),
    })

    # Create schema with required fields (not nullable)
    schema = pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("customer_name", pa.string(), nullable=False),
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("order_date", pa.timestamp('us'), nullable=False),
        pa.field("product", pa.string(), nullable=False),
        pa.field("quantity", pa.int64(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("discount_percentage", pa.float64(), nullable=True),  # Can be nullable
    ])

    # Apply schema to data
    data = pa.table({
        column: data[column] for column in data.column_names
    }, schema=schema)

    table.append(data)
    print("Written data with new schema")

    # Read the data again
    df = table.scan().to_pandas()
    print("\nData with new column:")
    print(df.tail())


def demonstrate_time_travel(table):
    """Demonstrate time travel capabilities."""
    print("\nDemonstrating time travel...")

    # List all snapshots
    snapshots = list(table.snapshots())
    print(f"Table has {len(snapshots)} snapshots")

    for i, snapshot in enumerate(snapshots):
        print(f"Snapshot {i}: ID={snapshot.snapshot_id}, "
              f"Time={datetime.fromtimestamp(snapshot.timestamp_ms / 1000)}")

    # Read from a specific snapshot (if multiple exist)
    if len(snapshots) > 1:
        print(f"\nReading from first snapshot (ID: {snapshots[0].snapshot_id})...")
        df_snapshot = table.scan(snapshot_id=snapshots[0].snapshot_id).to_pandas()
        print(f"Rows in first snapshot: {len(df_snapshot)}")


def verify_bigquery_table(catalog):
    """Verify the table exists in BigQuery."""
    print("\nVerifying BigQuery external table...")

    try:
        # Try to query the table directly in BigQuery
        from google.cloud import bigquery
        client = bigquery.Client(project=catalog.project_id)

        # Check if external table exists
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(catalog.project_id, catalog.dataset_id),
            "sales_orders"
        )

        try:
            bq_table = client.get_table(table_ref)
            print(f"✓ BigQuery external table exists: {catalog.project_id}.{catalog.dataset_id}.sales_orders")
            print(f"  Type: {bq_table.table_type}")
            print(f"  External: {bq_table.external_data_configuration is not None}")
            if bq_table.external_data_configuration:
                print(f"  Format: {bq_table.external_data_configuration.source_format}")
                # Show metadata
                if bq_table.external_data_configuration.metadata:
                    metadata = json.loads(bq_table.external_data_configuration.metadata)
                    print(f"  Table Type: {metadata.get('table_type', 'Unknown')}")
                    print(f"  Metadata Location: {metadata.get('metadata_location', 'Unknown')}")
        except Exception as e:
            print(f"✗ BigQuery external table not found: {e}")

    except ImportError:
        print("Note: google-cloud-bigquery not imported, skipping BigQuery verification")


def main():
    """Main example function."""
    print("PyIceberg BigQuery Catalog Example")
    print("==================================\n")

    # Set up catalog
    print("Step 1: Setting up BigQuery catalog...")
    catalog = setup_catalog()
    print(f"✓ Catalog created: {catalog.name}")
    print(f"✓ Project: {catalog.project_id}")
    print(f"✓ Dataset: {catalog.dataset_id}")
    print(f"✓ Location: {catalog.gcp_location}")
    print(f"✓ Warehouse: {catalog.warehouse_location}")

    # Create table
    print("\nStep 2: Creating Iceberg table...")
    table = create_sample_table(catalog)
    print(f"✓ Table location: {table.location()}")
    print(f"✓ Metadata location: {table.metadata_location}")

    # Write data
    print("\nStep 3: Writing initial data...")
    write_sample_data(table)

    # Verify BigQuery table was created
    verify_bigquery_table(catalog)

    # Read and analyze data
    print("\nStep 4: Reading and analyzing data...")
    read_and_analyze_data(table)

    # Demonstrate schema evolution
    print("\nStep 5: Schema evolution...")
    demonstrate_schema_evolution(table)

    # Demonstrate time travel
    print("\nStep 6: Time travel...")
    demonstrate_time_travel(table)

    # Show how to query in BigQuery
    print("\n" + "=" * 50)
    print("Example completed successfully!")
    print("\nYou can now query this table in BigQuery using:")
    print(f"  SELECT * FROM `{catalog.project_id}.{catalog.dataset_id}.sales_orders` LIMIT 10")
    print("\nNote: The BigQuery external table is created after the first data write.")
    print("If you don't see the table in BigQuery yet, make sure data was written successfully.")


if __name__ == "__main__":
    main()
