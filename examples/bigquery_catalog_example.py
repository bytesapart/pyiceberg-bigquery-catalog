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

import json
import logging
import os
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import DoubleType, LongType, NestedField, StringType, TimestampType

from pyiceberg_bigquery_catalog import BigQueryCatalog

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_catalog() -> Catalog:
    """Set up the BigQuery catalog with configuration."""
    # You can configure the catalog via environment variables
    os.environ[
        "PYICEBERG_CATALOG__BIGQUERY__PY-CATALOG-IMPL"
    ] = "pyiceberg_bigquery_catalog.BigQueryCatalog"
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
        },
    )

    return catalog


def create_sample_table(catalog: Catalog) -> Table:
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
        logger.info(f"Table {table_name} already exists, loading it...")
        table = catalog.load_table(table_name)
    else:
        logger.info(f"Creating table {table_name}...")
        table = catalog.create_table(
            identifier=table_name,
            schema=schema,
            properties={
                "format-version": "2",
                "write.parquet.compression-codec": "snappy",
            },
        )
        logger.info("Note: BigQuery external table will be created after first data write")

    return table


def write_sample_data(table: Table) -> None:
    """Write sample data to the table."""
    # Create sample data with proper nullability settings
    data = pa.table(
        {
            "customer_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "customer_name": pa.array(
                ["Alice", "Bob", "Charlie", "David", "Eve"], type=pa.string()
            ),
            "order_id": pa.array([1001, 1002, 1003, 1004, 1005], type=pa.int64()),
            "order_date": pa.array(
                [
                    datetime.now(),
                    datetime.now(),
                    datetime.now(),
                    datetime.now(),
                    datetime.now(),
                ],
                type=pa.timestamp("us"),
            ),
            "product": pa.array(
                ["Widget A", "Widget B", "Widget C", "Widget A", "Widget B"], type=pa.string()
            ),
            "quantity": pa.array([2, 1, 3, 1, 2], type=pa.int64()),
            "price": pa.array([29.99, 49.99, 19.99, 29.99, 49.99], type=pa.float64()),
        }
    )

    # Create schema with required fields (not nullable)
    schema = pa.schema(
        [
            pa.field("customer_id", pa.int64(), nullable=False),
            pa.field("customer_name", pa.string(), nullable=False),
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("order_date", pa.timestamp("us"), nullable=False),
            pa.field("product", pa.string(), nullable=False),
            pa.field("quantity", pa.int64(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
        ]
    )

    # Apply schema to data
    data = pa.table({column: data[column] for column in data.column_names}, schema=schema)

    logger.info("Writing data to table...")
    table.append(data)
    logger.info(f"Written {len(data)} rows")
    logger.info("BigQuery external table should now be created with the first snapshot")


def read_and_analyze_data(table: Table) -> None:
    """Read data from the table and perform simple analysis."""
    logger.info("Reading data from table...")

    # Scan all data
    df = table.scan().to_pandas()
    logger.info(f"Total rows: {len(df)}")
    logger.info(f"First 5 rows:\n{df.head()}")

    # Perform aggregation
    logger.info("Sales by product:")
    sales_by_product = (
        df.groupby("product")
        .agg({"quantity": "sum", "price": lambda x: (x * df.loc[x.index, "quantity"]).sum()})
        .rename(columns={"price": "total_revenue"})
    )
    logger.info(f"\n{sales_by_product}")

    # Filter data
    logger.info("High-value orders (quantity * price > 50):")
    df["order_value"] = df["quantity"] * df["price"]
    high_value_orders = df[df["order_value"] > 50]
    logger.info(f"\n{high_value_orders[['order_id', 'customer_name', 'product', 'order_value']]}")


def demonstrate_schema_evolution(table: Table) -> None:
    """Demonstrate schema evolution capabilities."""
    logger.info("Demonstrating schema evolution...")

    # Check if column already exists
    if "discount_percentage" in [field.name for field in table.schema().fields]:
        logger.info("'discount_percentage' column already exists")
    else:
        # Add a new column
        with table.update_schema() as update:
            update.add_column(
                path="discount_percentage",
                field_type=DoubleType(),
            )
        logger.info("Added 'discount_percentage' column")

    # Write data with the new column
    data = pa.table(
        {
            "customer_id": pa.array([6, 7], type=pa.int64()),
            "customer_name": pa.array(["Frank", "Grace"], type=pa.string()),
            "order_id": pa.array([1006, 1007], type=pa.int64()),
            "order_date": pa.array(
                [
                    datetime.now(),
                    datetime.now(),
                ],
                type=pa.timestamp("us"),
            ),
            "product": pa.array(["Widget C", "Widget A"], type=pa.string()),
            "quantity": pa.array([1, 2], type=pa.int64()),
            "price": pa.array([19.99, 29.99], type=pa.float64()),
            "discount_percentage": pa.array([10.0, 5.0], type=pa.float64()),
        }
    )

    # Create schema with required fields (not nullable)
    schema = pa.schema(
        [
            pa.field("customer_id", pa.int64(), nullable=False),
            pa.field("customer_name", pa.string(), nullable=False),
            pa.field("order_id", pa.int64(), nullable=False),
            pa.field("order_date", pa.timestamp("us"), nullable=False),
            pa.field("product", pa.string(), nullable=False),
            pa.field("quantity", pa.int64(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            pa.field("discount_percentage", pa.float64(), nullable=True),  # Can be nullable
        ]
    )

    # Apply schema to data
    data = pa.table({column: data[column] for column in data.column_names}, schema=schema)

    table.append(data)
    logger.info("Written data with new schema")

    # Read the data again
    df = table.scan().to_pandas()
    logger.info(f"Data with new column:\n{df.tail()}")


def demonstrate_time_travel(table: Table) -> None:
    """Demonstrate time travel capabilities."""
    logger.info("Demonstrating time travel...")

    # List all snapshots
    snapshots = list(table.snapshots())
    logger.info(f"Table has {len(snapshots)} snapshots")

    for i, snapshot in enumerate(snapshots):
        timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
        logger.info(f"Snapshot {i}: ID={snapshot.snapshot_id}, Time={timestamp}")

    # Read from a specific snapshot (if multiple exist)
    if len(snapshots) > 1:
        logger.info(f"Reading from first snapshot (ID: {snapshots[0].snapshot_id})...")
        df_snapshot = table.scan(snapshot_id=snapshots[0].snapshot_id).to_pandas()
        logger.info(f"Rows in first snapshot: {len(df_snapshot)}")


def verify_bigquery_table(catalog: BigQueryCatalog) -> None:
    """Verify the table exists in BigQuery."""
    logger.info("Verifying BigQuery external table...")

    try:
        # Try to query the table directly in BigQuery
        from google.cloud import bigquery

        # Ensure project_id and dataset_id are not None
        if not catalog.project_id or not catalog.dataset_id:
            logger.error("Catalog project_id or dataset_id is not set")
            return

        client = bigquery.Client(project=catalog.project_id)

        # Check if external table exists
        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(catalog.project_id, catalog.dataset_id), "sales_orders"
        )

        try:
            bq_table = client.get_table(table_ref)
            location = f"{catalog.project_id}.{catalog.dataset_id}.sales_orders"
            logger.info(f"✓ BigQuery external table exists: {location}")
            logger.info(f"  Type: {bq_table.table_type}")
            is_external = bq_table.external_data_configuration is not None
            logger.info(f"  External: {is_external}")
            if bq_table.external_data_configuration:
                config = bq_table.external_data_configuration
                logger.info(f"  Format: {config.source_format}")
                # Get metadata from table description instead
                if bq_table.description:
                    try:
                        desc_data = json.loads(bq_table.description)
                        if iceberg_metadata := desc_data.get("iceberg_metadata"):
                            table_type = iceberg_metadata.get("table_type", "Unknown")
                            logger.info(f"  Table Type: {table_type}")
                            metadata_loc = iceberg_metadata.get("metadata_location", "Unknown")
                            logger.info(f"  Metadata Location: {metadata_loc}")
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse table description as JSON")
                else:
                    logger.info("  No metadata found in table description")
        except Exception as e:
            logger.error(f"✗ BigQuery external table not found: {e}")

    except ImportError:
        logger.warning("google-cloud-bigquery not imported, skipping verification")


def main() -> None:
    """Run the PyIceberg BigQuery Catalog example."""
    logger.info("PyIceberg BigQuery Catalog Example")
    logger.info("==================================")

    # Set up catalog
    logger.info("Step 1: Setting up BigQuery catalog...")
    catalog = setup_catalog()
    # Verify we have a BigQueryCatalog instance
    if not isinstance(catalog, BigQueryCatalog):
        logger.error("Catalog is not a BigQueryCatalog instance!")
        return

    # Ensure required attributes are set
    if not catalog.project_id or not catalog.dataset_id:
        logger.error("Catalog project_id or dataset_id is not set")
        return

    logger.info(f"✓ Catalog created: {catalog.name}")
    logger.info(f"✓ Project: {catalog.project_id}")
    logger.info(f"✓ Dataset: {catalog.dataset_id}")
    logger.info(f"✓ Location: {catalog.gcp_location}")
    logger.info(f"✓ Warehouse: {catalog.warehouse_location}")

    # Create table
    logger.info("\nStep 2: Creating Iceberg table...")
    table = create_sample_table(catalog)
    logger.info(f"✓ Table location: {table.location()}")
    logger.info(f"✓ Metadata location: {table.metadata_location}")

    # Write data
    logger.info("\nStep 3: Writing initial data...")
    write_sample_data(table)

    # Verify BigQuery table was created
    verify_bigquery_table(catalog)

    # Read and analyze data
    logger.info("\nStep 4: Reading and analyzing data...")
    read_and_analyze_data(table)

    # Demonstrate schema evolution
    logger.info("\nStep 5: Schema evolution...")
    demonstrate_schema_evolution(table)

    # Demonstrate time travel
    logger.info("\nStep 6: Time travel...")
    demonstrate_time_travel(table)

    # Show how to query in BigQuery
    logger.info("=" * 50)
    logger.info("Example completed successfully!")
    logger.info("You can now query this table in BigQuery using:")
    query = (
        f"SELECT * FROM `{catalog.project_id}.{catalog.dataset_id}.sales_orders` LIMIT 10"  # nosec
    )
    logger.info(f"  {query}")
    logger.info("Note: The BigQuery external table is created after first data write.")
    logger.info("If you don't see the table in BigQuery, ensure data was written successfully.")


if __name__ == "__main__":
    main()
