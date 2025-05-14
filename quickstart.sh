#!/bin/bash
# quick_start.sh - Quick start script for PyIceberg BigQuery Catalog

echo "PyIceberg BigQuery Catalog - Quick Start"
echo "======================================="
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
if [[ $(echo "$PYTHON_VERSION < 3.8" | bc) -eq 1 ]]; then
    echo "Error: Python 3.8 or higher is required. Found: $PYTHON_VERSION"
    exit 1
fi

echo "Step 1: Installing pyiceberg-bigquery-catalog..."
pip install pyiceberg-bigquery-catalog

echo ""
echo "Step 2: Creating example configuration..."

# Create example configuration file
cat > .pyiceberg.yaml << EOF
catalog:
  bigquery_demo:
    py-catalog-impl: pyiceberg_bigquery_catalog.BigQueryCatalog
    project_id: ${GCP_PROJECT:-your-gcp-project}
    dataset_id: ${DATASET_ID:-iceberg_demo}
    gcp_location: ${GCP_LOCATION:-us-central1}
    warehouse: ${GCS_WAREHOUSE:-gs://your-bucket/iceberg-warehouse}
EOF

echo "Created .pyiceberg.yaml configuration file"

echo ""
echo "Step 3: Creating example Python script..."

# Create example Python script
cat > bigquery_iceberg_demo.py << 'EOF'
#!/usr/bin/env python3
"""
Demo script for PyIceberg BigQuery Catalog
"""

import os
from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestampType

def main():
    # Check for required environment variables
    if not os.getenv("GCP_PROJECT"):
        print("Please set GCP_PROJECT environment variable")
        print("export GCP_PROJECT=your-gcp-project")
        return

    if not os.getenv("DATASET_ID"):
        print("Please set DATASET_ID environment variable")
        print("export DATASET_ID=your_dataset")
        return

    if not os.getenv("GCS_WAREHOUSE"):
        print("Please set GCS_WAREHOUSE environment variable")
        print("export GCS_WAREHOUSE=gs://your-bucket/iceberg-warehouse")
        return

    print("Loading BigQuery catalog...")
    catalog = load_catalog("bigquery_demo")

    # The dataset is pre-configured, no need to create namespace
    print(f"Using dataset: {catalog.dataset_id}")

    # Define schema
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "message", StringType(), required=True),
        NestedField(3, "timestamp", TimestampType(), required=True),
    )

    # Create table (just table name, dataset is pre-configured)
    table_name = "demo_table"
    print(f"Creating table: {table_name}")

    if catalog.table_exists(table_name):
        table = catalog.load_table(table_name)
        print("Table already exists, using existing table")
    else:
        table = catalog.create_table(
            identifier=table_name,
            schema=schema,
            properties={"format-version": "2"}
        )
        print("Table created successfully")

    # Write sample data
    print("Writing sample data...")
    data = pa.table({
        "id": [1, 2, 3],
        "message": ["Hello", "World", "from BigQuery Iceberg!"],
        "timestamp": pa.array([
            datetime.now(),
            datetime.now(),
            datetime.now()
        ], type=pa.timestamp('us'))
    })

    table.append(data)
    print("Data written successfully")

    # Read data back
    print("\nReading data from table:")
    df = table.scan().to_pandas()
    print(df)

    print(f"\nSuccess! You can now query this table in BigQuery:")
    print(f"SELECT * FROM `{os.getenv('GCP_PROJECT')}.{catalog.dataset_id}.{table_name}`")

if __name__ == "__main__":
    main()
EOF

chmod +x bigquery_iceberg_demo.py

echo "Created bigquery_iceberg_demo.py"
echo ""
echo "Step 4: Setup instructions"
echo "========================="
echo ""
echo "1. Set up Google Cloud authentication:"
echo "   gcloud auth application-default login"
echo ""
echo "2. Set required environment variables:"
echo "   export GCP_PROJECT=your-gcp-project"
echo "   export DATASET_ID=your_dataset"
echo "   export GCS_WAREHOUSE=gs://your-bucket/iceberg-warehouse"
echo ""
echo "3. Run the demo:"
echo "   python3 bigquery_iceberg_demo.py"
echo ""
echo "For more details, see the README.md and INSTALLATION_GUIDE.md"