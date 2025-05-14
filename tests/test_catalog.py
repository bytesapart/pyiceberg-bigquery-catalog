# tests/test_catalog.py
"""Tests for BigQuery catalog implementation."""

import pytest
from unittest.mock import Mock, patch

from google.cloud.exceptions import NotFound
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchPropertyException
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType


def test_catalog_creation():
    """Test that catalog can be created with proper configuration."""
    with patch("pyiceberg_bigquery_catalog.catalog.bigquery.Client"):
        catalog = load_catalog(
            name="test",
            py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
            project_id="test-project",
            dataset_id="test_dataset",
            gcp_location="us-central1",
            warehouse="gs://test-bucket/warehouse",
        )

        assert catalog.name == "test"
        assert catalog.project_id == "test-project"
        assert catalog.dataset_id == "test_dataset"
        assert catalog.gcp_location == "us-central1"
        assert catalog.warehouse_location == "gs://test-bucket/warehouse"


def test_catalog_missing_project():
    """Test that catalog raises error when project is missing."""
    with pytest.raises(NoSuchPropertyException, match="Property 'project_id' is required"):
        load_catalog(
            name="test",
            py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
            dataset_id="test_dataset",
            warehouse="gs://test-bucket/warehouse",
        )


def test_catalog_missing_dataset():
    """Test that catalog raises error when dataset is missing."""
    with pytest.raises(NoSuchPropertyException, match="Property 'dataset_id' is required"):
        load_catalog(
            name="test",
            py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
            project_id="test-project",
            warehouse="gs://test-bucket/warehouse",
        )


def test_create_namespace():
    """Test creating a namespace (dataset) - should raise error as it's not supported."""
    with patch("pyiceberg_bigquery_catalog.catalog.bigquery.Client") as mock_client:
        catalog = load_catalog(
            name="test",
            py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
            project_id="test-project",
            dataset_id="test_dataset",
        )

        # Should raise ValidationError as namespace creation is not supported
        from pyiceberg.exceptions import ValidationError
        with pytest.raises(ValidationError, match="BigQuery catalog is configured with a fixed dataset"):
            catalog.create_namespace("new_dataset")


def test_table_operations():
    """Test basic table operations."""
    with patch("pyiceberg_bigquery_catalog.catalog.bigquery.Client") as mock_client:
        catalog = load_catalog(
            name="test",
            py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
            project_id="test-project",
            dataset_id="test_dataset",
            warehouse="gs://test-bucket/warehouse",
        )

        # Set up mocks
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.metadata = '{"metadata_location": "gs://bucket/metadata.json", "table_type": "iceberg"}'
        mock_table.external_data_configuration = mock_external_config

        # Test table_exists - table doesn't exist
        mock_client.return_value.get_table.side_effect = NotFound("Not found")
        assert not catalog.table_exists("test_table")

        # Test create_table
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
        )

        with patch.object(catalog, "file_io") as mock_file_io:
            mock_file_io.new_output.return_value = Mock()
            mock_client.return_value.get_table.side_effect = [NotFound("Not found"), mock_table]
            mock_client.return_value.create_table.return_value = mock_table

            # Should succeed (mock handles the actual creation)
            table = catalog.create_table(
                identifier="test_table",
                schema=schema,
            )

            assert table is not None


def test_list_operations():
    """Test listing operations."""
    with patch("pyiceberg_bigquery_catalog.catalog.bigquery.Client") as mock_client:
        catalog = load_catalog(
            name="test",
            py_catalog_impl="pyiceberg_bigquery_catalog.BigQueryCatalog",
            project_id="test-project",
            dataset_id="test_dataset",
        )

        # Mock list_namespaces - should only return the configured dataset
        namespaces = catalog.list_namespaces()
        assert ("test_dataset",) in namespaces

        # Mock list_tables
        mock_table = Mock()
        mock_table.table_id = "test_table"
        mock_client.return_value.list_tables.return_value = [mock_table]

        tables = catalog.list_tables()
        assert ("test_dataset", "test_table") in tables