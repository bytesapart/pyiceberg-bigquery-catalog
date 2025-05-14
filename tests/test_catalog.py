# tests/test_catalog.py
"""Tests for BigQuery catalog implementation."""

import json
from unittest.mock import Mock, patch, MagicMock, call

import pytest
from google.api_core.exceptions import NotFound, Conflict, PreconditionFailed
from google.cloud import bigquery

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
    ValidationError,
    CommitFailedException,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table, CommitTableRequest, CommitTableResponse
from pyiceberg.types import LongType, NestedField, StringType
from pyiceberg_bigquery_catalog import BigQueryCatalog


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    with patch("pyiceberg_bigquery_catalog.catalog.bigquery.Client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def catalog(mock_bigquery_client):
    """Create a BigQueryCatalog instance with a mocked client."""
    with patch("pyiceberg_bigquery_catalog.catalog.load_file_io") as mock_file_io:
        mock_file_io.return_value = Mock()
        catalog = BigQueryCatalog(
            name="test",
            project_id="test-project",
            dataset_id="test_dataset",
            gcp_location="us-central1",
            warehouse="gs://test-bucket/warehouse",
        )
        catalog._file_io = Mock()
        return catalog


class TestBigQueryCatalogInitialization:
    """Test catalog initialization."""

    def test_missing_project_id(self):
        """Test that catalog raises error when project_id is missing."""
        with pytest.raises(
                NoSuchPropertyException, match="Property 'project_id' is required"
        ):
            BigQueryCatalog(
                name="test",
                dataset_id="test_dataset",
                warehouse="gs://test-bucket/warehouse",
            )

    def test_missing_dataset_id(self):
        """Test that catalog raises error when dataset_id is missing."""
        with pytest.raises(
                NoSuchPropertyException, match="Property 'dataset_id' is required"
        ):
            BigQueryCatalog(
                name="test",
                project_id="test-project",
                warehouse="gs://test-bucket/warehouse",
            )

    def test_successful_initialization(self, mock_bigquery_client):
        """Test successful catalog initialization."""
        mock_dataset = Mock()
        mock_bigquery_client.get_dataset.return_value = mock_dataset

        catalog = BigQueryCatalog(
            name="test",
            project_id="test-project",
            dataset_id="test_dataset",
            gcp_location="us-central1",
            warehouse="gs://test-bucket/warehouse",
            filter_unsupported_tables="true",
        )

        assert catalog.name == "test"
        assert catalog.project_id == "test-project"
        assert catalog.dataset_id == "test_dataset"
        assert catalog.gcp_location == "us-central1"
        assert catalog.warehouse_location == "gs://test-bucket/warehouse"
        assert catalog.filter_unsupported_tables is True

    def test_dataset_creation_on_init(self, mock_bigquery_client):
        """Test that dataset is created if it doesn't exist."""
        mock_bigquery_client.get_dataset.side_effect = NotFound("Dataset not found")

        with patch("pyiceberg_bigquery_catalog.catalog.load_file_io"):
            catalog = BigQueryCatalog(
                name="test",
                project_id="test-project",
                dataset_id="test_dataset",
                gcp_location="us-central1",
                warehouse="gs://test-bucket/warehouse",
            )

        mock_bigquery_client.create_dataset.assert_called_once()
        created_dataset = mock_bigquery_client.create_dataset.call_args[0][0]
        assert created_dataset.dataset_id == "test_dataset"
        assert created_dataset.location == "us-central1"


class TestTableOperations:
    """Test table operations."""

    def test_create_table(self, catalog, mock_bigquery_client):
        """Test creating a new table."""
        # Mock file operations
        catalog.file_io.new_output.return_value = Mock()

        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
        )

        # Table doesn't exist yet
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        table = catalog.create_table(
            identifier="test_table",
            schema=schema,
            properties={"format-version": "2"},
        )

        assert isinstance(table, Table)
        assert table.identifier == ("test", "test_dataset", "test_table")

        # Verify metadata was written
        catalog.file_io.new_output.assert_called_once()

    def test_create_table_already_exists(self, catalog, mock_bigquery_client):
        """Test error when creating a table that already exists."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
        )

        # Mock existing table
        mock_table = Mock()
        mock_bigquery_client.get_table.return_value = mock_table

        with pytest.raises(TableAlreadyExistsError):
            catalog.create_table(identifier="test_table", schema=schema)

    def test_load_table(self, catalog, mock_bigquery_client):
        """Test loading an existing table."""
        # Mock BigQuery table with Iceberg metadata
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.metadata = json.dumps({
            "table_type": "iceberg",
            "metadata_location": "gs://bucket/metadata.json",
        })
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        # Mock metadata file
        catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input:
            mock_metadata = Mock()
            mock_from_input.table_metadata.return_value = mock_metadata

            table = catalog.load_table("test_table")

            assert isinstance(table, Table)
            assert table.identifier == ("test", "test_dataset", "test_table")

    def test_load_nonexistent_table(self, catalog, mock_bigquery_client):
        """Test error when loading a table that doesn't exist."""
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        with pytest.raises(NoSuchTableError):
            catalog.load_table("nonexistent_table")

    def test_drop_table(self, catalog, mock_bigquery_client):
        """Test dropping a table."""
        # Mock existing table
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.metadata = json.dumps({
            "table_type": "iceberg",
            "metadata_location": "gs://bucket/metadata.json",
        })
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile"):
            catalog.drop_table("test_table")

        mock_bigquery_client.delete_table.assert_called_once()

    def test_rename_table_not_supported(self, catalog):
        """Test that rename_table raises ValidationError."""
        with pytest.raises(ValidationError, match="rename operation is not supported"):
            catalog.rename_table("old_table", "new_table")

    def test_table_exists(self, catalog, mock_bigquery_client):
        """Test checking if a table exists."""
        # Mock existing table
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.metadata = json.dumps({
            "table_type": "iceberg",
            "metadata_location": "gs://bucket/metadata.json",
        })
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile"):
            assert catalog.table_exists("test_table") is True

        # Test non-existent table
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")
        assert catalog.table_exists("nonexistent_table") is False

    def test_commit_table(self, catalog, mock_bigquery_client):
        """Test committing table changes."""
        # Mock existing table
        mock_bq_table = Mock()
        mock_external_config = Mock()
        mock_external_config.metadata = json.dumps({
            "table_type": "iceberg",
            "metadata_location": "gs://bucket/metadata.json",
        })
        mock_bq_table.external_data_configuration = mock_external_config
        mock_bq_table.etag = "test-etag"
        mock_bigquery_client.get_table.return_value = mock_bq_table

        # Mock table and metadata
        mock_table = Mock()
        mock_table.metadata_location = "gs://bucket/warehouse/test_dataset.db/test_table/metadata/00001-abc.metadata.json"

        catalog.file_io.new_input.return_value = Mock()
        catalog.file_io.new_output.return_value = Mock()

        # Create mock updates
        from pyiceberg.table.update import UpdateSchema
        from pyiceberg.catalog import Identifier

        # Create a simple update
        schema_update = UpdateSchema()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile"), \
                patch("pyiceberg_bigquery_catalog.catalog.ToOutputFile"), \
                patch("pyiceberg_bigquery_catalog.catalog.update_table_metadata") as mock_update:
            mock_updated_metadata = Mock()
            mock_update.return_value = mock_updated_metadata

            response = catalog.commit_table(
                table=mock_table,
                requirements=(),
                updates=(schema_update,)
            )

            assert isinstance(response, CommitTableResponse)
            assert response.metadata == mock_updated_metadata


class TestNamespaceOperations:
    """Test namespace operations."""

    def test_create_namespace(self, catalog, mock_bigquery_client):
        """Test creating a namespace."""
        catalog.create_namespace("new_dataset", {"location": "us-central1"})

        mock_bigquery_client.create_dataset.assert_called_once()
        created_dataset = mock_bigquery_client.create_dataset.call_args[0][0]
        assert created_dataset.dataset_id == "new_dataset"
        assert created_dataset.location == "us-central1"

    def test_create_namespace_already_exists(self, catalog, mock_bigquery_client):
        """Test error when creating a namespace that already exists."""
        mock_bigquery_client.create_dataset.side_effect = Conflict("Dataset already exists")

        with pytest.raises(NamespaceAlreadyExistsError):
            catalog.create_namespace("existing_dataset")

    def test_drop_namespace(self, catalog, mock_bigquery_client):
        """Test dropping a namespace."""
        catalog.drop_namespace("test_dataset")
        mock_bigquery_client.delete_dataset.assert_called_once()

    def test_drop_nonexistent_namespace(self, catalog, mock_bigquery_client):
        """Test error when dropping a namespace that doesn't exist."""
        mock_bigquery_client.delete_dataset.side_effect = NotFound("Dataset not found")

        with pytest.raises(NoSuchNamespaceError):
            catalog.drop_namespace("nonexistent_dataset")

    def test_list_namespaces(self, catalog, mock_bigquery_client):
        """Test listing namespaces."""
        mock_dataset1 = Mock()
        mock_dataset1.dataset_id = "dataset1"
        mock_dataset2 = Mock()
        mock_dataset2.dataset_id = "dataset2"

        mock_bigquery_client.list_datasets.return_value = [mock_dataset1, mock_dataset2]

        namespaces = catalog.list_namespaces()
        assert ("dataset1",) in namespaces
        assert ("dataset2",) in namespaces

    def test_load_namespace_properties(self, catalog, mock_bigquery_client):
        """Test loading namespace properties."""
        mock_dataset = Mock()
        mock_dataset.labels = {
            "env": "production",
            "team": "data",
        }
        mock_dataset.location = "us-central1"
        mock_bigquery_client.get_dataset.return_value = mock_dataset

        properties = catalog.load_namespace_properties("test_dataset")
        assert properties["env"] == "production"
        assert properties["team"] == "data"
        assert properties["gcp_location"] == "us-central1"

    def test_update_namespace_properties(self, catalog, mock_bigquery_client):
        """Test updating namespace properties."""
        mock_dataset = Mock()
        mock_dataset.labels = {
            "env": "production",
            "team": "data",
        }
        mock_bigquery_client.get_dataset.return_value = mock_dataset

        summary = catalog.update_namespace_properties(
            namespace="test_dataset",
            removals={"env"},
            updates={"team": "analytics", "region": "us"},
        )

        assert isinstance(summary, PropertiesUpdateSummary)
        assert "env" in summary.removed
        assert "team" in summary.updated
        assert "region" in summary.updated

        mock_bigquery_client.update_dataset.assert_called_once()


class TestListOperations:
    """Test list operations."""

    def test_list_tables(self, catalog, mock_bigquery_client):
        """Test listing tables in a namespace."""
        mock_table1 = Mock()
        mock_table1.table_id = "table1"
        mock_table2 = Mock()
        mock_table2.table_id = "table2"

        mock_bigquery_client.list_tables.return_value = [mock_table1, mock_table2]

        tables = catalog.list_tables("test_dataset")
        assert ("test_dataset", "table1") in tables
        assert ("test_dataset", "table2") in tables

    def test_list_tables_with_filter(self, catalog, mock_bigquery_client):
        """Test listing tables with filter_unsupported_tables enabled."""
        catalog.filter_unsupported_tables = True

        mock_table1 = Mock()
        mock_table1.table_id = "iceberg_table"
        mock_table1.reference = Mock()

        mock_table2 = Mock()
        mock_table2.table_id = "regular_table"
        mock_table2.reference = Mock()

        mock_bigquery_client.list_tables.return_value = [mock_table1, mock_table2]

        # Mock get_table to differentiate between Iceberg and regular tables
        mock_iceberg_table = Mock()
        mock_iceberg_config = Mock()
        mock_iceberg_config.metadata = json.dumps({
            "table_type": "iceberg",
            "metadata_location": "gs://bucket/metadata.json",
        })
        mock_iceberg_table.external_data_configuration = mock_iceberg_config

        mock_regular_table = Mock()
        mock_regular_table.external_data_configuration = None

        mock_bigquery_client.get_table.side_effect = [
            mock_iceberg_table,  # First table is Iceberg
            mock_regular_table,  # Second table is regular
        ]

        tables = catalog.list_tables("test_dataset")
        assert ("test_dataset", "iceberg_table") in tables
        assert ("test_dataset", "regular_table") not in tables


class TestRegisterTable:
    """Test register table functionality."""

    def test_register_table(self, catalog, mock_bigquery_client):
        """Test registering an existing Iceberg table."""
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input:
            mock_metadata = Mock()
            mock_from_input.table_metadata.return_value = mock_metadata

            table = catalog.register_table(
                identifier="new_table",
                metadata_location="gs://bucket/metadata.json",
            )

            assert isinstance(table, Table)
            mock_bigquery_client.create_table.assert_called_once()


class TestHelperMethods:
    """Test helper methods."""

    def test_sanitize_label_key(self, catalog):
        """Test label key sanitization."""
        assert catalog._sanitize_label_key("valid_key") == "valid_key"
        assert catalog._sanitize_label_key("Invalid-Key") == "invalid_key"
        assert catalog._sanitize_label_key("123key") == "l_123key"
        assert catalog._sanitize_label_key("") == ""

        # Test truncation
        long_key = "a" * 100
        assert len(catalog._sanitize_label_key(long_key)) == 63

    def test_sanitize_label_value(self, catalog):
        """Test label value sanitization."""
        assert catalog._sanitize_label_value("valid-value") == "valid-value"
        assert catalog._sanitize_label_value("Invalid Value!") == "invalid_value_"
        assert catalog._sanitize_label_value("") == ""

    def test_validate_identifier(self, catalog):
        """Test identifier validation."""
        # Valid identifiers
        assert catalog._validate_identifier("table_name") == "table_name"
        assert catalog._validate_identifier("test_dataset.table_name") == "table_name"
        assert catalog._validate_identifier(("test_dataset", "table_name")) == "table_name"

        # Invalid identifiers
        with pytest.raises(ValidationError):
            catalog._validate_identifier("wrong_dataset.table_name")

        with pytest.raises(ValidationError):
            catalog._validate_identifier("project.dataset.table")

    def test_resolve_table_location(self, catalog):
        """Test table location resolution."""
        # With explicit location
        location = catalog._resolve_table_location(
            "gs://custom/location",
            "dataset",
            "table"
        )
        assert location == "gs://custom/location"

        # Without location, use warehouse
        location = catalog._resolve_table_location(
            None,
            "dataset",
            "table"
        )
        assert location == "gs://test-bucket/warehouse/dataset.db/table"

        # Without location or warehouse
        catalog.warehouse_location = None
        with pytest.raises(ValueError, match="No default path is set"):
            catalog._resolve_table_location(None, "dataset", "table")

    def test_get_metadata_location(self, catalog):
        """Test metadata location generation."""
        location = catalog._get_metadata_location("gs://bucket/table", 0)
        assert location.startswith("gs://bucket/table/metadata/00000-")
        assert location.endswith(".metadata.json")

        location = catalog._get_metadata_location("gs://bucket/table", 5)
        assert location.startswith("gs://bucket/table/metadata/00005-")

        with pytest.raises(ValueError):
            catalog._get_metadata_location("gs://bucket/table", -1)


class TestErrorScenarios:
    """Test various error scenarios."""

    def test_etag_validation_error(self, catalog, mock_bigquery_client):
        """Test error when table etag is empty."""
        mock_table = Mock()
        mock_table.etag = ""
        mock_table.table_id = "test_table"

        with pytest.raises(ValidationError, match="Etag of table test_table is empty"):
            catalog._validate_etag(mock_table)

    def test_commit_conflict_error(self, catalog, mock_bigquery_client):
        """Test commit conflict error handling."""
        mock_bq_table = Mock()
        mock_external_config = Mock()
        mock_external_config.metadata = json.dumps({
            "table_type": "iceberg",
            "metadata_location": "gs://bucket/metadata.json",
        })
        mock_bq_table.external_data_configuration = mock_external_config
        mock_bq_table.etag = "test-etag"
        mock_bigquery_client.get_table.return_value = mock_bq_table

        # Mock update failure
        mock_bigquery_client.update_table.side_effect = PreconditionFailed("Etag mismatch")

        mock_table = Mock()
        mock_table.metadata_location = "gs://bucket/warehouse/test_dataset.db/test_table/metadata/00001-abc.metadata.json"

        catalog.file_io.new_input.return_value = Mock()
        catalog.file_io.new_output.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile"), \
                patch("pyiceberg_bigquery_catalog.catalog.ToOutputFile"), \
                patch("pyiceberg_bigquery_catalog.catalog.update_table_metadata"):
            with pytest.raises(CommitFailedException, match="etag mismatch"):
                catalog.commit_table(
                    table=mock_table,
                    requirements=(),
                    updates=()
                )

            # Verify metadata file was cleaned up
            catalog.file_io.delete.assert_called()