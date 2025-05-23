# tests/test_catalog.py
"""Tests for BigQuery catalog implementation."""

import json
import uuid
from unittest.mock import Mock, patch

import pytest
from google.api_core.exceptions import Conflict, NotFound, PreconditionFailed
from pyiceberg.catalog import PropertiesUpdateSummary
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
    ValidationError,
)
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.types import LongType, NestedField, StringType

from pyiceberg_bigquery_catalog import BigQueryCatalog


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    with patch("pyiceberg_bigquery_catalog.catalog.bigquery.Client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def bq_catalog(mock_bigquery_client):
    """Create a BigQueryCatalog instance with a mocked client."""
    # Mock the entire _load_file_io method on the BigQueryCatalog class
    with patch.object(BigQueryCatalog, "_load_file_io") as mock_load_file_io:
        # Create a mock FileIO instance
        mock_file_io = Mock()
        mock_load_file_io.return_value = mock_file_io

        catalog = BigQueryCatalog(
            name="test",
            project_id="test-project",
            dataset_id="test_dataset",
            gcp_location="us-central1",
            warehouse="gs://test-bucket/warehouse",
        )

        # Set up the FileIO mock directly
        catalog._file_io = mock_file_io

        return catalog


class TestBigQueryCatalogInitialization:
    """Test catalog initialization."""

    def test_missing_project_id(self):
        """Test that catalog raises error when project_id is missing."""
        with pytest.raises(NoSuchPropertyException, match="Property 'project_id' is required"):
            BigQueryCatalog(
                name="test",
                dataset_id="test_dataset",
                warehouse="gs://test-bucket/warehouse",
            )

    def test_missing_dataset_id(self):
        """Test that catalog raises error when dataset_id is missing."""
        with pytest.raises(NoSuchPropertyException, match="Property 'dataset_id' is required"):
            BigQueryCatalog(
                name="test",
                project_id="test-project",
                warehouse="gs://test-bucket/warehouse",
            )

    def test_successful_initialization(self, mock_bigquery_client):
        """Test successful catalog initialization."""
        mock_dataset = Mock()
        mock_bigquery_client.get_dataset.return_value = mock_dataset

        with patch.object(BigQueryCatalog, "_load_file_io") as mock_load_file_io:
            mock_file_io = Mock()
            mock_load_file_io.return_value = mock_file_io

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

        with patch.object(BigQueryCatalog, "_load_file_io") as mock_load_file_io:
            mock_file_io = Mock()
            mock_load_file_io.return_value = mock_file_io

            BigQueryCatalog(
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

    def test_create_table(self, bq_catalog, mock_bigquery_client):
        """Test creating a new table."""
        # Mock file operations - create a proper mock for OutputFile
        mock_output_file = Mock()
        mock_output_stream = Mock()
        mock_output_file.create.return_value.__enter__ = Mock(return_value=mock_output_stream)
        mock_output_file.create.return_value.__exit__ = Mock(return_value=None)
        bq_catalog.file_io.new_output.return_value = mock_output_file

        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=True),
        )

        # Table doesn't exist yet
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        table = bq_catalog.create_table(
            identifier="test_table",
            schema=schema,
            properties={"format-version": "2"},
        )

        assert isinstance(table, Table)
        # Use name() method instead of identifier attribute
        assert table.name() == ("test", "test_dataset", "test_table")

        # Verify metadata was written
        bq_catalog.file_io.new_output.assert_called_once()

    def test_create_table_already_exists(self, bq_catalog, mock_bigquery_client):
        """Test error when creating a table that already exists."""
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
        )

        # Mock existing table - Fix the external_data_configuration
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.source_format = "ICEBERG"
        mock_external_config.source_uris = ["gs://bucket/metadata.json"]
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        # Mock metadata loading
        bq_catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input:
            mock_metadata = Mock()
            mock_from_input.table_metadata.return_value = mock_metadata

            with pytest.raises(TableAlreadyExistsError):
                bq_catalog.create_table(identifier="test_table", schema=schema)

    def test_load_table(self, bq_catalog, mock_bigquery_client):
        """Test loading an existing table."""
        # Mock BigQuery table with Iceberg metadata
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.source_format = "ICEBERG"
        mock_external_config.source_uris = ["gs://bucket/metadata.json"]
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        # Mock metadata file
        bq_catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input:
            mock_metadata = Mock()
            mock_from_input.table_metadata.return_value = mock_metadata

            table = bq_catalog.load_table("test_table")

            assert isinstance(table, Table)
            # Use name() method instead of identifier attribute
            assert table.name() == ("test", "test_dataset", "test_table")

    def test_load_nonexistent_table(self, bq_catalog, mock_bigquery_client):
        """Test error when loading a table that doesn't exist."""
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        with pytest.raises(NoSuchTableError):
            bq_catalog.load_table("nonexistent_table")

    def test_drop_table(self, bq_catalog, mock_bigquery_client):
        """Test dropping a table."""
        # Mock existing table
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.source_format = "ICEBERG"
        mock_external_config.source_uris = ["gs://bucket/metadata.json"]
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        bq_catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile"):
            bq_catalog.drop_table("test_table")

        mock_bigquery_client.delete_table.assert_called_once()

    def test_rename_table_not_supported(self, bq_catalog):
        """Test that rename_table raises ValidationError."""
        with pytest.raises(ValidationError, match="rename operation is not supported"):
            bq_catalog.rename_table("old_table", "new_table")

    def test_table_exists(self, bq_catalog, mock_bigquery_client):
        """Test checking if a table exists."""
        # Mock existing table
        mock_table = Mock()
        mock_external_config = Mock()
        mock_external_config.source_format = "ICEBERG"
        mock_external_config.source_uris = ["gs://bucket/metadata.json"]
        mock_table.external_data_configuration = mock_external_config
        mock_bigquery_client.get_table.return_value = mock_table

        bq_catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile"):
            assert bq_catalog.table_exists("test_table") is True

        # Test non-existent table
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")
        assert bq_catalog.table_exists("nonexistent_table") is False

    def test_commit_table(self, bq_catalog, mock_bigquery_client):
        """Test committing table changes."""
        # Mock existing table
        mock_bq_table = Mock()
        mock_external_config = Mock()
        mock_external_config.source_format = "ICEBERG"
        mock_external_config.source_uris = ["gs://bucket/metadata.json"]

        # Set up the description with iceberg metadata
        mock_bq_table.external_data_configuration = mock_external_config
        mock_bq_table.etag = "test-etag"
        mock_bq_table.description = json.dumps(
            {
                "iceberg_metadata": {
                    "table_type": "iceberg",
                    "metadata_location": "gs://bucket/metadata.json",
                }
            }
        )
        mock_bigquery_client.get_table.return_value = mock_bq_table

        # Mock table and metadata
        mock_table = Mock()
        mock_table.metadata_location = (
            "gs://bucket/warehouse/test_dataset.db/test_table/metadata/00001-abc.metadata.json"
        )

        # Create mock metadata that looks like a real metadata dict
        test_uuid = uuid.uuid4()
        mock_metadata_dict = {
            "format-version": 2,
            "table-uuid": str(test_uuid),
            "location": "gs://bucket/warehouse/test_dataset.db/test_table",
            "properties": {},
            "current-snapshot-id": -1,
            "schemas": [{"schema-id": 0, "fields": []}],
            "current-schema-id": 0,
            "last-sequence-number": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
        }

        mock_metadata = Mock()
        mock_metadata.table_uuid = test_uuid
        mock_metadata.location = "gs://bucket/warehouse/test_dataset.db/test_table"
        mock_metadata.properties = {}
        mock_metadata.current_snapshot.return_value = None
        mock_metadata.current_snapshot_id = None
        mock_metadata.model_dump.return_value = mock_metadata_dict
        mock_table.metadata = mock_metadata

        # Mock file I/O for metadata writing
        mock_output_file = Mock()
        mock_output_stream = Mock()
        mock_output_file.create.return_value.__enter__ = Mock(return_value=mock_output_stream)
        mock_output_file.create.return_value.__exit__ = Mock(return_value=None)
        bq_catalog.file_io.new_output.return_value = mock_output_file
        bq_catalog.file_io.new_input.return_value = Mock()

        # Create a simple update
        mock_update_obj = Mock()
        mock_update_obj.validate.return_value = None

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input, patch(
            "pyiceberg_bigquery_catalog.catalog.ToOutputFile"
        ) as mock_to_output, patch(
            "pyiceberg.table.update.update_table_metadata"
        ) as mock_update, patch.object(
            bq_catalog, "_parse_metadata_version"
        ) as mock_parse_version:
            # Mock the loaded metadata
            mock_from_input.table_metadata.return_value = mock_metadata

            # Mock the version parsing
            mock_parse_version.return_value = 1

            # Mock the updated metadata with proper structure
            mock_updated_metadata_dict = mock_metadata_dict.copy()
            mock_updated_metadata_dict["last-sequence-number"] = 1

            # Create a proper TableMetadata mock that can be serialized
            from pyiceberg.table.metadata import TableMetadataV2

            updated_metadata = Mock(spec=TableMetadataV2)
            updated_metadata.location = "gs://bucket/warehouse/test_dataset.db/test_table"
            updated_metadata.table_uuid = test_uuid
            updated_metadata.properties = {}
            updated_metadata.current_snapshot.return_value = None
            updated_metadata.current_snapshot_id = None  # Add missing attribute
            updated_metadata.format_version = 2
            updated_metadata.model_dump.return_value = mock_updated_metadata_dict
            updated_metadata.__class__ = TableMetadataV2
            # Add required attributes for validation
            updated_metadata.current_schema_id = 0
            updated_metadata.schemas = [Mock(schema_id=0, fields=[])]
            updated_metadata.last_column_id = 0
            # Create proper partition spec mock
            mock_partition_spec = Mock()
            mock_partition_spec.spec_id = 0
            mock_partition_spec.fields = []
            updated_metadata.partition_specs = [mock_partition_spec]
            updated_metadata.default_spec_id = 0
            # Create proper sort order mock
            mock_sort_order = Mock()
            mock_sort_order.order_id = 0
            mock_sort_order.fields = []
            updated_metadata.sort_orders = [mock_sort_order]
            updated_metadata.default_sort_order_id = 0

            mock_update.return_value = updated_metadata

            # Mock ToOutputFile to avoid validation issues
            mock_to_output.table_metadata = Mock()

            response = bq_catalog.commit_table(
                table=mock_table, requirements=(), updates=(mock_update_obj,)
            )

            assert isinstance(response, CommitTableResponse)
            assert response.metadata == updated_metadata


class TestNamespaceOperations:
    """Test namespace operations."""

    def test_create_namespace(self, bq_catalog, mock_bigquery_client):
        """Test creating a namespace."""
        bq_catalog.create_namespace("new_dataset", {"location": "us-central1"})

        mock_bigquery_client.create_dataset.assert_called_once()
        created_dataset = mock_bigquery_client.create_dataset.call_args[0][0]
        assert created_dataset.dataset_id == "new_dataset"
        assert created_dataset.location == "us-central1"

    def test_create_namespace_already_exists(self, bq_catalog, mock_bigquery_client):
        """Test error when creating a namespace that already exists."""
        mock_bigquery_client.create_dataset.side_effect = Conflict("Dataset already exists")

        with pytest.raises(NamespaceAlreadyExistsError):
            bq_catalog.create_namespace("existing_dataset")

    def test_drop_namespace(self, bq_catalog, mock_bigquery_client):
        """Test dropping a namespace."""
        bq_catalog.drop_namespace("test_dataset")
        mock_bigquery_client.delete_dataset.assert_called_once()

    def test_drop_nonexistent_namespace(self, bq_catalog, mock_bigquery_client):
        """Test error when dropping a namespace that doesn't exist."""
        mock_bigquery_client.delete_dataset.side_effect = NotFound("Dataset not found")

        with pytest.raises(NoSuchNamespaceError):
            bq_catalog.drop_namespace("nonexistent_dataset")

    def test_list_namespaces(self, bq_catalog, mock_bigquery_client):
        """Test listing namespaces."""
        mock_dataset1 = Mock()
        mock_dataset1.dataset_id = "dataset1"
        mock_dataset2 = Mock()
        mock_dataset2.dataset_id = "dataset2"

        mock_bigquery_client.list_datasets.return_value = [mock_dataset1, mock_dataset2]

        namespaces = bq_catalog.list_namespaces()
        assert ("dataset1",) in namespaces
        assert ("dataset2",) in namespaces

    def test_load_namespace_properties(self, bq_catalog, mock_bigquery_client):
        """Test loading namespace properties."""
        mock_dataset = Mock()
        mock_dataset.labels = {
            "env": "production",
            "team": "data",
        }
        mock_dataset.location = "us-central1"
        mock_bigquery_client.get_dataset.return_value = mock_dataset

        properties = bq_catalog.load_namespace_properties("test_dataset")
        assert properties["env"] == "production"
        assert properties["team"] == "data"
        assert properties["gcp_location"] == "us-central1"

    def test_update_namespace_properties(self, bq_catalog, mock_bigquery_client):
        """Test updating namespace properties."""
        mock_dataset = Mock()
        mock_dataset.labels = {
            "env": "production",
            "team": "data",
        }
        mock_bigquery_client.get_dataset.return_value = mock_dataset

        summary = bq_catalog.update_namespace_properties(
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

    def test_list_tables(self, bq_catalog, mock_bigquery_client):
        """Test listing tables in a namespace."""
        mock_table1 = Mock()
        mock_table1.table_id = "table1"
        mock_table2 = Mock()
        mock_table2.table_id = "table2"

        mock_bigquery_client.list_tables.return_value = [mock_table1, mock_table2]

        tables = bq_catalog.list_tables("test_dataset")
        assert ("test_dataset", "table1") in tables
        assert ("test_dataset", "table2") in tables

    def test_list_tables_with_filter(self, bq_catalog, mock_bigquery_client):
        """Test listing tables with filter_unsupported_tables enabled."""
        bq_catalog.filter_unsupported_tables = True

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
        mock_iceberg_config.source_format = "ICEBERG"
        mock_iceberg_config.source_uris = ["gs://bucket/metadata.json"]
        mock_iceberg_table.external_data_configuration = mock_iceberg_config

        mock_regular_table = Mock()
        mock_regular_table.external_data_configuration = None

        mock_bigquery_client.get_table.side_effect = [
            mock_iceberg_table,  # First table is Iceberg
            mock_regular_table,  # Second table is regular
        ]

        tables = bq_catalog.list_tables("test_dataset")
        assert ("test_dataset", "iceberg_table") in tables
        assert ("test_dataset", "regular_table") not in tables


class TestRegisterTable:
    """Test register table functionality."""

    def test_register_table(self, bq_catalog, mock_bigquery_client):
        """Test registering an existing Iceberg table."""
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        bq_catalog.file_io.new_input.return_value = Mock()

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input:
            mock_metadata = Mock()
            mock_metadata.location = "gs://bucket/table"
            mock_metadata.table_uuid = uuid.uuid4()
            mock_metadata.properties = {}
            mock_metadata.current_snapshot.return_value = None
            mock_from_input.table_metadata.return_value = mock_metadata

            table = bq_catalog.register_table(
                identifier="new_table",
                metadata_location="gs://bucket/metadata.json",
            )

            assert isinstance(table, Table)
            mock_bigquery_client.create_table.assert_called_once()


class TestHelperMethods:
    """Test helper methods."""

    def test_sanitize_label_key(self, bq_catalog):
        """Test label key sanitization."""
        assert bq_catalog._sanitize_label_key("valid_key") == "valid_key"
        assert bq_catalog._sanitize_label_key("Invalid-Key") == "invalid-key"
        assert bq_catalog._sanitize_label_key("123key") == "l_123key"
        assert bq_catalog._sanitize_label_key("") == ""

        # Test truncation
        long_key = "a" * 100
        assert len(bq_catalog._sanitize_label_key(long_key)) == 63

    def test_sanitize_label_value(self, bq_catalog):
        """Test label value sanitization."""
        assert bq_catalog._sanitize_label_value("valid-value") == "valid-value"
        assert bq_catalog._sanitize_label_value("Invalid Value!") == "invalid_value_"
        assert bq_catalog._sanitize_label_value("") == ""

    def test_validate_identifier(self, bq_catalog):
        """Test identifier validation."""
        # Valid identifiers
        assert bq_catalog._validate_identifier("table_name") == "table_name"
        assert bq_catalog._validate_identifier("test_dataset.table_name") == "table_name"
        assert bq_catalog._validate_identifier(("test_dataset", "table_name")) == "table_name"

        # Invalid identifiers
        with pytest.raises(ValidationError):
            bq_catalog._validate_identifier("wrong_dataset.table_name")

        with pytest.raises(ValidationError):
            bq_catalog._validate_identifier("project.dataset.table")

    def test_resolve_table_location(self, bq_catalog):
        """Test table location resolution."""
        # With explicit location
        location = bq_catalog._resolve_table_location("gs://custom/location", "dataset", "table")
        assert location == "gs://custom/location"

        # Without location, use warehouse
        location = bq_catalog._resolve_table_location(None, "dataset", "table")
        assert location == "gs://test-bucket/warehouse/dataset.db/table"

        # Without location or warehouse
        bq_catalog.warehouse_location = None
        with pytest.raises(ValueError, match="No default path is set"):
            bq_catalog._resolve_table_location(None, "dataset", "table")

    def test_get_metadata_location(self, bq_catalog):
        """Test metadata location generation."""
        location = bq_catalog._get_metadata_location("gs://bucket/table", 0)
        assert location.startswith("gs://bucket/table/metadata/00000-")
        assert location.endswith(".metadata.json")

        location = bq_catalog._get_metadata_location("gs://bucket/table", 5)
        assert location.startswith("gs://bucket/table/metadata/00005-")

        with pytest.raises(ValueError):
            bq_catalog._get_metadata_location("gs://bucket/table", -1)


class TestErrorScenarios:
    """Test various error scenarios."""

    def test_etag_validation_error(self, bq_catalog, mock_bigquery_client):
        """Test error when table etag is empty."""
        mock_table = Mock()
        mock_table.etag = ""
        mock_table.table_id = "test_table"

        with pytest.raises(ValidationError, match="Etag of table test_table is empty"):
            bq_catalog._validate_etag(mock_table)

    def test_commit_conflict_error(self, bq_catalog, mock_bigquery_client):
        """Test commit conflict error handling."""
        mock_bq_table = Mock()
        mock_external_config = Mock()
        mock_external_config.source_format = "ICEBERG"
        mock_external_config.source_uris = ["gs://bucket/metadata.json"]
        mock_bq_table.external_data_configuration = mock_external_config
        mock_bq_table.etag = "test-etag"
        mock_bq_table.description = json.dumps(
            {
                "iceberg_metadata": {
                    "table_type": "iceberg",
                    "metadata_location": "gs://bucket/metadata.json",
                }
            }
        )
        mock_bigquery_client.get_table.return_value = mock_bq_table

        # Mock update failure
        mock_bigquery_client.update_table.side_effect = PreconditionFailed("Etag mismatch")

        mock_table = Mock()
        mock_table.metadata_location = (
            "gs://bucket/warehouse/test_dataset.db/test_table/metadata/00001-abc.metadata.json"
        )
        mock_metadata = Mock()
        mock_metadata.table_uuid = uuid.uuid4()
        mock_metadata.location = "gs://bucket/warehouse/test_dataset.db/test_table"
        mock_metadata.properties = {}
        mock_metadata.current_snapshot.return_value = None
        mock_metadata.schemas = {}
        mock_metadata.current_schema_id = 1
        mock_metadata.last_sequence_number = 0
        mock_table.metadata = mock_metadata

        bq_catalog.file_io.new_input.return_value = Mock()
        bq_catalog.file_io.new_output.return_value = Mock()

        # Create a mock update object
        mock_update_obj = Mock()
        mock_update_obj.validate.return_value = None

        with patch("pyiceberg_bigquery_catalog.catalog.FromInputFile") as mock_from_input, patch(
            "pyiceberg_bigquery_catalog.catalog.ToOutputFile"
        ), patch("pyiceberg.table.update.update_table_metadata") as mock_update:
            mock_from_input.table_metadata.return_value = mock_metadata
            mock_update.return_value = mock_metadata

            with pytest.raises(CommitFailedException, match="etag mismatch"):
                bq_catalog.commit_table(
                    table=mock_table, requirements=(), updates=(mock_update_obj,)
                )

            # Verify metadata file was cleaned up
            bq_catalog.file_io.delete.assert_called()
