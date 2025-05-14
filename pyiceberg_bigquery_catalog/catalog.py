"""BigQuery catalog implementation for PyIceberg."""

from __future__ import annotations

import json
import logging
import re
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import pyarrow as pa

from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound
from pyiceberg.catalog import MetastoreCatalog, PropertiesUpdateSummary
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
    ValidationError,
)
from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestFile
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import TableRequirement, TableUpdate
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties

# Set up module-level logger
logger = logging.getLogger(__name__)


class BigQueryCatalog(MetastoreCatalog):
    """
    BigQuery catalog implementation for Apache Iceberg.

    This catalog uses BigQuery's external table features to store Iceberg metadata.
    """

    # Configuration keys
    PROPERTIES_KEY_PROJECT_ID = "project_id"
    PROPERTIES_KEY_DATASET_ID = "dataset_id"
    PROPERTIES_KEY_GCP_LOCATION = "gcp_location"
    PROPERTIES_KEY_FILTER_UNSUPPORTED_TABLES = "filter_unsupported_tables"
    PROPERTIES_KEY_BQ_CONNECTION = "bq_connection"

    # External table metadata keys
    EXTERNAL_TABLE_TYPE_KEY = "table_type"
    EXTERNAL_TABLE_TYPE_VALUE = "iceberg"
    EXTERNAL_METADATA_LOCATION_KEY = "metadata_location"
    EXTERNAL_PREVIOUS_METADATA_LOCATION_KEY = "previous_metadata_location"
    EXTERNAL_LOCATION_KEY = "location"
    EXTERNAL_UUID_KEY = "uuid"
    EXTERNAL_KEY = "EXTERNAL"

    # Type annotations
    _file_io: FileIO | None

    def __init__(self, name: str, **properties: Any):
        """Initialize the BigQuery catalog."""
        super().__init__(name, **properties)

        # Initialize type hint for _file_io
        self._file_io = None

        # Required properties
        self.project_id = properties.get(self.PROPERTIES_KEY_PROJECT_ID)
        if not self.project_id:
            raise NoSuchPropertyException(
                f"Property '{self.PROPERTIES_KEY_PROJECT_ID}' is required for BigQuery catalog"
            )

        self.dataset_id = properties.get(self.PROPERTIES_KEY_DATASET_ID)
        if not self.dataset_id:
            raise NoSuchPropertyException(
                f"Property '{self.PROPERTIES_KEY_DATASET_ID}' is required for BigQuery catalog"
            )

        # Optional properties
        self.gcp_location = properties.get(self.PROPERTIES_KEY_GCP_LOCATION, "us")
        self.filter_unsupported_tables = (
            str(properties.get(self.PROPERTIES_KEY_FILTER_UNSUPPORTED_TABLES, "false")).lower()
            == "true"
        )
        self.warehouse_location = properties.get("warehouse")

        # Initialize BigQuery client
        assert self.project_id is not None  # For mypy
        self.client = bigquery.Client(project=self.project_id)

        # Ensure dataset exists
        self._ensure_dataset_exists()

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """Create a table - following Java's delayed BigQuery table creation pattern."""
        schema_to_use: Schema
        if not isinstance(schema, Schema):
            # Convert PyArrow schema if needed
            schema_to_use = self._convert_schema_if_needed(schema)
        else:
            schema_to_use = schema

        table_name = self._validate_identifier(identifier)
        table_identifier = self._full_identifier(table_name)

        # Check if table already exists
        if self.table_exists(identifier):
            raise TableAlreadyExistsError(f"Table already exists: {'.'.join(table_identifier)}")

        # Determine table location - fix dataset argument type
        dataset_id = self.dataset_id
        if dataset_id is None:
            raise ValueError("dataset_id is required")
        location = self._resolve_table_location(location, dataset_id, table_name)

        # Create table metadata
        metadata = new_table_metadata(
            location=location,
            schema=schema_to_use,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )

        # Write metadata file
        metadata_location = self._get_metadata_location(location, 0)
        ToOutputFile.table_metadata(metadata, self.file_io.new_output(metadata_location))

        # Note: Not creating BigQuery external table yet
        logger.info(f"Created Iceberg metadata for table {table_name}")
        logger.info("BigQuery external table will be created after first data write")

        return self._create_table_instance(
            identifier=table_identifier,
            metadata=metadata,
            metadata_location=metadata_location,
        )

    def load_table(self, identifier: str | Identifier) -> Table:
        """Load a table from BigQuery."""
        table_name = self._validate_identifier(identifier)
        table_identifier = self._full_identifier(table_name)

        # Get BigQuery table
        assert self.project_id is not None and self.dataset_id is not None  # For mypy
        bq_table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id), table_name
        )

        try:
            bq_table = self.client.get_table(bq_table_ref)
        except NotFound:
            raise NoSuchTableError(f"Table does not exist: {'.'.join(table_identifier)}")

        # Validate it's an Iceberg table
        self._validate_table(bq_table)

        # Get metadata location
        metadata_location = self._get_metadata_location_or_throw(bq_table)

        # Load metadata
        metadata_file = self.file_io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(metadata_file)

        return self._create_table_instance(
            identifier=table_identifier,
            metadata=metadata,
            metadata_location=metadata_location,
        )

    def table_exists(self, identifier: str | Identifier) -> bool:
        """Check if a table exists."""
        try:
            self.load_table(identifier)
            return True
        except NoSuchTableError:
            return False

    def drop_table(self, identifier: str | Identifier) -> None:
        """Drop a table from BigQuery."""
        table_name = self._validate_identifier(identifier)

        # First verify the table exists and is a valid Iceberg table
        try:
            self.load_table(identifier)
        except NoSuchTableError:
            return  # Table doesn't exist, nothing to drop

        # Drop the BigQuery table
        assert self.project_id is not None and self.dataset_id is not None  # For mypy
        bq_table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id), table_name
        )

        with suppress(NotFound):
            self.client.delete_table(bq_table_ref)

    def purge_table(self, identifier: str | Identifier) -> None:
        """Drop a table and purge all data and metadata files."""
        identifier_tuple = self.identifier_to_tuple(identifier)
        table = self.load_table(identifier_tuple)

        # Drop the table first
        self.drop_table(identifier_tuple)

        # Then purge all files
        io = self.file_io
        metadata = table.metadata

        # Check if metadata has snapshots attribute
        snapshots = getattr(metadata, "snapshots", None)
        if not snapshots:
            return

        # Delete data files
        manifest_lists_to_delete = set()
        manifests_to_delete: list[ManifestFile] = []

        for snapshot in snapshots:
            manifest_list = snapshot.manifest_list
            if manifest_list:
                manifest_lists_to_delete.add(manifest_list)
                # Load and process manifests
                for manifest_file in snapshot.manifests(io):
                    manifests_to_delete.append(manifest_file)

        # Delete files
        self._delete_data_files(io, manifests_to_delete)
        self._delete_files(io, {m.manifest_path for m in manifests_to_delete}, "manifest")
        self._delete_files(io, manifest_lists_to_delete, "manifest list")
        self._delete_files(io, {table.metadata_location}, "metadata")

    def rename_table(
        self, from_identifier: str | Identifier, to_identifier: str | Identifier
    ) -> Table:
        """Rename a table - not supported in BigQuery."""
        raise ValidationError("Table rename operation is not supported in BigQuery catalog")

    def create_namespace(
        self, namespace: str | Identifier, properties: Properties = EMPTY_DICT
    ) -> None:
        """Create a namespace (dataset) in BigQuery."""
        database_name = self.identifier_to_database(namespace)

        assert self.project_id is not None  # For mypy
        dataset_ref = bigquery.DatasetReference(self.project_id, database_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.gcp_location

        # Handle properties
        dataset.labels = {}
        for key, value in properties.items():
            if key == "location":
                dataset.labels["default_storage_location"] = self._sanitize_label_value(value)
            else:
                label_key = self._sanitize_label_key(key)
                if label_key:
                    dataset.labels[label_key] = self._sanitize_label_value(str(value))

        try:
            self.client.create_dataset(dataset)
        except Conflict:
            raise NamespaceAlreadyExistsError(f"Namespace already exists: {database_name}")

    def drop_namespace(self, namespace: str | Identifier) -> None:
        """Drop a namespace (dataset) from BigQuery."""
        database_name = self.identifier_to_database(namespace)
        assert self.project_id is not None  # For mypy
        dataset_ref = bigquery.DatasetReference(self.project_id, database_name)

        try:
            self.client.delete_dataset(dataset_ref)
        except NotFound:
            raise NoSuchNamespaceError(f"Namespace does not exist: {database_name}")

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        """List tables in the given namespace."""
        database_name = self.identifier_to_database(namespace)

        try:
            tables: list[Identifier] = []
            assert self.project_id is not None  # For mypy
            for table in self.client.list_tables(f"{self.project_id}.{database_name}"):
                if self.filter_unsupported_tables:
                    try:
                        bq_table = self.client.get_table(table.reference)
                        if self._is_valid_iceberg_table(bq_table):
                            tables.append((database_name, table.table_id))
                    except Exception as e:
                        logger.warning(f"Failed list_tables due to {e}")
                else:
                    tables.append((database_name, table.table_id))
            return tables
        except NotFound:
            raise NoSuchNamespaceError(f"Namespace does not exist: {database_name}")

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        """List namespaces - BigQuery only supports single-level namespaces."""
        # Validate namespace parameter
        if namespace and len(self.identifier_to_tuple(namespace)) > 0:
            return []  # BigQuery doesn't support hierarchical namespaces

        datasets: list[Identifier] = []
        assert self.project_id is not None  # For mypy
        for dataset in self.client.list_datasets(self.project_id):
            datasets.append((dataset.dataset_id,))
        return datasets

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        """Load properties for a namespace."""
        database_name = self.identifier_to_database(namespace)
        assert self.project_id is not None  # For mypy
        dataset_ref = bigquery.DatasetReference(self.project_id, database_name)

        try:
            dataset = self.client.get_dataset(dataset_ref)
            properties = {}

            # Get properties from labels
            if dataset.labels:
                for label_key, label_value in dataset.labels.items():
                    property_key = self._label_to_property_key(label_key)
                    properties[property_key] = label_value

            # Add location property (make sure it's a string)
            if hasattr(dataset, "location") and dataset.location:
                properties["gcp_location"] = str(dataset.location)

            return properties
        except NotFound:
            raise NoSuchNamespaceError(f"Namespace does not exist: {database_name}")

    def update_namespace_properties(
        self,
        namespace: str | Identifier,
        removals: set[str] | None = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        """Update properties for a namespace."""
        database_name = self.identifier_to_database(namespace)
        assert self.project_id is not None  # For mypy
        dataset_ref = bigquery.DatasetReference(self.project_id, database_name)

        try:
            dataset = self.client.get_dataset(dataset_ref)
        except NotFound:
            raise NoSuchNamespaceError(f"Namespace does not exist: {database_name}")

        # Current labels - create a new dict instance
        current_labels = dict(dataset.labels) if dataset.labels else {}

        # Track changes
        removed = []
        updated = []

        # Apply removals
        if removals:
            for key in removals:
                label_key = self._property_to_label_key(key)
                if label_key in current_labels:
                    del current_labels[label_key]
                    removed.append(key)

        # Apply updates
        if updates:
            for key, value in updates.items():
                label_key = self._property_to_label_key(key)
                current_labels[label_key] = self._sanitize_label_value(str(value))
                updated.append(key)

        # Update dataset with new labels
        # Create a new dataset object to avoid mutation issues
        updated_dataset = bigquery.Dataset(dataset_ref)
        updated_dataset.labels = current_labels
        self.client.update_dataset(updated_dataset, ["labels"])

        return PropertiesUpdateSummary(removed=removed, updated=updated, missing=[])

    def register_table(self, identifier: str | Identifier, metadata_location: str) -> Table:
        """Register an existing Iceberg table in BigQuery."""
        table_name = self._validate_identifier(identifier)
        table_identifier = self._full_identifier(table_name)

        # Check if table already exists
        if self.table_exists(identifier):
            raise TableAlreadyExistsError(f"Table already exists: {'.'.join(table_identifier)}")

        # Load metadata from the provided location
        metadata_file = self.file_io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(metadata_file)

        # Create BigQuery external table
        assert self.project_id is not None and self.dataset_id is not None  # For mypy
        bq_table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id), table_name
        )
        bq_table = self._create_bigquery_external_table(
            table_ref=bq_table_ref,
            metadata=metadata,
            metadata_location=metadata_location,
        )

        try:
            self.client.create_table(bq_table)
        except Conflict:
            raise TableAlreadyExistsError(f"Table already exists: {'.'.join(table_identifier)}")

        return self._create_table_instance(
            identifier=table_identifier,
            metadata=metadata,
            metadata_location=metadata_location,
        )

    def commit_table(self, table: Table, requirements: Any, updates: Any) -> CommitTableResponse:
        """Commit table changes."""
        # Cast to proper types for internal use
        req_tuple: tuple[TableRequirement, ...] = requirements
        update_tuple: tuple[TableUpdate, ...] = updates
        from pyiceberg.table.update import AssertCreate, update_table_metadata

        # Extract table name from the table's metadata location
        metadata_location = table.metadata_location
        parts = metadata_location.split("/")

        # Find the table name (comes after dataset.db)
        table_name = None
        for i in range(len(parts) - 1):
            if parts[i].endswith(".db"):
                table_name = parts[i + 1]
                break

        if not table_name:
            # Fallback: use the last directory before "metadata"
            for i in range(len(parts) - 1, -1, -1):
                if parts[i] == "metadata" and i > 0:
                    table_name = parts[i - 1]
                    break

        if not table_name:
            raise ValueError(
                f"Could not extract table name from metadata location: {metadata_location}"
            )

        # Load current state
        current_metadata = None
        bq_table_exists = False

        assert self.project_id is not None and self.dataset_id is not None  # For mypy
        bq_table_ref = bigquery.TableReference(
            bigquery.DatasetReference(self.project_id, self.dataset_id), table_name
        )

        try:
            bq_table = self.client.get_table(bq_table_ref)
            bq_table_exists = True
            # If BigQuery table exists, load its metadata
            metadata_location_from_bq = self._get_metadata_location_or_throw(bq_table)
            metadata_file = self.file_io.new_input(metadata_location_from_bq)
            current_metadata = FromInputFile.table_metadata(metadata_file)
        except NotFound:
            # Table doesn't exist in BigQuery yet
            bq_table_exists = False
            # Check if this is an initial create
            has_assert_create = any(isinstance(req, AssertCreate) for req in req_tuple)
            if has_assert_create:
                # This is the initial creation, no current metadata expected
                current_metadata = None
            else:
                # This is an update, use the table's metadata
                current_metadata = table.metadata

        # Validate requirements
        for requirement in req_tuple:
            requirement.validate(current_metadata)

        # Update metadata
        base_metadata = current_metadata if current_metadata else table.metadata

        updated_metadata = update_table_metadata(
            base_metadata=base_metadata,
            updates=update_tuple,
            enforce_validation=False,  # We already validated requirements
        )

        # Get new metadata location
        current_version = self._parse_metadata_version(table.metadata_location)
        new_metadata_version = current_version + 1

        # Use getattr to safely access location attribute
        meta_obj = cast(Any, updated_metadata)  # Cast to Any to appease mypy
        metadata_loc_str = getattr(meta_obj, "location", None)
        if metadata_loc_str is None:
            raise ValueError("Updated metadata has no location attribute")
        metadata_location = str(metadata_loc_str)

        new_metadata_location = self._get_metadata_location(metadata_location, new_metadata_version)

        # Write new metadata
        ToOutputFile.table_metadata(
            updated_metadata, self.file_io.new_output(new_metadata_location)
        )

        # Create or update BigQuery table if needed
        if not bq_table_exists:
            # Check if metadata has current_snapshot method and data
            current_snapshot = None
            metadata_typed = cast(TableMetadata, updated_metadata)
            if hasattr(metadata_typed, "current_snapshot") and callable(
                metadata_typed.current_snapshot
            ):
                with suppress(Exception):
                    current_snapshot = metadata_typed.current_snapshot()

            has_snapshot = current_snapshot is not None
            if has_snapshot:
                # Create BigQuery table now that we have data
                bq_table = self._create_bigquery_external_table(
                    table_ref=bq_table_ref,
                    metadata=updated_metadata,
                    metadata_location=new_metadata_location,
                )

                try:
                    self.client.create_table(bq_table)
                except Exception as e:
                    # Rollback metadata write
                    self.file_io.delete(new_metadata_location)
                    raise CommitFailedException(f"Failed to create BigQuery table: {str(e)}")

        elif bq_table_exists:
            # Update existing table
            try:
                bq_table = self.client.get_table(bq_table_ref)

                # Validate etag
                self._validate_etag(bq_table)

                # Update metadata
                self._update_bigquery_table_metadata(
                    bq_table,
                    updated_metadata,
                    new_metadata_location,
                )

                # Apply update
                self.client.update_table(bq_table, ["external_data_configuration"])

            except PreconditionFailed:
                self.file_io.delete(new_metadata_location)
                raise CommitFailedException(
                    "Updating table failed due to conflict updates (etag mismatch). "
                    "Retry the update"
                )
            except Exception as e:
                self.file_io.delete(new_metadata_location)
                raise CommitFailedException(str(e))

        return CommitTableResponse(
            metadata=updated_metadata, **{"metadata-location": new_metadata_location}
        )

    # View operations - not supported in BigQuery
    def list_views(self, namespace: str | Identifier = ()) -> list[Identifier]:
        """List views - not supported."""
        return []

    def drop_view(self, identifier: str | Identifier) -> None:
        """Drop view - not supported."""
        raise NoSuchTableError("Views are not supported")

    def view_exists(self, identifier: str | Identifier) -> bool:
        """Check if view exists - not supported."""
        return False

    # Helper methods
    @property
    def file_io(self) -> FileIO:
        """Get or create FileIO instance."""
        if self._file_io is None:
            file_io_prop: Properties = self.properties if self.properties else {}
            self._file_io = self._load_file_io(file_io_prop, location=None)
        return self._file_io

    def _ensure_dataset_exists(self) -> None:
        """Ensure the configured dataset exists in BigQuery."""
        assert self.project_id is not None and self.dataset_id is not None  # For mypy
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)

        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            # Create the dataset if it doesn't exist
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.gcp_location

            # Set default storage location if warehouse is configured
            if self.warehouse_location:
                labels_dict: dict[str, str] = {
                    "default_storage_location": self._sanitize_label_value(self.warehouse_location)
                }
                dataset.labels = labels_dict

            with suppress(Conflict):
                self.client.create_dataset(dataset)

    def _validate_identifier(self, identifier: str | Identifier) -> str:
        """Validate identifier and return table name only."""
        if isinstance(identifier, str):
            parts = identifier.split(".")
            if len(parts) == 1:
                return parts[0]
            elif len(parts) == 2:
                dataset, table = parts
                if self.dataset_id and dataset != self.dataset_id:
                    raise ValidationError(
                        f"Dataset '{dataset}' does not match configured dataset '{self.dataset_id}'"
                    )
                return table
            else:
                raise ValidationError(f"Invalid identifier format: {identifier}")
        else:
            identifier_tuple = self.identifier_to_tuple(identifier)
            if len(identifier_tuple) == 1:
                return identifier_tuple[0]
            elif len(identifier_tuple) == 2:
                dataset, table = identifier_tuple
                if self.dataset_id and dataset != self.dataset_id:
                    raise ValidationError(
                        f"Dataset '{dataset}' does not match configured dataset '{self.dataset_id}'"
                    )
                return table
            else:
                raise ValidationError(f"Invalid identifier format: {identifier}")

    def _full_identifier(self, table_name: str) -> Identifier:
        """Get the full identifier including catalog name."""
        assert self.dataset_id is not None  # For mypy
        return (self.name, self.dataset_id, table_name)

    def _resolve_table_location(
        self, location: str | None, dataset_name: str, table_name: str
    ) -> str:
        """Resolve table location."""
        if location:
            return location.rstrip("/")

        # Check dataset properties first
        try:
            dataset_properties = self.load_namespace_properties((dataset_name,))
            if dataset_location := dataset_properties.get("location"):
                return f"{dataset_location.rstrip('/')}/{table_name}"
        except NoSuchNamespaceError:
            pass

        # Use warehouse from catalog properties
        if self.warehouse_location:
            return f"{self.warehouse_location.rstrip('/')}/{dataset_name}.db/{table_name}"

        raise ValueError("No default path is set, please specify a location when creating a table")

    @staticmethod
    def _get_metadata_location(location: str, new_version: int = 0) -> str:
        """Generate metadata file location."""
        if new_version < 0:
            raise ValueError(
                f"Table metadata version: `{new_version}` must be a non-negative integer"
            )
        version_str = f"{new_version:05d}"
        return f"{location}/metadata/{version_str}-{uuid.uuid4()}.metadata.json"

    def _create_bigquery_external_table(
        self,
        table_ref: bigquery.TableReference,
        metadata: TableMetadata,
        metadata_location: str,
    ) -> bigquery.Table:
        """Create a BigQuery external table configuration for Iceberg."""
        # Build the full table definition
        table_def: dict[str, Any] = {
            "tableReference": {
                "projectId": table_ref.project,
                "datasetId": table_ref.dataset_id,
                "tableId": table_ref.table_id,
            },
            "externalDataConfiguration": {
                "sourceFormat": "ICEBERG",
                "sourceUris": [metadata_location],
                # This is where we store the metadata
                "icebergOptions": {"metadataLocation": metadata_location, "fileFormat": "PARQUET"},
            },
        }

        # Add connection if provided
        bq_connection = (
            metadata.properties.get(self.PROPERTIES_KEY_BQ_CONNECTION)
            if hasattr(metadata, "properties")
            else None
        )
        if bq_connection:
            external_config = cast(dict[str, Any], table_def["externalDataConfiguration"])
            external_config["connectionId"] = bq_connection

        # Create table from the definition
        table = bigquery.Table.from_api_repr(table_def)

        # Store metadata in the table's description as a fallback
        table_metadata = self._build_table_parameters(metadata_location, metadata)
        table.description = json.dumps({"iceberg_metadata": table_metadata})[
            :1024
        ]  # Limit to BigQuery's description length limit

        return table

    def _build_table_parameters(
        self, metadata_location: str, metadata: TableMetadata
    ) -> dict[str, str]:
        """Build table parameters matching Java implementation."""
        parameters: dict[str, str] = {}

        # Add properties from metadata if available
        if hasattr(metadata, "properties"):
            parameters = dict(metadata.properties)

        # Add core metadata
        parameters[self.EXTERNAL_METADATA_LOCATION_KEY] = metadata_location

        # Add location if available
        if hasattr(metadata, "location"):
            parameters[self.EXTERNAL_LOCATION_KEY] = metadata.location

        parameters[self.EXTERNAL_TABLE_TYPE_KEY] = self.EXTERNAL_TABLE_TYPE_VALUE
        parameters[self.EXTERNAL_KEY] = "TRUE"

        # Add UUID if present
        if hasattr(metadata, "table_uuid") and metadata.table_uuid:
            parameters[self.EXTERNAL_UUID_KEY] = str(metadata.table_uuid)

        # Add snapshot metadata
        self._update_parameters_with_snapshot_metadata(metadata, parameters)

        return parameters

    def _update_parameters_with_snapshot_metadata(
        self, metadata: TableMetadata, parameters: dict[str, str]
    ) -> None:
        """Update parameters with snapshot metadata information."""
        # Check if metadata has current_snapshot method safely
        if hasattr(metadata, "current_snapshot") and callable(metadata.current_snapshot):
            try:
                snapshot = metadata.current_snapshot()
                if snapshot and hasattr(snapshot, "summary") and snapshot.summary:
                    summary = snapshot.summary
                    if "total-data-files" in summary:
                        parameters["numFiles"] = str(summary["total-data-files"])
                    if "total-records" in summary:
                        parameters["numRows"] = str(summary["total-records"])
                    if "total-files-size" in summary:
                        parameters["totalSize"] = str(summary["total-files-size"])
            except Exception:
                # Ignore errors in snapshot metadata extraction
                pass

    def _update_bigquery_table_metadata(
        self,
        bq_table: bigquery.Table,
        metadata: TableMetadata,
        metadata_location: str,
    ) -> None:
        """Update BigQuery external table metadata."""
        if not bq_table.external_data_configuration:
            raise ValueError("Table is not an external table")

        # Update the source URI to point to the new metadata location
        bq_table.external_data_configuration.source_uris = [metadata_location]

        # Update the description with the new metadata
        table_metadata = self._build_table_parameters(metadata_location, metadata)

        # Get old metadata from description if exists
        old_metadata = {}
        if bq_table.description:
            try:
                desc_data = json.loads(bq_table.description)
                if iceberg_metadata := desc_data.get("iceberg_metadata"):
                    old_metadata = iceberg_metadata if isinstance(iceberg_metadata, dict) else {}
            except json.JSONDecodeError:
                pass

        # Update previous metadata location if exists
        if old_location := old_metadata.get(self.EXTERNAL_METADATA_LOCATION_KEY):
            table_metadata[self.EXTERNAL_PREVIOUS_METADATA_LOCATION_KEY] = old_location

        # Update description with new metadata
        bq_table.description = json.dumps({"iceberg_metadata": table_metadata})[
            :1024
        ]  # Limit to BigQuery's description length limit

    def _validate_table(self, table: bigquery.Table) -> None:
        """Validate that a BigQuery table is a valid Iceberg table."""
        if not self._is_valid_iceberg_table(table):
            raise NoSuchTableError(f"Table {table.table_id} is not a valid Iceberg table")

    def _is_valid_iceberg_table(self, table: bigquery.Table) -> bool:
        """Check if a BigQuery table is a valid Iceberg table."""
        if not table.external_data_configuration:
            return False

        external_config = table.external_data_configuration

        # Check if it's an ICEBERG format
        if external_config.source_format != "ICEBERG":
            return False

        # For ICEBERG tables, having source URIs is sufficient
        if (
            external_config.source_uris
            and len(external_config.source_uris) > 0
            and external_config.source_uris[0]
        ):
            return True

        return False

    def _get_metadata_location_or_throw(self, table: bigquery.Table) -> str:
        """Extract metadata location from BigQuery table or throw error."""
        if not table.external_data_configuration:
            raise ValidationError(f"Table {table.table_id} is not an external table")

        # First check if the metadata is in the external configuration
        external_config = table.external_data_configuration

        # For ICEBERG tables, the metadata location might be in different places
        # Check icebergOptions first
        if hasattr(external_config, "iceberg_options") and external_config.iceberg_options:
            metadata_loc = external_config.iceberg_options.get("metadataLocation")
            if metadata_loc:
                return str(metadata_loc)

        # Check if it's in the description (where we stored it as a fallback)
        if table.description:
            try:
                desc_data = json.loads(table.description)
                iceberg_meta_raw = desc_data.get("iceberg_metadata")
                if isinstance(iceberg_meta_raw, dict):
                    metadata_loc = iceberg_meta_raw.get(self.EXTERNAL_METADATA_LOCATION_KEY)
                    if metadata_loc:
                        return str(metadata_loc)
            except json.JSONDecodeError:
                pass

        # For ICEBERG external tables, the source URI might be the metadata location
        if (
            external_config.source_uris
            and len(external_config.source_uris) > 0
            and external_config.source_uris[0]
        ):
            # This should be the metadata location for ICEBERG tables
            return str(external_config.source_uris[0])

        raise ValidationError(
            f"Table {table.table_id} is not a valid BigQuery Metastore Iceberg table, "
            "metadata location not found"
        )

    def _validate_etag(self, table: bigquery.Table) -> None:
        """Validate table etag for update operations."""
        if not table.etag:
            raise ValidationError(
                f"Etag of table {table.table_id} is empty, "
                "manually update the table via the BigQuery API or recreate and retry"
            )

    def _create_table_instance(
        self,
        identifier: Identifier,
        metadata: TableMetadata,
        metadata_location: str,
    ) -> Table:
        """Create a Table instance properly integrated with PyIceberg."""
        return Table(
            identifier=identifier,
            metadata=metadata,
            metadata_location=metadata_location,
            io=self.file_io,
            catalog=self,
        )

    @staticmethod
    def _delete_files(io: FileIO, files: set[str], file_type: str) -> None:
        """Delete files and log warnings on failure."""
        for file in files:
            try:
                io.delete(file)
            except Exception as e:
                # Log warning but continue
                logger.warning(f"Failed to delete {file_type} file {file}: {e}")

    @staticmethod
    def _delete_data_files(io: FileIO, manifests: list[ManifestFile]) -> None:
        """Delete data files referenced by manifests."""
        deleted_files = set()

        for manifest_file in manifests:
            for entry in manifest_file.fetch_manifest_entry(io, discard_deleted=False):
                path = entry.data_file.file_path
                if path not in deleted_files:
                    try:
                        io.delete(path)
                        deleted_files.add(path)
                    except Exception as e:
                        logger.warning(f"Failed to delete data file {path}: {e}")

    @staticmethod
    def _sanitize_label_key(key: str) -> str:
        """Sanitize a key to be valid as a BigQuery label key."""
        if not key:
            return ""
        # BigQuery label keys: lowercase letters, numbers, hyphens, underscores
        # Must start with a letter
        sanitized = re.sub(r"[^a-z0-9_-]", "_", key.lower())
        # Ensure it starts with a letter
        if sanitized and not sanitized[0].isalpha():
            sanitized = "l_" + sanitized
        return sanitized[:63]

    @staticmethod
    def _sanitize_label_value(value: str) -> str:
        """Sanitize a value to be valid as a BigQuery label value."""
        if not value:
            return ""
        # BigQuery label values: lowercase letters, numbers, hyphens, underscores
        sanitized = re.sub(r"[^a-z0-9_-]", "_", value.lower())
        return sanitized[:63]

    def _property_to_label_key(self, property_key: str) -> str:
        """Convert a property key to a BigQuery label key."""
        if property_key == "location":
            return "default_storage_location"
        return self._sanitize_label_key(property_key)

    def _label_to_property_key(self, label_key: str) -> str:
        """Convert a BigQuery label key back to a property key."""
        if label_key == "default_storage_location":
            return "location"
        # Best effort reverse conversion
        return label_key.replace("_", ".")

    @staticmethod
    def _parse_metadata_version(metadata_location: str) -> int:
        """Parse the metadata version from a metadata location path."""
        # Pattern: {location}/metadata/{version}-{uuid}.metadata.json
        pattern = r"/metadata/(\d{5})-[a-f0-9-]+\.metadata\.json$"
        match = re.search(pattern, metadata_location)
        if match:
            return int(match.group(1))
        return 0
