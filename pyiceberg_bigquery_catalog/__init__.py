# pyiceberg_bigquery_catalog/__init__.py
"""
PyIceberg BigQuery Catalog implementation.

This module provides a catalog implementation for Apache Iceberg that uses Google BigQuery
as the metadata store.
"""

__version__ = "0.0.0"  # This will be replaced by poetry-dynamic-versioning

from pyiceberg_bigquery_catalog.catalog import BigQueryCatalog

__all__ = ["BigQueryCatalog"]