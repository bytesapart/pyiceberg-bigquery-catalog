# pyiceberg_bigquery_catalog/__init__.py
"""
PyIceberg BigQuery Catalog implementation.

This module provides a catalog implementation for Apache Iceberg that uses Google BigQuery
as the metadata store.
"""

__version__ = "0.1.0"

from pyiceberg_bigquery_catalog.catalog import BigQueryCatalog

__all__ = ["BigQueryCatalog"]