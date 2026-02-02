"""
Phase 1: Metadata-Driven Discovery Engine

Crawls Unity Catalog to find tables tagged for monitoring.
"""

import fnmatch
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, TableType

from dpo.config import DiscoveryConfig

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredTable:
    """Represents a table discovered for monitoring."""

    full_name: str
    tags: Dict[str, str] = field(default_factory=dict)
    has_primary_key: bool = False
    columns: List[ColumnInfo] = field(default_factory=list)
    table_type: str = "UNKNOWN"
    priority: int = 99

    @property
    def catalog(self) -> str:
        return self.full_name.split(".")[0]

    @property
    def schema(self) -> str:
        return self.full_name.split(".")[1]

    @property
    def table_name(self) -> str:
        return self.full_name.split(".")[2]


class TableDiscovery:
    """
    Discovers tables in Unity Catalog based on tags and filters.
    
    Features:
    - Filter by configurable tags (e.g., monitor_enabled=true)
    - Exclude schemas matching glob patterns
    - Priority sorting for quota-aware provisioning
    - Primary key detection for optimal drift metrics
    - UC tag collection for metadata injection
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        config: DiscoveryConfig,
        catalog: str,
    ):
        """
        Args:
            workspace_client: Authenticated Databricks WorkspaceClient.
            config: Discovery configuration with tags and filters.
            catalog: Target catalog name.
        """
        self.w = workspace_client
        self.config = config
        self.catalog = catalog
        self._table_tags_cache: Dict[str, Dict[str, str]] = {}

    def discover(self) -> List[DiscoveredTable]:
        """
        Discover all tables matching the configured criteria.
        
        Returns tables sorted by priority (lower = more important).
        """
        tables = []
        schemas = self._get_schemas()

        logger.info(f"Scanning {len(schemas)} schemas in catalog '{self.catalog}'")

        self._fetch_all_tags(schemas)
        logger.info(f"Fetched tags for {len(self._table_tags_cache)} tables")

        for schema_name in schemas:
            try:
                schema_tables = self._discover_schema(schema_name)
                tables.extend(schema_tables)
            except Exception as e:
                logger.warning(f"Failed to scan schema '{schema_name}': {e}")
                continue

        # Sort by priority (lower = more important, provisioned first)
        tables = sorted(tables, key=lambda t: t.priority)

        logger.info(f"Discovered {len(tables)} tables tagged for monitoring")
        return tables

    def _fetch_all_tags(self, schemas: List[str]) -> None:
        """Fetch all table tags. Uses SparkSQL if available, falls back to SDK."""
        if not schemas:
            return

        self._table_tags_cache = {}

        try:
            self._fetch_tags_via_spark(schemas)
        except Exception:
            self._fetch_tags_via_sdk(schemas)

    def _fetch_tags_via_spark(self, schemas: List[str]) -> None:
        """Fetch tags using SparkSQL."""
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session")

        schema_list = ", ".join(f"'{s}'" for s in schemas)
        query = f"""
            SELECT catalog_name, schema_name, table_name, tag_name, tag_value
            FROM {self.catalog}.information_schema.table_tags
            WHERE schema_name IN ({schema_list})
        """

        df = spark.sql(query)
        for row in df.collect():
            full_name = f"{row.catalog_name}.{row.schema_name}.{row.table_name}"
            if full_name not in self._table_tags_cache:
                self._table_tags_cache[full_name] = {}
            self._table_tags_cache[full_name][row.tag_name] = row.tag_value

        logger.debug(f"Fetched tags via SparkSQL for {len(self._table_tags_cache)} tables")

    def _fetch_tags_via_sdk(self, schemas: List[str]) -> None:
        """Fetch tags using EntityTagAssignmentsAPI."""
        for schema_name in schemas:
            for table_summary in self.w.tables.list(
                catalog_name=self.catalog, schema_name=schema_name
            ):
                full_name = table_summary.full_name
                try:
                    tags = self.w.entity_tag_assignments.list(
                        entity_type="TABLE", entity_name=full_name
                    )
                    self._table_tags_cache[full_name] = {
                        tag.tag_key: tag.tag_value for tag in tags
                    }
                except Exception as e:
                    logger.debug(f"Could not fetch tags for {full_name}: {e}")

        logger.debug(f"Fetched tags via SDK for {len(self._table_tags_cache)} tables")

    def _get_schemas(self) -> List[str]:
        """Get schemas in the catalog, applying include/exclude filters."""
        schemas = []

        for schema in self.w.schemas.list(catalog_name=self.catalog):
            schema_name = schema.name

            # If include_schemas specified, check whitelist first
            if self.config.include_schemas:
                if not self._matches_include_pattern(schema_name):
                    logger.debug(f"Skipping schema '{schema_name}' (not in include_schemas)")
                    continue

            # Then check exclusions
            if self._matches_exclude_pattern(schema_name):
                logger.debug(f"Excluding schema '{schema_name}' (matches exclude pattern)")
                continue

            schemas.append(schema_name)

        return schemas

    def _matches_include_pattern(self, schema_name: str) -> bool:
        """Check if schema name matches any include glob pattern."""
        for pattern in self.config.include_schemas:
            if fnmatch.fnmatch(schema_name, pattern):
                return True
        return False

    def _matches_exclude_pattern(self, schema_name: str) -> bool:
        """Check if schema name matches any exclude glob pattern."""
        for pattern in self.config.exclude_schemas:
            if fnmatch.fnmatch(schema_name, pattern):
                return True
        return False

    def _discover_schema(self, schema_name: str) -> List[DiscoveredTable]:
        """Discover tables in a single schema."""
        tables = []

        for table_summary in self.w.tables.list(catalog_name=self.catalog, schema_name=schema_name):
            try:
                full_name = table_summary.full_name

                if not self._matches_tags(full_name):
                    continue

                # Get full table info for columns and type checking
                table_info = self.w.tables.get(full_name=full_name)

                # Only include Delta tables (as per Databricks constraints)
                if not self._is_supported_table_type(table_info):
                    logger.debug(f"Skipping non-Delta table: {full_name}")
                    continue

                discovered = self._build_discovered_table(table_info)
                tables.append(discovered)

                logger.debug(f"Discovered table: {discovered.full_name} (priority={discovered.priority})")

            except Exception as e:
                logger.warning(f"Failed to inspect table '{table_summary.full_name}': {e}")
                continue

        return tables

    def _matches_tags(self, full_name: str) -> bool:
        """Check if table has all required tags using cached INFORMATION_SCHEMA data."""
        table_tags = self._table_tags_cache.get(full_name, {})

        for required_key, required_value in self.config.include_tags.items():
            if table_tags.get(required_key) != required_value:
                return False

        return True

    def _is_supported_table_type(self, table_info) -> bool:
        """Check if table type is supported for Data Profiling."""
        supported_types = {
            TableType.MANAGED,
            TableType.EXTERNAL,
            TableType.VIEW,
            TableType.MATERIALIZED_VIEW,
            TableType.STREAMING_TABLE,
        }
        return table_info.table_type in supported_types

    def _build_discovered_table(self, table_info) -> DiscoveredTable:
        """Build a DiscoveredTable from table metadata."""
        tags = self._table_tags_cache.get(table_info.full_name, {})
        columns = table_info.columns or []

        return DiscoveredTable(
            full_name=table_info.full_name,
            tags=tags,
            has_primary_key=self._has_primary_key(columns),
            columns=columns,
            table_type=table_info.table_type.value if table_info.table_type else "UNKNOWN",
            priority=int(tags.get("monitor_priority", "99")),
        )

    def _has_primary_key(self, columns: List[ColumnInfo]) -> bool:
        """
        Check if table has a primary key or unique identifier column.
        
        Looks for:
        - Columns with primary key constraint
        - Columns named 'id', 'pk', or ending with '_id'
        """
        for col in columns:
            # Check for PK constraint (if available in metadata)
            if hasattr(col, "constraint") and col.constraint:
                if "primary" in str(col.constraint).lower():
                    return True

            # Heuristic: common ID column naming patterns
            col_name = col.name.lower()
            if col_name in ("id", "pk", "primary_key", "row_id", "observation_id"):
                return True
            if col_name.endswith("_id") and col_name not in ("model_id", "request_id"):
                return True

        return False

    def get_column_names(self, table: DiscoveredTable) -> List[str]:
        """Get list of column names for a table."""
        return [col.name for col in table.columns]

    def find_column(self, table: DiscoveredTable, name: str) -> Optional[ColumnInfo]:
        """Find a column by name in a table."""
        for col in table.columns:
            if col.name.lower() == name.lower():
                return col
        return None

    def get_column_type(self, table: DiscoveredTable, name: str) -> Optional[str]:
        """Get the data type of a column."""
        col = self.find_column(table, name)
        return col.type_text if col else None
