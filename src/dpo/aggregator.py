"""Unified Metrics Aggregator.

Creates unified views across all profile_metrics and drift_metrics tables
for centralized observability. Supports per-group aggregation.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from databricks.sdk import WorkspaceClient

from dpo.config import OrchestratorConfig
from dpo.discovery import DiscoveredTable
from dpo.utils import sanitize_sql_identifier

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates metrics from multiple monitors into unified views.

    Supports both profile_metrics and drift_metrics tables for comprehensive
    data quality and drift monitoring.

    Features:
    - Explicit column selection (avoids SELECT * schema mismatch issues)
    - Materialized View support for performance
    - UC tag metadata injection for business unit filtering
    - Support for BASELINE and CONSECUTIVE drift types
    """

    # Standard columns for drift_metrics tables with chi_square for categorical
    STANDARD_DRIFT_COLUMNS = [
        "window_start",
        "window_end",
        "column_name",
        "CAST(js_divergence AS DOUBLE) as js_divergence",
        "CAST(ks_statistic AS DOUBLE) as ks_statistic",
        "CAST(wasserstein_distance AS DOUBLE) as wasserstein_distance",
        "CAST(chi_square_statistic AS DOUBLE) as chi_square_statistic",
        "drift_type",
        "slice_key",
        "slice_value",
    ]

    # Standard columns for profile_metrics tables
    STANDARD_PROFILE_COLUMNS = [
        "window_start",
        "window_end",
        "column_name",
        "CAST(count AS BIGINT) as record_count",
        "CAST(null_count AS BIGINT) as null_count",
        "CAST(percent_null AS DOUBLE) as null_rate",
        "CAST(distinct_count AS BIGINT) as distinct_count",
        "CAST(mean AS DOUBLE) as mean",
        "CAST(stddev AS DOUBLE) as stddev",
        "CAST(min AS DOUBLE) as min_value",
        "CAST(max AS DOUBLE) as max_value",
        "slice_key",
        "slice_value",
    ]

    def __init__(
        self, workspace_client: WorkspaceClient, config: OrchestratorConfig
    ):
        self.w = workspace_client
        self.config = config
        self.catalog = config.catalog_name
        self.output_schema = config.profile_defaults.output_schema_name
        self.warehouse_id = config.warehouse_id

    @staticmethod
    def _sql_literal(value: Optional[str]) -> str:
        """Return a safely-escaped SQL string literal."""
        if value is None:
            return "NULL"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _safe_priority(value: Optional[str]) -> int:
        """Parse priority tag into an integer, falling back safely."""
        try:
            return int(value) if value is not None else 99
        except (TypeError, ValueError):
            return 99

    def _resolve_drift_threshold(self, table: DiscoveredTable) -> float:
        """Resolve drift threshold for a table with config override support."""
        table_cfg = self.config.monitored_tables.get(table.full_name)
        if table_cfg and table_cfg.drift_threshold is not None:
            return table_cfg.drift_threshold
        return self.config.alerting.drift_threshold

    def create_unified_drift_view(
        self,
        discovered_tables: List[DiscoveredTable],
        output_view: str,
        use_materialized: Optional[bool] = None,
    ) -> str:
        """Create a unified view aggregating all drift_metrics tables.

        Args:
            discovered_tables: Tables with active monitors.
            output_view: Full view name (e.g., "catalog.schema.unified_drift_metrics").
            use_materialized: Force materialized view (auto-detect if None).

        Returns:
            The DDL statement that was executed.
        """
        if use_materialized is None:
            use_materialized = self._supports_materialized_views()

        self._ensure_output_schema(output_view)
        ddl = self._generate_unified_drift_view_ddl(
            discovered_tables, output_view, use_materialized
        )

        logger.info(
            "Creating unified drift view: %s (materialized=%s)",
            output_view,
            use_materialized,
        )

        try:
            result = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=ddl,
                wait_timeout="120s",
            )

            if result.status and result.status.state.value == "FAILED":
                raise RuntimeError(f"Failed to create view: {result.status.error}")

            logger.info("Unified drift view created successfully: %s", output_view)

        except Exception as e:
            logger.error("Failed to create unified drift view: %s", e)
            raise

        return ddl

    def create_unified_profile_view(
        self,
        discovered_tables: List[DiscoveredTable],
        output_view: str,
        use_materialized: Optional[bool] = None,
    ) -> str:
        """Create a unified view aggregating all profile_metrics tables.

        Args:
            discovered_tables: Tables with active monitors.
            output_view: Full view name (e.g., "catalog.schema.unified_profile_metrics").
            use_materialized: Force materialized view (auto-detect if None).

        Returns:
            The DDL statement that was executed.
        """
        if use_materialized is None:
            use_materialized = self._supports_materialized_views()

        self._ensure_output_schema(output_view)
        ddl = self._generate_unified_profile_view_ddl(
            discovered_tables, output_view, use_materialized
        )

        logger.info(
            "Creating unified profile view: %s (materialized=%s)",
            output_view,
            use_materialized,
        )

        try:
            result = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=ddl,
                wait_timeout="120s",
            )

            if result.status and result.status.state.value == "FAILED":
                raise RuntimeError(f"Failed to create view: {result.status.error}")

            logger.info("Unified profile view created successfully: %s", output_view)

        except Exception as e:
            logger.error("Failed to create unified profile view: %s", e)
            raise

        return ddl

    def _generate_unified_drift_view_ddl(
        self,
        discovered_tables: List[DiscoveredTable],
        output_view: str,
        use_materialized: bool,
    ) -> str:
        """Generate DDL for unified drift metrics view with enrichment metadata."""
        view_type = "MATERIALIZED VIEW" if use_materialized else "VIEW"
        columns_sql = ", ".join(self.STANDARD_DRIFT_COLUMNS)

        union_parts = []
        for table in discovered_tables:
            owner = table.owner or table.tags.get("owner", "unknown")
            department = table.tags.get("department", "unknown")
            priority = self._safe_priority(table.tags.get("monitor_priority", "99"))
            runbook_url = table.runbook_url or ""
            lineage_url = table.lineage_url or ""
            drift_threshold = self._resolve_drift_threshold(table)

            drift_table = (
                f"{self.catalog}.{self.output_schema}.{table.table_name}_drift_metrics"
            )

            union_parts.append(f"""
                SELECT
                    {columns_sql},
                    {self._sql_literal(table.full_name)} as source_table_name,
                    {self._sql_literal(owner)} as owner,
                    {self._sql_literal(department)} as department,
                    {priority} as priority,
                    {self._sql_literal(runbook_url)} as runbook_url,
                    {self._sql_literal(lineage_url)} as lineage_url,
                    CAST({drift_threshold} AS DOUBLE) as drift_threshold
                FROM {drift_table}
            """)

        if not union_parts:
            return f"""
            CREATE OR REPLACE {view_type} {output_view} AS
            SELECT
                CAST(NULL AS TIMESTAMP) as window_start,
                CAST(NULL AS TIMESTAMP) as window_end,
                CAST(NULL AS STRING) as column_name,
                CAST(NULL AS DOUBLE) as js_divergence,
                CAST(NULL AS DOUBLE) as ks_statistic,
                CAST(NULL AS DOUBLE) as wasserstein_distance,
                CAST(NULL AS DOUBLE) as chi_square_statistic,
                CAST(NULL AS STRING) as drift_type,
                CAST(NULL AS STRING) as slice_key,
                CAST(NULL AS STRING) as slice_value,
                CAST(NULL AS STRING) as source_table_name,
                CAST(NULL AS STRING) as owner,
                CAST(NULL AS STRING) as department,
                CAST(NULL AS INT) as priority,
                CAST(NULL AS STRING) as runbook_url,
                CAST(NULL AS STRING) as lineage_url,
                CAST(NULL AS DOUBLE) as drift_threshold
            WHERE 1=0
            """

        return (
            f"CREATE OR REPLACE {view_type} {output_view} AS "
            + " UNION ALL ".join(union_parts)
        )

    def _generate_unified_profile_view_ddl(
        self,
        discovered_tables: List[DiscoveredTable],
        output_view: str,
        use_materialized: bool,
    ) -> str:
        """Generate DDL for unified profile metrics view."""
        view_type = "MATERIALIZED VIEW" if use_materialized else "VIEW"
        columns_sql = ", ".join(self.STANDARD_PROFILE_COLUMNS)

        union_parts = []
        for table in discovered_tables:
            owner = table.owner or table.tags.get("owner", "unknown")
            department = table.tags.get("department", "unknown")
            priority = self._safe_priority(table.tags.get("monitor_priority", "99"))
            runbook_url = table.runbook_url or ""
            lineage_url = table.lineage_url or ""

            profile_table = (
                f"{self.catalog}.{self.output_schema}."
                f"{table.table_name}_profile_metrics"
            )

            union_parts.append(f"""
                SELECT
                    {columns_sql},
                    {self._sql_literal(table.full_name)} as source_table_name,
                    {self._sql_literal(owner)} as owner,
                    {self._sql_literal(department)} as department,
                    {priority} as priority,
                    {self._sql_literal(runbook_url)} as runbook_url,
                    {self._sql_literal(lineage_url)} as lineage_url
                FROM {profile_table}
            """)

        if not union_parts:
            return f"""
            CREATE OR REPLACE {view_type} {output_view} AS
            SELECT
                CAST(NULL AS TIMESTAMP) as window_start,
                CAST(NULL AS TIMESTAMP) as window_end,
                CAST(NULL AS STRING) as column_name,
                CAST(NULL AS BIGINT) as record_count,
                CAST(NULL AS BIGINT) as null_count,
                CAST(NULL AS DOUBLE) as null_rate,
                CAST(NULL AS BIGINT) as distinct_count,
                CAST(NULL AS DOUBLE) as mean,
                CAST(NULL AS DOUBLE) as stddev,
                CAST(NULL AS DOUBLE) as min_value,
                CAST(NULL AS DOUBLE) as max_value,
                CAST(NULL AS STRING) as slice_key,
                CAST(NULL AS STRING) as slice_value,
                CAST(NULL AS STRING) as source_table_name,
                CAST(NULL AS STRING) as owner,
                CAST(NULL AS STRING) as department,
                CAST(NULL AS INT) as priority,
                CAST(NULL AS STRING) as runbook_url,
                CAST(NULL AS STRING) as lineage_url
            WHERE 1=0
            """

        return (
            f"CREATE OR REPLACE {view_type} {output_view} AS "
            + " UNION ALL ".join(union_parts)
        )

    def _supports_materialized_views(self) -> bool:
        """Check if workspace has Serverless SQL enabled."""
        try:
            warehouses = list(self.w.warehouses.list())
            supports = any(
                getattr(wh, "enable_serverless_compute", False)
                for wh in warehouses
            )

            if not supports:
                logger.warning(
                    "Materialized Views not available. "
                    "Dashboard may be slow with 100+ tables."
                )

            return supports

        except Exception as e:
            logger.warning("Failed to check Serverless support: %s", e)
            return False

    def _ensure_output_schema(self, output_view: str) -> None:
        """Ensure the output schema exists."""
        parts = output_view.split(".")
        if len(parts) >= 2:
            catalog = parts[0]
            schema = parts[1]

            try:
                self.w.schemas.get(full_name=f"{catalog}.{schema}")
            except Exception:
                logger.info("Creating schema %s.%s", catalog, schema)
                try:
                    self.w.schemas.create(name=schema, catalog_name=catalog)
                except Exception as e:
                    logger.warning("Failed to create schema: %s", e)

    def get_drift_summary_query(self, unified_view: str) -> str:
        """Generate a "Wall of Shame" query for worst drifting models.

        Args:
            unified_view: The unified drift view name.

        Returns:
            SQL query for top drifters across all monitored tables.
        """
        return f"""
        SELECT
            source_table_name,
            department,
            owner,
            COUNT(*) as drift_events,
            MAX(js_divergence) as max_drift,
            AVG(js_divergence) as avg_drift,
            MAX(window_end) as last_drift_time
        FROM {unified_view}
        WHERE js_divergence >= 0.1
        GROUP BY source_table_name, department, owner
        ORDER BY max_drift DESC
        LIMIT 20
        """

    def get_feature_drift_query(self, unified_view: str, table_name: str) -> str:
        """Generate query for per-feature drift analysis for a specific table.

        Args:
            unified_view: The unified drift view name.
            table_name: The source table to filter by.

        Returns:
            SQL query for feature-level drift details.
        """
        return f"""
        SELECT
            column_name,
            window_start,
            window_end,
            js_divergence,
            ks_statistic,
            wasserstein_distance,
            chi_square_statistic,
            drift_type
        FROM {unified_view}
        WHERE source_table_name = '{table_name}'
        ORDER BY window_end DESC, js_divergence DESC
        """

    def get_data_quality_summary_query(self, unified_profile_view: str) -> str:
        """Generate query for data quality summary across all tables.

        Args:
            unified_profile_view: The unified profile view name.

        Returns:
            SQL query for data quality metrics summary.
        """
        return f"""
        SELECT
            source_table_name,
            department,
            owner,
            COUNT(DISTINCT column_name) as columns_monitored,
            AVG(null_rate) as avg_null_rate,
            MAX(null_rate) as max_null_rate,
            SUM(record_count) as total_records,
            MAX(window_end) as last_profile_time
        FROM {unified_profile_view}
        GROUP BY source_table_name, department, owner
        ORDER BY max_null_rate DESC
        LIMIT 20
        """

    def create_unified_views_by_group(
        self,
        discovered_tables: List[DiscoveredTable],
        output_schema: str,
        group_tag: str = "monitor_group",
    ) -> Dict[str, Tuple[str, str]]:
        """Create unified drift and profile views for each monitor group.

        Args:
            discovered_tables: All discovered tables.
            output_schema: Schema for unified views.
            group_tag: Tag name used to group tables.

        Returns:
            Dict mapping group_name -> (drift_view_name, profile_view_name)
        """
        # Group tables by monitor_group tag
        groups: Dict[str, List[DiscoveredTable]] = defaultdict(list)
        for table in discovered_tables:
            group_name = table.tags.get(group_tag, "default")
            groups[group_name].append(table)

        results = {}
        for group_name, tables in groups.items():
            # Use sanitize_sql_identifier to handle special characters
            safe_name = sanitize_sql_identifier(group_name)

            drift_view = f"{output_schema}.unified_drift_metrics_{safe_name}"
            profile_view = f"{output_schema}.unified_profile_metrics_{safe_name}"

            self.create_unified_drift_view(tables, drift_view)
            self.create_unified_profile_view(tables, profile_view)

            results[group_name] = (drift_view, profile_view)
            logger.info(
                f"Created unified views for group '{group_name}': {drift_view}, {profile_view}"
            )

        return results

    def cleanup_stale_views(
        self,
        output_schema: str,
        active_groups: Set[str],
    ) -> List[str]:
        """Remove unified views that no longer correspond to active groups.

        Scenario: User renames monitor_group "marketing" to "growth".
        Result: unified_drift_metrics_marketing becomes stale.

        Safety: Skips cleanup when active_groups is empty to prevent
        accidental deletion of all views during transient failures.

        Args:
            output_schema: Schema containing unified views.
            active_groups: Set of currently active group names (already sanitized).

        Returns:
            List of dropped view names.
        """
        dropped = []

        if not active_groups:
            logger.warning("No active groups; skipping view cleanup to prevent accidental deletion")
            return dropped

        # List all tables/views in the output schema
        try:
            result = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=f"SHOW VIEWS IN {output_schema}",
                wait_timeout="30s",
            )

            if not result.result or not result.result.data_array:
                return dropped

            for row in result.result.data_array:
                view_name = row[0]

                # Check if it's a unified view we manage
                for prefix in ["unified_drift_metrics_", "unified_profile_metrics_"]:
                    if view_name.startswith(prefix):
                        group_suffix = view_name[len(prefix) :]

                        # If suffix is not in active groups, drop it
                        if group_suffix not in active_groups:
                            full_view_name = f"{output_schema}.{view_name}"
                            try:
                                self.w.statement_execution.execute_statement(
                                    warehouse_id=self.warehouse_id,
                                    statement=f"DROP VIEW IF EXISTS {full_view_name}",
                                    wait_timeout="30s",
                                )
                                dropped.append(full_view_name)
                                logger.info(f"Dropped stale view: {full_view_name}")
                            except Exception as e:
                                logger.warning(
                                    f"Failed to drop stale view {full_view_name}: {e}"
                                )
        except Exception as e:
            logger.warning(f"Failed to list views for cleanup: {e}")

        return dropped
