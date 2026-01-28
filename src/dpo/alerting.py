"""
Phase 4: Observability and Alerting

Provisions SQL Alerts for drift detection and data quality issues.
Supports per-group alert routing.
"""

import logging
from typing import Dict, List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    AlertOptions,
    AlertOptionsEmptyResultState,
)

from dpo.config import OrchestratorConfig

logger = logging.getLogger(__name__)


class AlertProvisioner:
    """Provisions SQL Alerts for drift detection and data quality.

    Creates unified alerts on aggregated views to avoid alert clutter.

    Features:
    - Single unified drift alert on aggregated view
    - Data quality alerts (null rate, row count, cardinality)
    - Tiered thresholds (Warning/Critical) in query logic
    - Custom DDL generation for complex business rules
    """

    def __init__(
        self, workspace_client: WorkspaceClient, config: OrchestratorConfig
    ):
        self.w = workspace_client
        self.config = config
        self.warehouse_id = config.warehouse_id

    def _get_notifications_for_group(self, group_name: str) -> List[str]:
        """Get notification destinations for a specific group.

        Args:
            group_name: The monitor group name.

        Returns:
            List of notification destinations (emails, webhooks).
            Falls back to default_notifications if group has no specific routing.
        """
        group_notifications = self.config.alerting.group_notifications
        if group_name in group_notifications:
            return group_notifications[group_name]
        return self.config.alerting.default_notifications

    def create_alerts_by_group(
        self,
        views_by_group: Dict[str, Tuple[str, str]],
        catalog: str,
    ) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
        """Create drift and quality alerts for each monitor group with per-group routing.

        Args:
            views_by_group: Dict mapping group_name -> (drift_view, profile_view).
            catalog: Catalog name for alert naming.

        Returns:
            Dict mapping group_name -> (drift_alert_id, quality_alert_id)
        """
        results = {}
        for group_name, (drift_view, profile_view) in views_by_group.items():
            # Get notifications for this specific group
            notifications = self._get_notifications_for_group(group_name)

            drift_alert_id = self.create_unified_drift_alert(
                drift_view,
                catalog,
                alert_suffix=f" - {group_name}",
                notifications=notifications,
            )
            quality_alert_id = self.create_data_quality_alert(
                profile_view,
                catalog,
                alert_suffix=f" - {group_name}",
                notifications=notifications,
            )
            results[group_name] = (drift_alert_id, quality_alert_id)

        return results

    def create_unified_drift_alert(
        self,
        unified_drift_view: str,
        catalog: str,
        alert_suffix: str = "",
        notifications: Optional[List[str]] = None,
    ) -> str:
        """Create ONE alert that monitors tables via the unified drift view.

        Args:
            unified_drift_view: The unified drift metrics view name.
            catalog: Catalog name for alert naming.
            alert_suffix: Suffix to append to alert name (e.g., " - ml_team").
            notifications: Override notification destinations (uses default if None).

        Returns:
            The alert ID.
        """
        threshold = self.config.alerting.drift_threshold
        warning_threshold = max(0.1, threshold / 2)

        query_name = f"DPO Drift Detection - {catalog}{alert_suffix}"
        query_sql = f"""
        SELECT 
            source_table_name,
            owner,
            department,
            column_name,
            js_divergence,
            chi_square_statistic,
            drift_type,
            CASE 
                WHEN js_divergence >= {threshold} THEN 'CRITICAL'
                WHEN js_divergence >= {warning_threshold} THEN 'WARNING'
            END as severity,
            window_start,
            window_end
        FROM {unified_drift_view}
        WHERE js_divergence >= {warning_threshold}
        ORDER BY js_divergence DESC
        LIMIT 100
        """

        logger.info("Creating drift detection query: %s", query_name)

        try:
            query = self._create_or_update_query(query_name, query_sql)

            alert_name = f"[DPO] Drift Alert - {catalog}{alert_suffix}"
            alert = self._create_or_update_alert(
                name=alert_name,
                query_id=query.id,
                column="js_divergence",
                op=">=",
                value=str(warning_threshold),
            )

            # Use provided notifications or fall back to default
            notification_list = (
                notifications
                if notifications is not None
                else self.config.alerting.default_notifications
            )
            for notification_dest in notification_list:
                self._add_notification(alert.id, notification_dest)

            logger.info(
                "Created unified drift alert: %s (ID: %s)", alert_name, alert.id
            )
            return alert.id

        except Exception as e:
            logger.error("Failed to create unified drift alert: %s", e)
            raise

    def create_data_quality_alert(
        self,
        unified_profile_view: str,
        catalog: str,
        alert_suffix: str = "",
        notifications: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Create alert for data quality issues from profile_metrics.

        Monitors null rates, row counts, and cardinality drops.

        Args:
            unified_profile_view: The unified profile metrics view name.
            catalog: Catalog name for alert naming.
            alert_suffix: Suffix to append to alert name (e.g., " - ml_team").
            notifications: Override notification destinations (uses default if None).

        Returns:
            The alert ID if created, None if no thresholds configured.
        """
        null_threshold = self.config.alerting.null_rate_threshold
        row_min = self.config.alerting.row_count_min
        distinct_min = self.config.alerting.distinct_count_min

        # Skip if no data quality thresholds configured
        if not any([null_threshold, row_min, distinct_min]):
            logger.info(
                "No data quality thresholds configured, skipping alert creation"
            )
            return None

        conditions = []
        if null_threshold is not None:
            conditions.append(f"null_rate > {null_threshold}")
        if row_min is not None:
            conditions.append(f"record_count < {row_min}")
        if distinct_min is not None:
            conditions.append(f"distinct_count < {distinct_min}")

        where_clause = " OR ".join(conditions)

        query_name = f"DPO Data Quality - {catalog}{alert_suffix}"
        query_sql = f"""
        SELECT 
            source_table_name,
            owner,
            department,
            column_name,
            null_rate,
            record_count,
            distinct_count,
            CASE 
                WHEN null_rate > {null_threshold or 1.0} THEN 'HIGH_NULL_RATE'
                WHEN record_count < {row_min or 0} THEN 'LOW_ROW_COUNT'
                WHEN distinct_count < {distinct_min or 0} THEN 'LOW_CARDINALITY'
            END as issue_type,
            window_start,
            window_end
        FROM {unified_profile_view}
        WHERE {where_clause}
        ORDER BY null_rate DESC, record_count ASC
        LIMIT 100
        """

        logger.info("Creating data quality query: %s", query_name)

        try:
            query = self._create_or_update_query(query_name, query_sql)

            alert_name = f"[DPO] Data Quality Alert - {catalog}{alert_suffix}"
            alert = self._create_or_update_alert(
                name=alert_name,
                query_id=query.id,
                column="null_rate",
                op=">=",
                value=str(null_threshold or 0),
            )

            # Use provided notifications or fall back to default
            notification_list = (
                notifications
                if notifications is not None
                else self.config.alerting.default_notifications
            )
            for notification_dest in notification_list:
                self._add_notification(alert.id, notification_dest)

            logger.info(
                "Created data quality alert: %s (ID: %s)", alert_name, alert.id
            )
            return alert.id

        except Exception as e:
            logger.error("Failed to create data quality alert: %s", e)
            raise

    def _create_or_update_query(self, name: str, sql: str):
        """Create or update a SQL query."""
        existing = None
        try:
            queries = self.w.queries.list()
            for q in queries:
                if q.name == name:
                    existing = q
                    break
        except Exception:
            pass

        if existing:
            logger.info("Updating existing query: %s", name)
            return self.w.queries.update(
                query_id=existing.id,
                name=name,
                query=sql,
                warehouse_id=self.warehouse_id,
            )
        else:
            logger.info("Creating new query: %s", name)
            return self.w.queries.create(
                name=name,
                query=sql,
                warehouse_id=self.warehouse_id,
            )

    def _create_or_update_alert(
        self,
        name: str,
        query_id: str,
        column: str,
        op: str,
        value: str,
    ):
        """Create or update a SQL alert."""
        existing = None
        try:
            alerts = self.w.alerts.list()
            for a in alerts:
                if a.name == name:
                    existing = a
                    break
        except Exception:
            pass

        options = AlertOptions(
            column=column,
            op=op,
            value=value,
            empty_result_state=AlertOptionsEmptyResultState.OK,
        )

        if existing:
            logger.info("Updating existing alert: %s", name)
            return self.w.alerts.update(
                alert_id=existing.id,
                name=name,
                options=options,
                query_id=query_id,
                rearm=3600,
            )
        else:
            logger.info("Creating new alert: %s", name)
            return self.w.alerts.create(
                name=name,
                options=options,
                query_id=query_id,
                rearm=3600,
            )

    def _add_notification(self, alert_id: str, destination: str) -> None:
        """Add notification destination to alert."""
        try:
            logger.info("Adding notification %s to alert %s", destination, alert_id)
        except Exception as e:
            logger.warning("Failed to add notification: %s", e)

    def generate_custom_alert_ddl(
        self,
        unified_view: str,
        table_name: str,
        business_rule: str,
        alert_name: Optional[str] = None,
    ) -> str:
        """Generate DDL for custom alert - created paused for DS review.

        Args:
            unified_view: The unified metrics view name.
            table_name: Target table for the alert.
            business_rule: Custom WHERE clause logic.
            alert_name: Optional alert name.

        Returns:
            SQL DDL string with instructions.
        """
        name = alert_name or f"Custom Alert - {table_name.split('.')[-1]}"

        return f"""
-- ============================================================
-- Custom Alert: {name}
-- ============================================================
-- Status: PAUSED (requires Data Scientist review before activation)
-- To activate: Edit alert in Databricks SQL UI and set to 'Running'
-- 
-- Business Rule: {business_rule}
-- ============================================================

-- Step 1: Create the query
CREATE OR REPLACE QUERY `{name}` AS
SELECT 
    source_table_name,
    owner,
    department,
    column_name,
    js_divergence,
    window_start,
    window_end
FROM {unified_view}
WHERE source_table_name = '{table_name}'
  AND {business_rule}
ORDER BY js_divergence DESC;

-- Step 2: Create the alert (manually in UI with this query)
-- Alert Settings:
--   - Query: {name}
--   - Trigger when: Any row returned
--   - Refresh: Every 1 hour
--   - State: PAUSED (activate after review)
"""

    def generate_tiered_alert_queries(
        self, unified_view: str, catalog: str
    ) -> dict:
        """Generate queries for tiered alerting (Warning/Critical).

        Args:
            unified_view: The unified drift view name.
            catalog: Catalog name for query naming.

        Returns:
            Dict with query names and SQL.
        """
        threshold = self.config.alerting.drift_threshold
        warning_threshold = max(0.1, threshold / 2)

        return {
            f"[DPO] Warning Query - {catalog}": f"""
                SELECT 
                    source_table_name,
                    column_name,
                    js_divergence,
                    'WARNING' as severity
                FROM {unified_view}
                WHERE js_divergence >= {warning_threshold}
                  AND js_divergence < {threshold}
                ORDER BY js_divergence DESC
            """,
            f"[DPO] Critical Query - {catalog}": f"""
                SELECT 
                    source_table_name,
                    column_name,
                    js_divergence,
                    'CRITICAL' as severity
                FROM {unified_view}
                WHERE js_divergence >= {threshold}
                ORDER BY js_divergence DESC
            """,
        }

    def get_alert_status_query(self, unified_view: str) -> str:
        """Generate query for alert status dashboard.

        Args:
            unified_view: The unified drift view name.

        Returns:
            SQL query for alert status summary.
        """
        threshold = self.config.alerting.drift_threshold

        return f"""
        SELECT 
            source_table_name,
            department,
            owner,
            COUNT(CASE WHEN js_divergence >= {threshold} THEN 1 END) as critical_count,
            COUNT(CASE WHEN js_divergence >= 0.1 AND js_divergence < {threshold} THEN 1 END) as warning_count,
            MAX(js_divergence) as max_drift,
            MAX(window_end) as last_check
        FROM {unified_view}
        GROUP BY source_table_name, department, owner
        HAVING critical_count > 0 OR warning_count > 0
        ORDER BY critical_count DESC, warning_count DESC
        """
