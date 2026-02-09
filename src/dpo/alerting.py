"""
Phase 4: Observability and Alerting

Provisions SQL Alerts for drift detection and data quality issues.
Supports per-group notification routing via email or notification destination.
"""

import logging
from typing import Dict, List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    AlertEvaluationState,
    AlertV2,
    AlertV2Evaluation,
    AlertV2Notification,
    AlertV2Operand,
    AlertV2OperandColumn,
    AlertV2OperandValue,
    AlertV2Subscription,
    ComparisonOperator,
    CronSchedule,
    SchedulePauseStatus,
)

from dpo.config import OrchestratorConfig

logger = logging.getLogger(__name__)


class AlertProvisioner:
    """Provisions SQL Alerts for drift detection and data quality.

    Features:
    - Single unified drift alert on aggregated view
    - Data quality alerts (null rate, row count, cardinality)
    - Tiered thresholds (Warning/Critical) in query logic
    - Per-group notification routing (email and destination ID)
    - Custom DDL generation for complex business rules
    """

    COMPARISON_OP_MAP = {
        ">=": ComparisonOperator.GREATER_THAN_OR_EQUAL,
        ">": ComparisonOperator.GREATER_THAN,
        "<=": ComparisonOperator.LESS_THAN_OR_EQUAL,
        "<": ComparisonOperator.LESS_THAN,
        "==": ComparisonOperator.EQUAL,
        "!=": ComparisonOperator.NOT_EQUAL,
    }

    def __init__(
        self, workspace_client: WorkspaceClient, config: OrchestratorConfig
    ):
        self.w = workspace_client
        self.config = config
        self.warehouse_id = config.warehouse_id
        self._destination_cache: Optional[Dict[str, str]] = None

    def _get_notifications_for_group(self, group_name: str) -> List[str]:
        """Get notification destinations for a specific group.

        Args:
            group_name: The monitor group name.

        Returns:
            List of notification destinations (emails, webhook names, destination IDs).
            Falls back to default_notifications if group has no specific routing.
        """
        group_notifications = self.config.alerting.group_notifications
        if group_name in group_notifications:
            return group_notifications[group_name]
        return self.config.alerting.default_notifications

    # ------------------------------------------------------------------
    # Subscription resolution
    # ------------------------------------------------------------------

    def _resolve_subscriptions(
        self, destinations: List[str]
    ) -> List[AlertV2Subscription]:
        """Convert config notification strings to SDK subscription objects.

        Supports two formats:
        - Email addresses (contains '@') -> AlertV2Subscription(user_email=...)
        - Notification destination display names or IDs -> AlertV2Subscription(destination_id=...)

        Args:
            destinations: List of notification destination strings from config.

        Returns:
            List of AlertV2Subscription objects.
        """
        subscriptions = []
        for dest in destinations:
            if "@" in dest:
                subscriptions.append(AlertV2Subscription(user_email=dest))
            else:
                dest_id = self._resolve_destination_id(dest)
                if dest_id:
                    subscriptions.append(AlertV2Subscription(destination_id=dest_id))
                else:
                    logger.warning(
                        "Notification destination '%s' not found — skipping. "
                        "Create it in Workspace Settings > Notification Destinations.",
                        dest,
                    )
        return subscriptions

    def _resolve_destination_id(self, name_or_id: str) -> Optional[str]:
        """Resolve a notification destination display name to its ID.

        Caches the destination list on first call to avoid repeated API calls.

        Args:
            name_or_id: Display name or raw ID of the notification destination.

        Returns:
            The destination ID, or the input as-is if it looks like a UUID.
        """
        # If it looks like a UUID, return as-is
        if len(name_or_id) == 36 and name_or_id.count("-") == 4:
            return name_or_id

        if self._destination_cache is None:
            try:
                cache = {}
                for dest in self.w.notification_destinations.list():
                    if dest.display_name and dest.id:
                        cache[dest.display_name] = dest.id
                self._destination_cache = cache
            except Exception as e:
                logger.warning("Failed to list notification destinations: %s", e)
                return None

        return self._destination_cache.get(name_or_id)

    # ------------------------------------------------------------------
    # Public alert creation
    # ------------------------------------------------------------------

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

        alert_name = f"[DPO] Drift Alert - {catalog}{alert_suffix}"
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

        notification_list = (
            notifications
            if notifications is not None
            else self.config.alerting.default_notifications
        )

        try:
            alert = self._create_or_update_alert(
                name=alert_name,
                query_sql=query_sql,
                column="js_divergence",
                op=">=",
                value=str(warning_threshold),
                notifications=notification_list,
            )

            logger.info("Created unified drift alert: %s (ID: %s)", alert_name, alert.id)
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

        alert_name = f"[DPO] Data Quality Alert - {catalog}{alert_suffix}"
        query_sql = f"""
        SELECT
            source_table_name,
            owner,
            department,
            column_name,
            null_rate,
            record_count,
            distinct_count,
            CAST(1 AS DOUBLE) as trigger_value,
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

        notification_list = (
            notifications
            if notifications is not None
            else self.config.alerting.default_notifications
        )

        try:
            alert = self._create_or_update_alert(
                name=alert_name,
                query_sql=query_sql,
                column="trigger_value",
                op=">=",
                value="1",
                notifications=notification_list,
            )

            logger.info("Created data quality alert: %s (ID: %s)", alert_name, alert.id)
            return alert.id

        except Exception as e:
            logger.error("Failed to create data quality alert: %s", e)
            raise

    # ------------------------------------------------------------------
    # V2 Alert CRUD
    # ------------------------------------------------------------------

    def _create_or_update_alert(
        self,
        name: str,
        query_sql: str,
        column: str,
        op: str,
        value: str,
        notifications: Optional[List[str]] = None,
    ) -> AlertV2:
        """Create or update an SQL alert with embedded query and notifications.

        Args:
            name: Display name for the alert.
            query_sql: SQL query text to embed in the alert.
            column: Column name used for the evaluation condition.
            op: Comparison operator string (e.g., ">=").
            value: Threshold value for the condition.
            notifications: List of email addresses or destination names/IDs.

        Returns:
            The created or updated AlertV2 object.
        """
        existing = self._find_existing_alert(name)
        subscriptions = self._resolve_subscriptions(notifications or [])

        notification_config = AlertV2Notification(
            notify_on_ok=False,
            retrigger_seconds=3600,
            subscriptions=subscriptions if subscriptions else None,
        )

        evaluation = AlertV2Evaluation(
            source=AlertV2OperandColumn(name=column),
            comparison_operator=self.COMPARISON_OP_MAP.get(
                op, ComparisonOperator.GREATER_THAN_OR_EQUAL
            ),
            threshold=AlertV2Operand(
                value=AlertV2OperandValue(double_value=float(value)),
            ),
            empty_result_state=AlertEvaluationState.OK,
            notification=notification_config,
        )

        schedule = CronSchedule(
            quartz_cron_schedule=self.config.alerting.alert_cron_schedule,
            timezone_id=self.config.alerting.alert_timezone,
            pause_status=SchedulePauseStatus.UNPAUSED,
        )

        alert_v2 = AlertV2(
            display_name=name,
            query_text=query_sql,
            warehouse_id=self.warehouse_id,
            evaluation=evaluation,
            schedule=schedule,
        )

        if existing:
            logger.info("Updating existing alert: %s", name)
            return self.w.alerts_v2.update_alert(
                id=existing.id,
                alert=alert_v2,
                update_mask="display_name,query_text,warehouse_id,evaluation,schedule",
            )
        else:
            logger.info("Creating new alert: %s", name)
            return self.w.alerts_v2.create_alert(alert=alert_v2)

    def _find_existing_alert(self, name: str) -> Optional[AlertV2]:
        """Find an existing alert by display name."""
        try:
            for alert in self.w.alerts_v2.list_alerts():
                if alert.display_name == name:
                    return alert
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # DDL / Query generators (unchanged)
    # ------------------------------------------------------------------

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
        warning_threshold = max(0.1, threshold / 2)

        return f"""
        SELECT
            source_table_name,
            department,
            owner,
            COUNT(CASE WHEN js_divergence >= {threshold} THEN 1 END) as critical_count,
            COUNT(CASE WHEN js_divergence >= {warning_threshold} AND js_divergence < {threshold} THEN 1 END) as warning_count,
            MAX(js_divergence) as max_drift,
            MAX(window_end) as last_check
        FROM {unified_view}
        GROUP BY source_table_name, department, owner
        HAVING critical_count > 0 OR warning_count > 0
        ORDER BY critical_count DESC, warning_count DESC
        """
