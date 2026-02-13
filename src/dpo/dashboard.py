"""Dashboard Provisioning.

Auto-deploys Lakeview dashboard for global health visibility.
Supports per-group dashboard deployment and cleanup.
"""

import json
import logging
from typing import Dict, List, Optional, Set, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

from dpo.config import OrchestratorConfig
from dpo.coverage import CoverageReport

logger = logging.getLogger(__name__)


def _sql_value(value: Optional[object]) -> str:
    """Return a SQL literal for VALUES clauses."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _build_coverage_queries(coverage_report: Optional[CoverageReport]) -> Dict[str, str]:
    """Build SQL queries for coverage dashboard datasets."""
    report = coverage_report or CoverageReport()
    snapshot_timestamp = _sql_value(report.timestamp or None)

    summary_query = f"""
        SELECT
            {snapshot_timestamp} AS snapshot_timestamp_utc,
            {report.total_catalog_tables} AS total_catalog_tables,
            {report.total_monitored} AS total_monitored,
            {len(report.unmonitored)} AS unmonitored_tables,
            {len(report.stale)} AS stale_monitors,
            {len(report.orphans)} AS orphan_monitors
    """

    def values_or_empty(
        rows: List[Tuple[Optional[object], ...]],
        columns: List[str],
        empty_select: str,
    ) -> str:
        if not rows:
            return empty_select
        values_sql = ", ".join(
            "(" + ", ".join(_sql_value(v) for v in row) + ")"
            for row in rows
        )
        return f"SELECT * FROM VALUES {values_sql} AS t({', '.join(columns)})"

    unmonitored_rows = [
        (u.full_name, u.schema_name, u.owner, u.reason)
        for u in report.unmonitored
    ]
    unmonitored_query = values_or_empty(
        unmonitored_rows,
        ["source_table_name", "schema_name", "owner", "reason"],
        """
        SELECT
            CAST(NULL AS STRING) AS source_table_name,
            CAST(NULL AS STRING) AS schema_name,
            CAST(NULL AS STRING) AS owner,
            CAST(NULL AS STRING) AS reason
        WHERE 1=0
        """,
    )

    stale_rows = [
        (s.table_name, s.monitor_id, s.days_since_refresh, s.status)
        for s in report.stale
    ]
    stale_query = values_or_empty(
        stale_rows,
        ["source_table_name", "monitor_id", "days_since_refresh", "status"],
        """
        SELECT
            CAST(NULL AS STRING) AS source_table_name,
            CAST(NULL AS STRING) AS monitor_id,
            CAST(NULL AS INT) AS days_since_refresh,
            CAST(NULL AS STRING) AS status
        WHERE 1=0
        """,
    )

    orphan_rows = [
        (o.table_name, o.monitor_id, o.reason)
        for o in report.orphans
    ]
    orphan_query = values_or_empty(
        orphan_rows,
        ["source_table_name", "monitor_id", "reason"],
        """
        SELECT
            CAST(NULL AS STRING) AS source_table_name,
            CAST(NULL AS STRING) AS monitor_id,
            CAST(NULL AS STRING) AS reason
        WHERE 1=0
        """,
    )

    return {
        "coverage_summary": summary_query,
        "coverage_unmonitored": unmonitored_query,
        "coverage_stale": stale_query,
        "coverage_orphans": orphan_query,
    }


def _build_dashboard_template(
    drift_threshold: float,
    null_rate_threshold: Optional[float],
    row_count_min: Optional[int],
) -> dict:
    """Build the Lakeview dashboard JSON template with configurable thresholds.

    Args:
        drift_threshold: JS divergence threshold for CRITICAL alerts.
        null_rate_threshold: Null rate threshold for data quality panels.
        row_count_min: Minimum row count threshold for data quality panels.
    """
    pages = [
        # --- Page 1: Drift Overview ---
        {
            "name": "overview",
            "displayName": "Drift Overview",
            "layout": [
                {
                    "widget": {
                        "name": "summary_stats",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_drift",
                                    "fields": [
                                        {"name": "total_tables", "expression": "COUNT(DISTINCT `source_table_name`)"},
                                        {
                                            "name": "critical_alerts",
                                            "expression": (
                                                f"COUNT(CASE WHEN `js_divergence` >= COALESCE(`drift_threshold`, {drift_threshold}) THEN 1 END)"
                                            ),
                                        },
                                        {
                                            "name": "warning_alerts",
                                            "expression": (
                                                f"COUNT(CASE WHEN `js_divergence` >= LEAST(COALESCE(`drift_threshold`, {drift_threshold}), GREATEST(0.1, COALESCE(`drift_threshold`, {drift_threshold}) / 2.0)) "
                                                f"AND `js_divergence` < COALESCE(`drift_threshold`, {drift_threshold}) THEN 1 END)"
                                            ),
                                        },
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "type": "counter",
                            "encodings": {"value": {"fieldName": "total_tables", "displayName": "Monitored Tables"}},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 1},
                },
                {
                    "widget": {
                        "name": "drift_trend",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_drift",
                                    "fields": [
                                        {"name": "window_end", "expression": "`window_end`"},
                                        {"name": "avg_drift", "expression": "AVG(`js_divergence`)"},
                                        {"name": "max_drift", "expression": "MAX(`js_divergence`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "type": "line",
                            "encodings": {
                                "x": {"fieldName": "window_end", "scale": {"type": "temporal"}},
                                "y": {"fieldName": "avg_drift", "scale": {"type": "quantitative"}},
                            },
                        },
                    },
                    "position": {"x": 2, "y": 0, "width": 4, "height": 2},
                },
                {
                    "widget": {
                        "name": "top_drifters",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_drift",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "department", "expression": "`department`"},
                                        {"name": "owner", "expression": "`owner`"},
                                        {"name": "max_drift", "expression": "MAX(`js_divergence`)"},
                                        {"name": "drift_count", "expression": "COUNT(*)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {"type": "table", "title": "Wall of Shame - Top Drifters"},
                    },
                    "position": {"x": 0, "y": 2, "width": 6, "height": 3},
                },
                {
                    "widget": {
                        "name": "department_breakdown",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_drift",
                                    "fields": [
                                        {"name": "department", "expression": "`department`"},
                                        {"name": "avg_drift", "expression": "AVG(`js_divergence`)"},
                                        {"name": "table_count", "expression": "COUNT(DISTINCT `source_table_name`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "type": "bar",
                            "encodings": {
                                "x": {"fieldName": "department", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "avg_drift", "scale": {"type": "quantitative"}},
                            },
                        },
                    },
                    "position": {"x": 0, "y": 5, "width": 3, "height": 2},
                },
                {
                    "widget": {
                        "name": "feature_drift_heatmap",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_drift",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "column_name", "expression": "`column_name`"},
                                        {"name": "js_divergence", "expression": "MAX(`js_divergence`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {"type": "heatmap"},
                    },
                    "position": {"x": 3, "y": 5, "width": 3, "height": 2},
                },
            ],
        },
        # --- Page 2: Data Quality ---
        {
            "name": "data_quality",
            "displayName": "Data Quality",
            "layout": [
                {
                    "widget": {
                        "name": "quality_summary",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_profile",
                                    "fields": [
                                        {"name": "tables_profiled", "expression": "COUNT(DISTINCT `source_table_name`)"},
                                        {"name": "high_null_columns", "expression": f"COUNT(CASE WHEN `null_rate` > {null_rate_threshold or 1.0} THEN 1 END)"},
                                        {"name": "low_row_tables", "expression": f"COUNT(DISTINCT CASE WHEN `record_count` < {row_count_min or 0} THEN `source_table_name` END)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "type": "counter",
                            "encodings": {"value": {"fieldName": "tables_profiled", "displayName": "Tables Profiled"}},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 1},
                },
                {
                    "widget": {
                        "name": "null_rate_trend",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_profile",
                                    "fields": [
                                        {"name": "window_end", "expression": "`window_end`"},
                                        {"name": "avg_null_rate", "expression": "AVG(`null_rate`)"},
                                        {"name": "max_null_rate", "expression": "MAX(`null_rate`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "type": "line",
                            "encodings": {
                                "x": {"fieldName": "window_end", "scale": {"type": "temporal"}},
                                "y": {"fieldName": "avg_null_rate", "scale": {"type": "quantitative"}},
                            },
                        },
                    },
                    "position": {"x": 2, "y": 0, "width": 4, "height": 2},
                },
                {
                    "widget": {
                        "name": "row_count_by_table",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_profile",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "latest_record_count", "expression": "MAX(`record_count`)"},
                                        {"name": "latest_distinct_count", "expression": "MAX(`distinct_count`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "type": "bar",
                            "encodings": {
                                "x": {"fieldName": "source_table_name", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "latest_record_count", "scale": {"type": "quantitative"}},
                            },
                        },
                    },
                    "position": {"x": 0, "y": 2, "width": 3, "height": 2},
                },
                {
                    "widget": {
                        "name": "null_rate_by_column",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_profile",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "column_name", "expression": "`column_name`"},
                                        {"name": "null_rate", "expression": "MAX(`null_rate`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {"type": "heatmap", "title": "Null Rate by Column"},
                    },
                    "position": {"x": 3, "y": 2, "width": 3, "height": 2},
                },
                {
                    "widget": {
                        "name": "quality_details_table",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_profile",
                                    "fields": [
                                        {"name": "window_end", "expression": "`window_end`"},
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "column_name", "expression": "`column_name`"},
                                        {"name": "null_rate", "expression": "`null_rate`"},
                                        {"name": "record_count", "expression": "`record_count`"},
                                        {"name": "distinct_count", "expression": "`distinct_count`"},
                                        {"name": "owner", "expression": "`owner`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {"type": "table"},
                    },
                    "position": {"x": 0, "y": 4, "width": 6, "height": 3},
                },
            ],
        },
        # --- Page 3: Detailed Drift Analysis ---
        {
            "name": "details",
            "displayName": "Detailed Drift Analysis",
            "layout": [
                {
                    "widget": {
                        "name": "drift_details_table",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "unified_drift",
                                    "fields": [
                                        {"name": "window_end", "expression": "`window_end`"},
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "column_name", "expression": "`column_name`"},
                                        {"name": "js_divergence", "expression": "`js_divergence`"},
                                        {"name": "ks_statistic", "expression": "`ks_statistic`"},
                                        {"name": "wasserstein_distance", "expression": "`wasserstein_distance`"},
                                        {"name": "drift_type", "expression": "`drift_type`"},
                                        {"name": "owner", "expression": "`owner`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {"type": "table"},
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 5},
                }
            ],
        },
    ]

    # --- Page 4: Coverage Governance ---
    pages.append(
        {
            "name": "coverage",
            "displayName": "Coverage Governance",
            "layout": [
                {
                    "widget": {
                        "name": "coverage_summary",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "coverage_summary",
                                    "fields": [
                                        {"name": "snapshot_timestamp_utc", "expression": "`snapshot_timestamp_utc`"},
                                        {"name": "total_catalog_tables", "expression": "`total_catalog_tables`"},
                                        {"name": "total_monitored", "expression": "`total_monitored`"},
                                        {"name": "unmonitored_tables", "expression": "`unmonitored_tables`"},
                                        {"name": "stale_monitors", "expression": "`stale_monitors`"},
                                        {"name": "orphan_monitors", "expression": "`orphan_monitors`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {"type": "table", "title": "Coverage Summary"},
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 1},
                },
                {
                    "widget": {
                        "name": "unmonitored_tables",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "coverage_unmonitored",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "schema_name", "expression": "`schema_name`"},
                                        {"name": "owner", "expression": "`owner`"},
                                        {"name": "reason", "expression": "`reason`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {"type": "table", "title": "Unmonitored Tables"},
                    },
                    "position": {"x": 0, "y": 1, "width": 6, "height": 2},
                },
                {
                    "widget": {
                        "name": "stale_monitors",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "coverage_stale",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "monitor_id", "expression": "`monitor_id`"},
                                        {"name": "days_since_refresh", "expression": "`days_since_refresh`"},
                                        {"name": "status", "expression": "`status`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {"type": "table", "title": "Stale Monitors"},
                    },
                    "position": {"x": 0, "y": 3, "width": 6, "height": 2},
                },
                {
                    "widget": {
                        "name": "orphan_monitors",
                        "queries": [
                            {
                                "query": {
                                    "datasetName": "coverage_orphans",
                                    "fields": [
                                        {"name": "source_table_name", "expression": "`source_table_name`"},
                                        {"name": "monitor_id", "expression": "`monitor_id`"},
                                        {"name": "reason", "expression": "`reason`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {"type": "table", "title": "Orphan Monitors"},
                    },
                    "position": {"x": 0, "y": 5, "width": 6, "height": 2},
                },
            ],
        }
    )

    return {
        "displayName": "DPO Global Health Dashboard",
        "pages": pages,
        "datasets": [
            {
                "name": "unified_drift",
                "displayName": "Unified Drift Metrics",
                "query": "SELECT * FROM {unified_drift_view}",
            },
            {
                "name": "unified_profile",
                "displayName": "Unified Profile Metrics",
                "query": "SELECT * FROM {unified_profile_view}",
            },
            {
                "name": "coverage_summary",
                "displayName": "Coverage Summary",
                "query": "{coverage_summary_query}",
            },
            {
                "name": "coverage_unmonitored",
                "displayName": "Coverage Unmonitored",
                "query": "{coverage_unmonitored_query}",
            },
            {
                "name": "coverage_stale",
                "displayName": "Coverage Stale Monitors",
                "query": "{coverage_stale_query}",
            },
            {
                "name": "coverage_orphans",
                "displayName": "Coverage Orphan Monitors",
                "query": "{coverage_orphan_query}",
            },
        ],
    }


class DashboardProvisioner:
    """
    Provisions Lakeview dashboards for model monitoring.

    Features:
    - Global health dashboard template
    - "Wall of Shame" top drifters view
    - Department breakdown
    - Feature drift heatmap
    - Data quality panels (null rates, row counts, cardinality)
    """

    def __init__(self, workspace_client: WorkspaceClient, config: OrchestratorConfig):
        self.w = workspace_client
        self.config = config
        self.catalog = config.catalog_name

    def deploy_dashboard(
        self,
        unified_drift_view: str,
        parent_path: str,
        unified_profile_view: Optional[str] = None,
        dashboard_name: Optional[str] = None,
        coverage_report: Optional[CoverageReport] = None,
    ) -> str:
        """
        Deploy Lakeview dashboard pointing to the unified views.

        Args:
            unified_drift_view: Full name of unified drift metrics view
            parent_path: Workspace path for dashboard (e.g., "/Workspace/Shared/DPO")
            unified_profile_view: Full name of unified profile metrics view
            dashboard_name: Optional custom dashboard name
            coverage_report: Optional coverage governance report snapshot

        Returns:
            Dashboard ID
        """
        name = dashboard_name or f"DPO Global Health - {self.catalog}"

        threshold = self.config.alerting.drift_threshold

        template = _build_dashboard_template(
            drift_threshold=threshold,
            null_rate_threshold=self.config.alerting.null_rate_threshold,
            row_count_min=self.config.alerting.row_count_min,
        )
        template["displayName"] = name

        # Inject actual view names into dataset queries
        profile_view = unified_profile_view or unified_drift_view.replace(
            "_drift", "_profile"
        )
        coverage_queries = _build_coverage_queries(coverage_report)
        for dataset in template.get("datasets", []):
            if dataset.get("name") == "unified_drift":
                dataset["query"] = f"SELECT * FROM {unified_drift_view}"
            elif dataset.get("name") == "unified_profile":
                dataset["query"] = f"SELECT * FROM {profile_view}"
            elif dataset.get("name") == "coverage_summary":
                dataset["query"] = coverage_queries["coverage_summary"]
            elif dataset.get("name") == "coverage_unmonitored":
                dataset["query"] = coverage_queries["coverage_unmonitored"]
            elif dataset.get("name") == "coverage_stale":
                dataset["query"] = coverage_queries["coverage_stale"]
            elif dataset.get("name") == "coverage_orphans":
                dataset["query"] = coverage_queries["coverage_orphans"]

        logger.info(f"Deploying dashboard: {name} to {parent_path}")

        try:
            existing = self._find_existing_dashboard(name, parent_path)

            if existing:
                logger.info(f"Updating existing dashboard: {existing.dashboard_id}")
                dashboard = self.w.lakeview.update(
                    dashboard_id=existing.dashboard_id,
                    dashboard=Dashboard(
                        display_name=name,
                        serialized_dashboard=json.dumps(template),
                    ),
                )
            else:
                dashboard = self.w.lakeview.create(
                    dashboard=Dashboard(
                        display_name=name,
                        parent_path=parent_path,
                        serialized_dashboard=json.dumps(template),
                    ),
                )

            dashboard_id = dashboard.dashboard_id
            logger.info(f"Dashboard deployed successfully: {dashboard_id}")
            return dashboard_id

        except Exception as e:
            logger.error(f"Failed to deploy dashboard: {e}")
            raise

    def _find_existing_dashboard(self, name: str, parent_path: str):
        """Find existing dashboard with same name."""
        try:
            dashboards = self.w.lakeview.list(path=parent_path)
            for d in dashboards:
                if d.display_name == name:
                    return d
        except Exception:
            pass
        return None

    def get_dashboard_url(self, dashboard_id: str) -> str:
        """Get the URL for a dashboard."""
        try:
            host = self.w.config.host
            return f"{host}/dashboards/{dashboard_id}"
        except Exception:
            return f"/dashboards/{dashboard_id}"

    def deploy_dashboards_by_group(
        self,
        views_by_group: Dict[str, Tuple[str, str]],
        parent_path: str,
        coverage_report: Optional[CoverageReport] = None,
    ) -> Dict[str, str]:
        """Deploy a dashboard for each monitor group.

        Args:
            views_by_group: Dict mapping group_name -> (drift_view, profile_view).
            parent_path: Workspace path for dashboards.

        Returns:
            Dict mapping group_name -> dashboard_id
        """
        results = {}
        for group_name, (drift_view, profile_view) in views_by_group.items():
            dashboard_id = self.deploy_dashboard(
                drift_view,
                parent_path,
                unified_profile_view=profile_view,
                dashboard_name=f"DPO Health - {group_name}",
                coverage_report=coverage_report,
            )
            results[group_name] = dashboard_id

        return results

    def cleanup_stale_dashboards(
        self,
        parent_path: str,
        active_group_names: Set[str],
    ) -> List[str]:
        """Remove dashboards that no longer correspond to active groups.

        Scenario: User renames monitor_group "marketing" to "growth".
        Result: "DPO Health - marketing" dashboard becomes stale.

        Safety: Skips cleanup when active_group_names is empty to prevent
        accidental deletion of all dashboards during transient failures.

        Args:
            parent_path: Folder path containing DPO dashboards.
            active_group_names: Set of currently active group names (original, not sanitized).

        Returns:
            List of deleted dashboard names.
        """
        deleted = []
        dpo_prefix = "DPO Health - "

        if not active_group_names:
            logger.warning("No active groups; skipping dashboard cleanup to prevent accidental deletion")
            return deleted

        try:
            dashboards = self.w.lakeview.list(path=parent_path)

            for dashboard in dashboards:
                name = dashboard.display_name or ""

                if name.startswith(dpo_prefix):
                    group_name = name[len(dpo_prefix) :]

                    if group_name not in active_group_names:
                        try:
                            self.w.lakeview.trash(dashboard.dashboard_id)
                            deleted.append(name)
                            logger.info(f"Deleted stale dashboard: {name}")
                        except Exception as e:
                            logger.warning(
                                f"Failed to delete stale dashboard {name}: {e}"
                            )

        except Exception as e:
            logger.warning(f"Failed to list dashboards for cleanup: {e}")

        return deleted

    def generate_custom_dashboard_template(
        self,
        unified_view: str,
        table_name: str,
        dashboard_name: str,
    ) -> dict:
        """Generate a custom dashboard template for a specific table.

        Returns dashboard JSON that can be manually imported.
        """
        return {
            "displayName": dashboard_name,
            "pages": [
                {
                    "name": "main",
                    "displayName": f"Drift Analysis - {table_name.split('.')[-1]}",
                    "layout": [
                        {
                            "widget": {
                                "name": "drift_timeline",
                                "queries": [
                                    {
                                        "query": {
                                            "datasetName": "table_drift",
                                            "fields": [
                                                {
                                                    "name": "window_end",
                                                    "expression": "`window_end`",
                                                },
                                                {
                                                    "name": "column_name",
                                                    "expression": "`column_name`",
                                                },
                                                {
                                                    "name": "js_divergence",
                                                    "expression": "`js_divergence`",
                                                },
                                            ],
                                            "disaggregated": True,
                                        }
                                    }
                                ],
                                "spec": {
                                    "type": "line",
                                    "encodings": {
                                        "x": {"fieldName": "window_end"},
                                        "y": {"fieldName": "js_divergence"},
                                        "color": {"fieldName": "column_name"},
                                    },
                                },
                            }
                        }
                    ],
                }
            ],
            "datasets": [
                {
                    "name": "table_drift",
                    "displayName": f"Drift Metrics - {table_name.split('.')[-1]}",
                    "query": f"SELECT * FROM {unified_view} WHERE source_table_name = '{table_name}'",
                }
            ],
        }
