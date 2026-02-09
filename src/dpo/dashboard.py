"""
Phase 5: Dashboard Provisioning

Auto-deploys Lakeview dashboard for global health visibility.
Supports per-group dashboard deployment and cleanup.
"""

import json
import logging
from typing import Dict, List, Optional, Set, Tuple

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

from dpo.config import OrchestratorConfig

logger = logging.getLogger(__name__)


# Lakeview Dashboard JSON template for Global Health view
GLOBAL_HEALTH_DASHBOARD_TEMPLATE = {
    "displayName": "DPO Global Health Dashboard",
    "pages": [
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
                                        {"name": "critical_alerts", "expression": "COUNT(CASE WHEN `js_divergence` >= 0.2 THEN 1 END)"},
                                        {"name": "warning_alerts", "expression": "COUNT(CASE WHEN `js_divergence` >= 0.1 AND `js_divergence` < 0.2 THEN 1 END)"},
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
        {
            "name": "details",
            "displayName": "Detailed Analysis",
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
    ],
    "datasets": [
        {
            "name": "unified_drift",
            "displayName": "Unified Drift Metrics",
            "query": "SELECT * FROM {unified_view}",
        }
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
    """

    def __init__(self, workspace_client: WorkspaceClient, config: OrchestratorConfig):
        self.w = workspace_client
        self.config = config
        self.catalog = config.catalog_name

    def deploy_dashboard(
        self,
        unified_view: str,
        parent_path: str,
        dashboard_name: Optional[str] = None,
    ) -> str:
        """
        Deploy Lakeview dashboard pointing to the unified view.

        Args:
            unified_view: Full name of unified drift metrics view
            parent_path: Workspace path for dashboard (e.g., "/Workspace/Shared/DPO")
            dashboard_name: Optional custom dashboard name

        Returns:
            Dashboard ID
        """
        name = dashboard_name or f"DPO Global Health - {self.catalog}"

        # Prepare template with actual view name
        template = json.loads(json.dumps(GLOBAL_HEALTH_DASHBOARD_TEMPLATE))
        template["displayName"] = name

        # Update dataset query with actual view
        for dataset in template.get("datasets", []):
            if dataset.get("name") == "unified_drift":
                dataset["query"] = f"SELECT * FROM {unified_view}"

        logger.info(f"Deploying dashboard: {name} to {parent_path}")

        try:
            # Check for existing dashboard with same name
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
    ) -> Dict[str, str]:
        """Deploy a dashboard for each monitor group.

        Args:
            views_by_group: Dict mapping group_name -> (drift_view, profile_view).
            parent_path: Workspace path for dashboards.

        Returns:
            Dict mapping group_name -> dashboard_id
        """
        results = {}
        for group_name, (drift_view, _) in views_by_group.items():
            dashboard_id = self.deploy_dashboard(
                drift_view,
                parent_path,
                dashboard_name=f"DPO Health - {group_name}",
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

        Args:
            parent_path: Folder path containing DPO dashboards.
            active_group_names: Set of currently active group names (original, not sanitized).

        Returns:
            List of deleted dashboard names.
        """
        deleted = []
        dpo_prefix = "DPO Health - "

        try:
            # List all dashboards in the parent path
            dashboards = self.w.lakeview.list(path=parent_path)

            for dashboard in dashboards:
                name = dashboard.display_name or ""

                # Check if it's a DPO dashboard
                if name.startswith(dpo_prefix):
                    group_name = name[len(dpo_prefix) :]

                    # If group is not in active groups, delete
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
