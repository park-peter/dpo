"""Dashboard Provisioning.

Auto-deploys Lakeview dashboard for global health visibility.
Supports per-group dashboard deployment and cleanup.
"""

import json
import logging
from typing import Dict, List, Optional, Set

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.dashboards import Dashboard

from dpo.config import OrchestratorConfig
from dpo.naming import GroupArtifactNames


def _ensure_query_names(template: dict) -> dict:
    """Ensure every query entry in widgets has a 'name' field (required by Lakeview API)."""
    for page in template.get("pages", []):
        for item in page.get("layout", []):
            widget = item.get("widget", {})
            for i, q in enumerate(widget.get("queries", [])):
                if "name" not in q:
                    q["name"] = "main_query" if i == 0 else f"query_{i}"
    return template


def _normalize_template(template: dict) -> dict:
    """Normalize the dashboard template for Lakeview API compatibility.

    - Converts dataset ``query`` (string) to ``queryLines`` (list of strings).
    - Adds ``pageType: PAGE_TYPE_CANVAS`` to pages that lack it.
    """
    for ds in template.get("datasets", []):
        if "query" in ds and "queryLines" not in ds:
            ds["queryLines"] = [ds.pop("query")]
    for page in template.get("pages", []):
        if "pageType" not in page:
            page["pageType"] = "PAGE_TYPE_CANVAS"
    return template

logger = logging.getLogger(__name__)


def _fallback_numeric(value: Optional[float | int], default: float | int) -> float | int:
    """Return the configured numeric value, preserving zeros."""
    return value if value is not None else default


def _empty_unified_performance_query() -> str:
    """Return an empty but schema-compatible unified performance dataset query."""
    return """
        SELECT
            CAST(NULL AS TIMESTAMP) AS window_start,
            CAST(NULL AS TIMESTAMP) AS window_end,
            CAST(NULL AS STRING) AS granularity,
            CAST(NULL AS STRING) AS column_name,
            CAST(NULL AS DOUBLE) AS accuracy_score,
            CAST(NULL AS DOUBLE) AS log_loss,
            CAST(NULL AS DOUBLE) AS precision_weighted,
            CAST(NULL AS DOUBLE) AS recall_weighted,
            CAST(NULL AS DOUBLE) AS f1_weighted,
            CAST(NULL AS DOUBLE) AS roc_auc_weighted,
            CAST(NULL AS DOUBLE) AS predictive_parity,
            CAST(NULL AS DOUBLE) AS predictive_equality,
            CAST(NULL AS DOUBLE) AS equal_opportunity,
            CAST(NULL AS DOUBLE) AS statistical_parity,
            CAST(NULL AS DOUBLE) AS mean_squared_error,
            CAST(NULL AS DOUBLE) AS root_mean_squared_error,
            CAST(NULL AS DOUBLE) AS mean_average_error,
            CAST(NULL AS DOUBLE) AS mean_absolute_percentage_error,
            CAST(NULL AS DOUBLE) AS r2_score,
            CAST(NULL AS STRING) AS slice_key,
            CAST(NULL AS STRING) AS slice_value,
            CAST(NULL AS STRING) AS source_table_name,
            CAST(NULL AS STRING) AS owner
        WHERE 1=0
    """


def _empty_unified_profile_query() -> str:
    """Return an empty but schema-compatible unified profile dataset query."""
    return """
        SELECT
            CAST(NULL AS TIMESTAMP) AS window_start,
            CAST(NULL AS TIMESTAMP) AS window_end,
            CAST(NULL AS STRING) AS column_name,
            CAST(NULL AS BIGINT) AS record_count,
            CAST(NULL AS BIGINT) AS null_count,
            CAST(NULL AS DOUBLE) AS null_rate,
            CAST(NULL AS BIGINT) AS distinct_count,
            CAST(NULL AS DOUBLE) AS mean,
            CAST(NULL AS DOUBLE) AS stddev,
            CAST(NULL AS DOUBLE) AS min_value,
            CAST(NULL AS DOUBLE) AS max_value,
            CAST(NULL AS STRING) AS granularity,
            CAST(NULL AS STRING) AS slice_key,
            CAST(NULL AS STRING) AS slice_value,
            CAST(NULL AS STRING) AS source_table_name,
            CAST(NULL AS STRING) AS owner,
            CAST(NULL AS STRING) AS department,
            CAST(NULL AS INT) AS priority,
            CAST(NULL AS STRING) AS runbook_url,
            CAST(NULL AS STRING) AS lineage_url
        WHERE 1=0
    """


def _empty_perf_vs_drift_query() -> str:
    """Return an empty but schema-compatible performance-vs-drift dataset query."""
    return """
        SELECT
            CAST(NULL AS STRING) AS source_table_name,
            CAST(NULL AS TIMESTAMP) AS window_start,
            CAST(NULL AS TIMESTAMP) AS window_end,
            CAST(NULL AS STRING) AS granularity,
            CAST(NULL AS DOUBLE) AS accuracy_score,
            CAST(NULL AS DOUBLE) AS r2_score,
            CAST(NULL AS DOUBLE) AS precision_weighted,
            CAST(NULL AS DOUBLE) AS f1_weighted,
            CAST(NULL AS DOUBLE) AS max_js_distance,
            CAST(NULL AS DOUBLE) AS avg_js_distance,
            CAST(NULL AS BIGINT) AS columns_drifted
        WHERE 1=0
    """


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
                                                f"COUNT(CASE WHEN `js_distance` >= COALESCE(`drift_threshold`, {drift_threshold}) THEN 1 END)"
                                            ),
                                        },
                                        {
                                            "name": "warning_alerts",
                                            "expression": (
                                                f"COUNT(CASE WHEN `js_distance` >= LEAST(COALESCE(`drift_threshold`, {drift_threshold}), GREATEST(0.1, COALESCE(`drift_threshold`, {drift_threshold}) / 2.0)) "
                                                f"AND `js_distance` < COALESCE(`drift_threshold`, {drift_threshold}) THEN 1 END)"
                                            ),
                                        },
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "total_tables", "displayName": "Monitored Tables"}},
                            "frame": {"showTitle": True, "title": "Monitored Tables"},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 3},
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
                                        {"name": "avg_drift", "expression": "AVG(`js_distance`)"},
                                        {"name": "max_drift", "expression": "MAX(`js_distance`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "window_end", "scale": {"type": "temporal"}, "displayName": "Window End"},
                                "y": {"fieldName": "avg_drift", "scale": {"type": "quantitative"}, "displayName": "Avg Drift"},
                            },
                            "frame": {"showTitle": True, "title": "Drift Trend"},
                        },
                    },
                    "position": {"x": 2, "y": 0, "width": 4, "height": 3},
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
                                        {"name": "max_drift", "expression": "MAX(`js_distance`)"},
                                        {"name": "drift_count", "expression": f"COUNT(CASE WHEN `js_distance` >= COALESCE(`drift_threshold`, {drift_threshold}) THEN 1 END)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "source_table_name", "displayName": "Table"},
                                    {"fieldName": "department", "displayName": "Department"},
                                    {"fieldName": "owner", "displayName": "Owner"},
                                    {"fieldName": "max_drift", "displayName": "Max Drift"},
                                    {"fieldName": "drift_count", "displayName": "Drift Count"},
                                ],
                            },
                            "frame": {"showTitle": True, "title": "Wall of Shame - Top Drifters"},
                        },
                    },
                    "position": {"x": 0, "y": 3, "width": 6, "height": 5},
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
                                        {"name": "avg_drift", "expression": "AVG(`js_distance`)"},
                                        {"name": "table_count", "expression": "COUNT(DISTINCT `source_table_name`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "department", "scale": {"type": "categorical"}, "displayName": "Department"},
                                "y": {"fieldName": "avg_drift", "scale": {"type": "quantitative"}, "displayName": "Avg Drift"},
                            },
                            "frame": {"showTitle": True, "title": "Drift by Department"},
                        },
                    },
                    "position": {"x": 0, "y": 8, "width": 3, "height": 5},
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
                                        {"name": "js_distance", "expression": "MAX(`js_distance`)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "column_name", "scale": {"type": "categorical"}, "displayName": "Column"},
                                "y": {"fieldName": "js_distance", "scale": {"type": "quantitative"}, "displayName": "Max Drift"},
                                "color": {"fieldName": "source_table_name", "scale": {"type": "categorical"}, "displayName": "Table"},
                            },
                            "frame": {"showTitle": True, "title": "Feature Drift by Column"},
                        },
                    },
                    "position": {"x": 3, "y": 8, "width": 3, "height": 5},
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
                                        {"name": "high_null_columns", "expression": f"COUNT(CASE WHEN `null_rate` > {_fallback_numeric(null_rate_threshold, 1.0)} THEN 1 END)"},
                                        {"name": "low_row_tables", "expression": f"COUNT(DISTINCT CASE WHEN `record_count` < {_fallback_numeric(row_count_min, 0)} THEN `source_table_name` END)"},
                                    ],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "tables_profiled", "displayName": "Tables Profiled"}},
                            "frame": {"showTitle": True, "title": "Tables Profiled"},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 3},
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
                                    "filters": [{"expression": "`column_name` != ':table'"}],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "window_end", "scale": {"type": "temporal"}, "displayName": "Window End"},
                                "y": {"fieldName": "avg_null_rate", "scale": {"type": "quantitative"}, "displayName": "Avg Null Rate"},
                            },
                            "frame": {"showTitle": True, "title": "Null Rate Trend"},
                        },
                    },
                    "position": {"x": 2, "y": 0, "width": 4, "height": 3},
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
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "source_table_name", "scale": {"type": "categorical"}, "displayName": "Table"},
                                "y": {"fieldName": "latest_record_count", "scale": {"type": "quantitative"}, "displayName": "Record Count"},
                            },
                            "frame": {"showTitle": True, "title": "Row Count by Table"},
                        },
                    },
                    "position": {"x": 0, "y": 3, "width": 3, "height": 5},
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
                                    "filters": [{"expression": "`column_name` != ':table'"}],
                                    "disaggregated": False,
                                }
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "column_name", "scale": {"type": "categorical"}, "displayName": "Column"},
                                "y": {"fieldName": "null_rate", "scale": {"type": "quantitative"}, "displayName": "Max Null Rate"},
                                "color": {"fieldName": "source_table_name", "scale": {"type": "categorical"}, "displayName": "Table"},
                            },
                            "frame": {"showTitle": True, "title": "Null Rate by Column"},
                        },
                    },
                    "position": {"x": 3, "y": 3, "width": 3, "height": 5},
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
                                    "filters": [{"expression": "`column_name` != ':table'"}],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "window_end", "displayName": "Window End"},
                                    {"fieldName": "source_table_name", "displayName": "Table"},
                                    {"fieldName": "column_name", "displayName": "Column"},
                                    {"fieldName": "null_rate", "displayName": "Null Rate"},
                                    {"fieldName": "record_count", "displayName": "Record Count"},
                                    {"fieldName": "distinct_count", "displayName": "Distinct Count"},
                                    {"fieldName": "owner", "displayName": "Owner"},
                                ],
                            },
                            "frame": {"showTitle": True, "title": "Quality Details"},
                        },
                    },
                    "position": {"x": 0, "y": 8, "width": 6, "height": 6},
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
                                        {"name": "js_distance", "expression": "`js_distance`"},
                                        {"name": "ks_statistic", "expression": "`ks_statistic`"},
                                        {"name": "wasserstein_distance", "expression": "`wasserstein_distance`"},
                                        {"name": "drift_type", "expression": "`drift_type`"},
                                        {"name": "owner", "expression": "`owner`"},
                                    ],
                                    "disaggregated": True,
                                }
                            }
                        ],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "window_end", "displayName": "Window End"},
                                    {"fieldName": "source_table_name", "displayName": "Table"},
                                    {"fieldName": "column_name", "displayName": "Column"},
                                    {"fieldName": "js_distance", "displayName": "JS Distance"},
                                    {"fieldName": "ks_statistic", "displayName": "KS Statistic"},
                                    {"fieldName": "wasserstein_distance", "displayName": "Wasserstein Distance"},
                                    {"fieldName": "drift_type", "displayName": "Drift Type"},
                                    {"fieldName": "owner", "displayName": "Owner"},
                                ],
                            },
                            "frame": {"showTitle": True, "title": "Drift Details"},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 8},
                }
            ],
        },
    ]

    # --- Page 4: Model Performance ---
    pages.append(
        {
            "name": "model_performance",
            "displayName": "Model Performance",
            "layout": [
                {
                    "widget": {
                        "name": "perf_summary",
                        "queries": [{
                            "query": {
                                "datasetName": "unified_performance",
                                "fields": [
                                    {"name": "models_monitored", "expression": "COUNT(DISTINCT `source_table_name`)"},
                                    {"name": "models_with_labels", "expression": "COUNT(DISTINCT CASE WHEN `accuracy_score` IS NOT NULL OR `r2_score` IS NOT NULL THEN `source_table_name` END)"},
                                ],
                                "disaggregated": False,
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "models_monitored", "displayName": "Models Monitored"}},
                            "frame": {"showTitle": True, "title": "Models Monitored"},
                        },
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 3},
                },
                {
                    "widget": {
                        "name": "perf_trend",
                        "queries": [{
                            "query": {
                                "datasetName": "unified_performance",
                                "fields": [
                                    {"name": "window_end", "expression": "`window_end`"},
                                    {"name": "source_table_name", "expression": "`source_table_name`"},
                                    {"name": "score", "expression": "COALESCE(`accuracy_score`, `r2_score`)"},
                                ],
                                "disaggregated": False,
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "window_end", "scale": {"type": "temporal"}, "displayName": "Window End"},
                                "y": {"fieldName": "score", "scale": {"type": "quantitative"}, "displayName": "Score"},
                                "color": {"fieldName": "source_table_name", "scale": {"type": "categorical"}, "displayName": "Model"},
                            },
                            "frame": {"showTitle": True, "title": "Performance Trend"},
                        },
                    },
                    "position": {"x": 2, "y": 0, "width": 4, "height": 3},
                },
                {
                    "widget": {
                        "name": "perf_table",
                        "queries": [{
                            "query": {
                                "datasetName": "unified_performance",
                                "fields": [
                                    {"name": "source_table_name", "expression": "`source_table_name`"},
                                    {"name": "accuracy_score", "expression": "MAX(`accuracy_score`)"},
                                    {"name": "r2_score", "expression": "MAX(`r2_score`)"},
                                    {"name": "f1_weighted", "expression": "MAX(`f1_weighted`)"},
                                    {"name": "precision_weighted", "expression": "MAX(`precision_weighted`)"},
                                    {"name": "owner", "expression": "MAX(`owner`)"},
                                ],
                                "disaggregated": False,
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "source_table_name", "displayName": "Model"},
                                    {"fieldName": "accuracy_score", "displayName": "Accuracy"},
                                    {"fieldName": "r2_score", "displayName": "R2"},
                                    {"fieldName": "f1_weighted", "displayName": "F1 Weighted"},
                                    {"fieldName": "precision_weighted", "displayName": "Precision Weighted"},
                                    {"fieldName": "owner", "displayName": "Owner"},
                                ],
                            },
                            "frame": {"showTitle": True, "title": "Model Performance Summary"},
                        },
                    },
                    "position": {"x": 0, "y": 3, "width": 6, "height": 5},
                },
                {
                    "widget": {
                        "name": "perf_vs_drift_scatter",
                        "queries": [{
                            "query": {
                                "datasetName": "perf_vs_drift",
                                "fields": [
                                    {"name": "source_table_name", "expression": "`source_table_name`"},
                                    {"name": "score", "expression": "COALESCE(`accuracy_score`, `r2_score`)"},
                                    {"name": "max_js_distance", "expression": "`max_js_distance`"},
                                ],
                                "disaggregated": True,
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "scatter",
                            "encodings": {
                                "x": {"fieldName": "max_js_distance", "scale": {"type": "quantitative"}, "displayName": "Max JS Distance"},
                                "y": {"fieldName": "score", "scale": {"type": "quantitative"}, "displayName": "Score"},
                                "color": {"fieldName": "source_table_name", "scale": {"type": "categorical"}, "displayName": "Model"},
                            },
                            "frame": {"showTitle": True, "title": "Performance vs Drift"},
                        },
                    },
                    "position": {"x": 0, "y": 8, "width": 6, "height": 5},
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
                "name": "unified_performance",
                "displayName": "Unified Performance Metrics",
                "query": "SELECT * FROM {unified_performance_view}",
            },
            {
                "name": "perf_vs_drift",
                "displayName": "Performance vs Drift",
                "query": """
                    WITH drift_summary AS (
                        SELECT source_table_name, window_start, window_end, granularity,
                            MAX(js_distance) AS max_js_distance,
                            AVG(js_distance) AS avg_js_distance,
                            COUNT(DISTINCT column_name) AS columns_drifted
                        FROM {unified_drift_view}
                        WHERE js_distance IS NOT NULL
                        GROUP BY source_table_name, window_start, window_end, granularity
                    ),
                    perf AS (
                        SELECT source_table_name, window_start, window_end, granularity,
                            accuracy_score, r2_score, precision_weighted, f1_weighted
                        FROM {unified_performance_view}
                    )
                    SELECT p.source_table_name, p.window_start, p.window_end, p.granularity,
                        p.accuracy_score, p.r2_score, p.precision_weighted, p.f1_weighted,
                        d.max_js_distance, d.avg_js_distance, d.columns_drifted
                    FROM perf p
                    LEFT JOIN drift_summary d
                        ON p.source_table_name = d.source_table_name
                        AND p.window_start = d.window_start
                        AND p.window_end = d.window_end
                        AND p.granularity = d.granularity
                """,
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
        unified_performance_view: Optional[str] = None,
        dashboard_name: Optional[str] = None,
    ) -> str:
        """Deploy Lakeview dashboard pointing to the unified views.

        Args:
            unified_drift_view: Full name of unified drift metrics view.
            parent_path: Workspace path for dashboard.
            unified_profile_view: Full name of unified profile metrics view.
            unified_performance_view: Full name of unified performance metrics view.
            dashboard_name: Optional custom dashboard name.

        Returns:
            Dashboard ID.
        """
        name = dashboard_name or f"DPO Global Health - {self.catalog}"

        threshold = self.config.alerting.drift_threshold

        template = _build_dashboard_template(
            drift_threshold=threshold,
            null_rate_threshold=self.config.alerting.null_rate_threshold,
            row_count_min=self.config.alerting.row_count_min,
        )
        template["displayName"] = name
        _ensure_query_names(template)

        # Derive view names from the drift view when not provided
        profile_view = unified_profile_view
        profile_available = False
        if profile_view:
            profile_available = True
        else:
            candidate_profile_view = unified_drift_view.replace(
                "_drift", "_profile"
            )
            if candidate_profile_view != unified_drift_view:
                try:
                    self.w.tables.get(full_name=candidate_profile_view)
                    profile_view = candidate_profile_view
                    profile_available = True
                except Exception:
                    profile_available = False
        perf_view = unified_performance_view
        perf_available = False
        if perf_view:
            perf_available = True
        else:
            candidate_perf_view = unified_drift_view.replace(
                "_drift_metrics", "_performance_metrics"
            )
            if candidate_perf_view != unified_drift_view:
                try:
                    self.w.tables.get(full_name=candidate_perf_view)
                    perf_view = candidate_perf_view
                    perf_available = True
                except Exception:
                    perf_available = False

        # Inject actual view names into dataset queries
        for dataset in template.get("datasets", []):
            ds_name = dataset.get("name")
            if ds_name == "unified_drift":
                dataset["query"] = f"SELECT * FROM {unified_drift_view}"
            elif ds_name == "unified_profile":
                if profile_available and profile_view:
                    dataset["query"] = f"SELECT * FROM {profile_view}"
                else:
                    dataset["query"] = _empty_unified_profile_query()
            elif ds_name == "unified_performance":
                if perf_available and perf_view:
                    dataset["query"] = f"SELECT * FROM {perf_view}"
                else:
                    dataset["query"] = _empty_unified_performance_query()
            elif ds_name == "perf_vs_drift":
                if perf_available and perf_view:
                    dataset["query"] = dataset["query"].replace(
                        "{unified_drift_view}", unified_drift_view
                    ).replace(
                        "{unified_performance_view}", perf_view
                    )
                else:
                    dataset["query"] = _empty_perf_vs_drift_query()

        logger.info(f"Deploying dashboard: {name} to {parent_path}")

        try:
            self.w.workspace.mkdirs(parent_path)
        except Exception:
            pass

        try:
            dashboard_id = self._create_or_update_dashboard(name, parent_path, template)
            logger.info(f"Dashboard deployed successfully: {dashboard_id}")
            return dashboard_id
        except Exception as e:
            logger.error(f"Failed to deploy dashboard: {e}")
            raise

    def _find_existing_dashboard(self, name: str, parent_path: str):
        """Find existing dashboard with same name via Lakeview list API."""
        try:
            dashboards = self.w.lakeview.list(path=parent_path)
            for d in dashboards:
                if d.display_name == name:
                    return d
        except Exception:
            pass
        return None

    def _resolve_dashboard_by_path(self, name: str, parent_path: str) -> Optional[str]:
        """Resolve dashboard ID from its workspace file path.

        Falls back to the Workspace get-status API when lakeview.list fails
        to locate a dashboard that already exists on disk.
        """
        file_path = f"{parent_path}/{name}.lvdash.json"
        try:
            status = self.w.workspace.get_status(file_path)
            if status and status.resource_id:
                return status.resource_id
        except Exception:
            pass
        return None

    def _create_or_update_dashboard(
        self, name: str, parent_path: str, template: dict
    ) -> str:
        """Create a new dashboard or update it if one already exists.

        Handles the race where lakeview.list misses an existing dashboard
        and lakeview.create raises ResourceAlreadyExists.
        """
        _normalize_template(template)
        serialized = json.dumps(template)
        wh_id = getattr(self.config, "warehouse_id", None)
        existing = self._find_existing_dashboard(name, parent_path)

        if existing:
            logger.info(f"Updating existing dashboard: {existing.dashboard_id}")
            dashboard = self.w.lakeview.update(
                dashboard_id=existing.dashboard_id,
                dashboard=Dashboard(
                    display_name=name,
                    serialized_dashboard=serialized,
                    warehouse_id=wh_id,
                ),
            )
            return dashboard.dashboard_id

        try:
            dashboard = self.w.lakeview.create(
                dashboard=Dashboard(
                    display_name=name,
                    parent_path=parent_path,
                    serialized_dashboard=serialized,
                    warehouse_id=wh_id,
                ),
            )
            return dashboard.dashboard_id
        except ResourceAlreadyExists:
            logger.warning(
                f"Dashboard '{name}' already exists but was not found via list API; "
                "resolving by workspace path"
            )
            dashboard_id = self._resolve_dashboard_by_path(name, parent_path)
            if dashboard_id:
                dashboard = self.w.lakeview.update(
                    dashboard_id=dashboard_id,
                    dashboard=Dashboard(
                        display_name=name,
                        serialized_dashboard=serialized,
                        warehouse_id=wh_id,
                    ),
                )
                return dashboard.dashboard_id
            raise

    def get_dashboard_url(self, dashboard_id: str) -> str:
        """Get the URL for a dashboard."""
        try:
            host = self.w.config.host
            return f"{host}/dashboards/{dashboard_id}"
        except Exception:
            return f"/dashboards/{dashboard_id}"

    def deploy_dashboards_by_group(
        self,
        group_artifacts: Dict[str, GroupArtifactNames],
        parent_path: str,
    ) -> Dict[str, str]:
        """Deploy a dashboard for each monitor group.

        Args:
            group_artifacts: Dict mapping group_name -> resolved artifact names.
            parent_path: Workspace path for dashboards.

        Returns:
            Dict mapping group_name -> dashboard_id.
        """
        results = {}
        for group_name, artifacts in group_artifacts.items():
            dashboard_id = self.deploy_dashboard(
                artifacts.drift_view,
                parent_path,
                unified_profile_view=artifacts.profile_view,
                unified_performance_view=artifacts.performance_view,
                dashboard_name=artifacts.dashboard_name,
            )
            results[group_name] = dashboard_id

        return results

    def deploy_executive_rollup(
        self,
        group_artifacts: Dict[str, GroupArtifactNames],
        parent_path: str,
    ) -> str:
        """Deploy a single cross-group executive rollup dashboard.

        Args:
            group_artifacts: Mapping of group name to resolved artifact names.
            parent_path: Workspace path for the dashboard.

        Returns:
            Dashboard ID.
        """
        # Build UNION ALL queries across all group drift views
        def _sql_escape(val: str) -> str:
            return val.replace("'", "''")

        drift_union = " UNION ALL ".join(
            f"SELECT *, '{_sql_escape(group)}' as monitor_group "
            f"FROM {artifacts.drift_view}"
            for group, artifacts in group_artifacts.items()
        )

        template = {
            "displayName": "DPO Executive Rollup",
            "pages": [
                {
                    "name": "executive_rollup",
                    "displayName": "Executive Rollup",
                    "layout": [
                        {
                            "widget": {
                                "name": "group_summary",
                                "queries": [{
                                    "query": {
                                        "datasetName": "rollup_drift",
                                        "fields": [
                                            {"name": "monitor_group", "expression": "`monitor_group`"},
                                            {"name": "tables_monitored", "expression": "COUNT(DISTINCT `source_table_name`)"},
                                            {"name": "critical_alerts", "expression": f"COUNT(CASE WHEN `js_distance` >= {self.config.alerting.drift_threshold} THEN 1 END)"},
                                        ],
                                        "disaggregated": False,
                                    }
                                }],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            {"fieldName": "monitor_group", "displayName": "Group"},
                                            {"fieldName": "tables_monitored", "displayName": "Tables Monitored"},
                                            {"fieldName": "critical_alerts", "displayName": "Critical Alerts"},
                                        ],
                                    },
                                    "frame": {"showTitle": True, "title": "Group Summary"},
                                },
                            },
                            "position": {"x": 0, "y": 0, "width": 6, "height": 5},
                        },
                        {
                            "widget": {
                                "name": "worst_drifters",
                                "queries": [{
                                    "query": {
                                        "datasetName": "rollup_drift",
                                        "fields": [
                                            {"name": "source_table_name", "expression": "`source_table_name`"},
                                            {"name": "monitor_group", "expression": "`monitor_group`"},
                                            {"name": "max_drift", "expression": "MAX(`js_distance`)"},
                                        ],
                                        "disaggregated": False,
                                    }
                                }],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            {"fieldName": "source_table_name", "displayName": "Table"},
                                            {"fieldName": "monitor_group", "displayName": "Group"},
                                            {"fieldName": "max_drift", "displayName": "Max Drift"},
                                        ],
                                    },
                                    "frame": {"showTitle": True, "title": "Cross-Group Worst Drifters"},
                                },
                            },
                            "position": {"x": 0, "y": 5, "width": 6, "height": 5},
                        },
                        {
                            "widget": {
                                "name": "group_health",
                                "queries": [{
                                    "query": {
                                        "datasetName": "rollup_drift",
                                        "fields": [
                                            {"name": "monitor_group", "expression": "`monitor_group`"},
                                            {"name": "avg_drift", "expression": "AVG(`js_distance`)"},
                                        ],
                                        "disaggregated": False,
                                    }
                                }],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "bar",
                                    "encodings": {
                                        "x": {"fieldName": "monitor_group", "scale": {"type": "categorical"}, "displayName": "Group"},
                                        "y": {"fieldName": "avg_drift", "scale": {"type": "quantitative"}, "displayName": "Avg Drift"},
                                    },
                                    "frame": {"showTitle": True, "title": "Group Health"},
                                },
                            },
                            "position": {"x": 0, "y": 10, "width": 6, "height": 5},
                        },
                    ],
                },
            ],
            "datasets": [
                {
                    "name": "rollup_drift",
                    "displayName": "Cross-Group Drift",
                    "query": drift_union,
                },
            ],
        }

        name = "DPO Executive Rollup"
        _ensure_query_names(template)

        try:
            self.w.workspace.mkdirs(parent_path)
        except Exception:
            pass

        try:
            dashboard_id = self._create_or_update_dashboard(name, parent_path, template)
            logger.info(f"Executive rollup dashboard deployed: {dashboard_id}")
            return dashboard_id
        except Exception as e:
            logger.error(f"Failed to deploy executive rollup dashboard: {e}")
            raise

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
                    "pageType": "PAGE_TYPE_CANVAS",
                    "layout": [
                        {
                            "widget": {
                                "name": "drift_timeline",
                                "queries": [
                                    {
                                        "name": "main_query",
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
                                                    "name": "js_distance",
                                                    "expression": "`js_distance`",
                                                },
                                            ],
                                            "disaggregated": True,
                                        }
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "line",
                                    "encodings": {
                                        "x": {"fieldName": "window_end", "scale": {"type": "temporal"}, "displayName": "Window End"},
                                        "y": {"fieldName": "js_distance", "scale": {"type": "quantitative"}, "displayName": "JS Distance"},
                                        "color": {"fieldName": "column_name", "scale": {"type": "categorical"}, "displayName": "Column"},
                                    },
                                    "frame": {"showTitle": True, "title": "Drift Timeline"},
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
                    "queryLines": [f"SELECT * FROM {unified_view} WHERE source_table_name = '{table_name}'"],
                }
            ],
        }
