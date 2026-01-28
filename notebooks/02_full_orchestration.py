# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Full Orchestration
# MAGIC 
# MAGIC This notebook demonstrates the complete DPO pipeline:
# MAGIC 1. Discovery - Find tables tagged for monitoring
# MAGIC 2. Provisioning - Create/update Data Profiling monitors (Snapshot, TimeSeries, or Inference)
# MAGIC 3. Aggregation - Create unified metrics views (profile_metrics + drift_metrics)
# MAGIC 4. Alerting - Set up drift alerts AND data quality alerts
# MAGIC 5. Dashboard - Deploy global health dashboard
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Tables tagged with `monitor_enabled = 'true'`
# MAGIC - SQL Warehouse for statement execution
# MAGIC - Appropriate permissions on target catalog/schema

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.68.0 pyyaml pydantic tenacity tabulate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC 
# MAGIC DPO supports three profile types:
# MAGIC - **SNAPSHOT**: Simple data quality checks (no time windows)
# MAGIC - **TIMESERIES**: Time-windowed data quality monitoring
# MAGIC - **INFERENCE**: ML model monitoring with drift + quality metrics

# COMMAND ----------

# Option A: Load from YAML file
# from dpo import load_config
# config = load_config("/Workspace/path/to/config.yaml")

# Option B: Configure inline
from dpo.config import (
    OrchestratorConfig,
    DiscoveryConfig,
    ProfileConfig,
    InferenceConfig,
    TimeSeriesConfig,
    AlertConfig,
    CustomMetricConfig,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration for INFERENCE Profile (ML Models)

# COMMAND ----------

config_inference = OrchestratorConfig(
    warehouse_id="YOUR_WAREHOUSE_ID",  # Required: Your SQL Warehouse ID
    discovery=DiscoveryConfig(
        catalog_name="prod",  # Change to your catalog
        include_tags={"monitor_enabled": "true"},
        exclude_schemas=["information_schema", "tmp_*"],
    ),
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        granularity="1 day",
        slicing_columns=["model_version", "region"],
        max_slicing_cardinality=50,
        # Optional: baseline table for drift comparison against training data
        baseline_table=None,  # e.g., "prod.ml.training_data"
        inference_settings=InferenceConfig(
            problem_type="PROBLEM_TYPE_CLASSIFICATION",
            prediction_col="prediction",
            timestamp_col="timestamp",
            label_col="label",
            model_id_col="model_version",
        ),
        # Optional: custom business metrics
        custom_metrics=[
            # CustomMetricConfig(
            #     name="revenue_per_prediction",
            #     metric_type="aggregate",
            #     input_columns=["revenue"],
            #     definition="SUM(revenue)",
            #     output_type="double",
            # ),
        ],
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,
        null_rate_threshold=0.1,
        row_count_min=1000,
        default_notifications=["mlops@company.com"],
    ),
    dry_run=True,  # Start with dry run to preview changes
    cleanup_orphans=False,
    deploy_aggregated_dashboard=True,
    dashboard_parent_path="/Workspace/Shared/DPO",
)

print("INFERENCE Configuration")
print(f"  Catalog: {config_inference.discovery.catalog_name}")
print(f"  Profile Type: {config_inference.profile_defaults.profile_type}")
print(f"  Output schema: {config_inference.profile_defaults.output_schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration for TIMESERIES Profile (Event Data)

# COMMAND ----------

config_timeseries = OrchestratorConfig(
    warehouse_id="YOUR_WAREHOUSE_ID",
    discovery=DiscoveryConfig(
        catalog_name="prod",
        include_tags={"monitor_enabled": "true", "profile_type": "timeseries"},
    ),
    profile_defaults=ProfileConfig(
        profile_type="TIMESERIES",
        output_schema_name="monitoring_results",
        granularity="1 day",
        timeseries_settings=TimeSeriesConfig(
            timestamp_col="event_time",
        ),
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,
        null_rate_threshold=0.1,
    ),
    dry_run=True,
)

print("\nTIMESERIES Configuration")
print(f"  Profile Type: {config_timeseries.profile_defaults.profile_type}")
print(f"  Timestamp Col: {config_timeseries.profile_defaults.timeseries_settings.timestamp_col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration for SNAPSHOT Profile (Static Data Quality)

# COMMAND ----------

config_snapshot = OrchestratorConfig(
    warehouse_id="YOUR_WAREHOUSE_ID",
    discovery=DiscoveryConfig(
        catalog_name="prod",
        include_tags={"monitor_enabled": "true", "profile_type": "snapshot"},
    ),
    profile_defaults=ProfileConfig(
        profile_type="SNAPSHOT",
        output_schema_name="monitoring_results",
        granularity="1 day",  # Still needed for refresh schedule
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        null_rate_threshold=0.05,  # Stricter for static data
        row_count_min=10000,
    ),
    dry_run=True,
)

print("\nSNAPSHOT Configuration")
print(f"  Profile Type: {config_snapshot.profile_defaults.profile_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run Orchestration (Dry Run)
# MAGIC 
# MAGIC First, run with `dry_run=True` to preview changes without executing.

# COMMAND ----------

# Select which config to use
config = config_inference  # or config_timeseries, config_snapshot

from dpo import run_orchestration

# Run orchestration
report = run_orchestration(config)

print("\n" + "=" * 60)
print("ORCHESTRATION REPORT")
print("=" * 60)
print(f"Tables discovered: {report.tables_discovered}")
print(f"Monitors created: {report.monitors_created}")
print(f"Monitors updated: {report.monitors_updated}")
print(f"Monitors skipped: {report.monitors_skipped}")
print(f"Monitors failed: {report.monitors_failed}")
print(f"Orphans cleaned: {report.orphans_cleaned}")
print(f"\nViews Created (by group):")
for group, view in report.unified_drift_views.items():
    print(f"  [{group}] Drift: {view}")
for group, view in report.unified_profile_views.items():
    print(f"  [{group}] Profile: {view}")
print(f"\nAlerts Created (by group):")
for group, alert_id in report.drift_alert_ids.items():
    print(f"  [{group}] Drift Alert: {alert_id}")
for group, alert_id in report.quality_alert_ids.items():
    print(f"  [{group}] Quality Alert: {alert_id}")
print(f"\nDashboards (by group):")
for group, dashboard_id in report.dashboard_ids.items():
    print(f"  [{group}] Dashboard: {dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute (Set dry_run=False)
# MAGIC 
# MAGIC After reviewing the dry run output, set `dry_run=False` to execute.

# COMMAND ----------

# Uncomment to execute for real
# config.dry_run = False
# report = run_orchestration(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Step-by-Step Execution
# MAGIC 
# MAGIC Alternatively, run each phase individually for more control.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from dpo.discovery import TableDiscovery
from dpo.provisioning import ProfileProvisioner
from dpo.aggregator import MetricsAggregator
from dpo.alerting import AlertProvisioner
from dpo.dashboard import DashboardProvisioner
from dpo.utils import verify_output_schema_permissions

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Pre-flight Permission Check

# COMMAND ----------

# Verify we can write to the output schema
verify_output_schema_permissions(
    w=w,
    catalog=config.discovery.catalog_name,
    schema=config.profile_defaults.output_schema_name,
    warehouse_id=config.warehouse_id,
)
print("Permission check passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Discovery

# COMMAND ----------

discovery = TableDiscovery(w, config.discovery)
tables = discovery.discover()
print(f"Discovered {len(tables)} tables")

# Show table details
import pandas as pd
if tables:
    df = pd.DataFrame([
        {
            "Table": t.full_name,
            "Priority": t.priority,
            "Owner": t.tags.get("owner", "unknown"),
        }
        for t in tables[:10]
    ])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Provisioning

# COMMAND ----------

provisioner = ProfileProvisioner(w, config)

# Dry run
results = provisioner.dry_run_all(tables)

# Or execute for real:
# results = provisioner.provision_all(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Aggregation - Both Profile and Drift Views

# COMMAND ----------

aggregator = MetricsAggregator(w, config)
catalog = config.discovery.catalog_name

# Define view names
unified_drift_view = f"{catalog}.global_monitoring.unified_drift_metrics"
unified_profile_view = f"{catalog}.global_monitoring.unified_profile_metrics"

# Preview Drift View DDL
print("=== DRIFT VIEW DDL ===")
drift_ddl = aggregator._generate_unified_drift_view_ddl(tables, unified_drift_view, use_materialized=False)
print(drift_ddl[:1000] + "..." if len(drift_ddl) > 1000 else drift_ddl)

# Preview Profile View DDL
print("\n=== PROFILE VIEW DDL ===")
profile_ddl = aggregator._generate_unified_profile_view_ddl(tables, unified_profile_view, use_materialized=False)
print(profile_ddl[:1000] + "..." if len(profile_ddl) > 1000 else profile_ddl)

# Execute:
# aggregator.create_unified_drift_view(tables, unified_drift_view)
# aggregator.create_unified_profile_view(tables, unified_profile_view)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Alerting - Drift AND Data Quality

# COMMAND ----------

alerter = AlertProvisioner(w, config)

# Preview drift alert query
print("=== DRIFT ALERT ===")
queries = alerter.generate_tiered_alert_queries(unified_drift_view, catalog)
for name, sql in queries.items():
    print(f"\n--- {name} ---")
    print(sql[:500] + "..." if len(sql) > 500 else sql)

# Preview data quality alert conditions
print("\n=== DATA QUALITY ALERT CONDITIONS ===")
print(f"  Null rate threshold: {config.alerting.null_rate_threshold}")
print(f"  Min row count: {config.alerting.row_count_min}")
print(f"  Min distinct count: {config.alerting.distinct_count_min}")

# Create alerts:
# alerter.create_unified_drift_alert(unified_drift_view, catalog)
# alerter.create_data_quality_alert(unified_profile_view, catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Dashboard

# COMMAND ----------

dashboard_provisioner = DashboardProvisioner(w, config)

# Preview template
import json
from dpo.dashboard import GLOBAL_HEALTH_DASHBOARD_TEMPLATE
print(json.dumps(GLOBAL_HEALTH_DASHBOARD_TEMPLATE, indent=2)[:2000] + "...")

# Deploy dashboard:
# dashboard_id = dashboard_provisioner.deploy_aggregated_dashboard(unified_drift_view, config.dashboard_parent_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Querying Results
# MAGIC 
# MAGIC After execution, query the unified views:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drift Metrics Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment and run after orchestration completes
# MAGIC -- SELECT 
# MAGIC --     source_table_name,
# MAGIC --     column_name,
# MAGIC --     js_divergence,
# MAGIC --     chi_square_statistic,
# MAGIC --     drift_type,
# MAGIC --     owner,
# MAGIC --     department
# MAGIC -- FROM prod.global_monitoring.unified_drift_metrics
# MAGIC -- WHERE js_divergence >= 0.1
# MAGIC -- ORDER BY js_divergence DESC
# MAGIC -- LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profile Metrics Query (Data Quality)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment and run after orchestration completes
# MAGIC -- SELECT 
# MAGIC --     source_table_name,
# MAGIC --     column_name,
# MAGIC --     null_rate,
# MAGIC --     record_count,
# MAGIC --     distinct_count,
# MAGIC --     mean,
# MAGIC --     stddev,
# MAGIC --     owner
# MAGIC -- FROM prod.global_monitoring.unified_profile_metrics
# MAGIC -- WHERE null_rate > 0.1  -- High null columns
# MAGIC -- ORDER BY null_rate DESC
# MAGIC -- LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Wall of Shame Query
# MAGIC 
# MAGIC Find the worst drifting models across the company:

# COMMAND ----------

print("Wall of Shame Query:")
print(aggregator.get_drift_summary_query(unified_drift_view))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Summary Query

# COMMAND ----------

print("Data Quality Summary Query:")
print(aggregator.get_data_quality_summary_query(unified_profile_view))
