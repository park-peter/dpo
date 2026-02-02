# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Full Orchestration
# MAGIC 
# MAGIC This notebook demonstrates the complete DPO pipeline:
# MAGIC 1. Discovery - Find tables tagged for monitoring (or use config-driven tables)
# MAGIC 2. Provisioning - Create/update Data Profiling monitors (Snapshot, TimeSeries, or Inference)
# MAGIC 3. Aggregation - Create unified metrics views (profile_metrics + drift_metrics)
# MAGIC 4. Alerting - Set up drift alerts AND data quality alerts
# MAGIC 5. Dashboard - Deploy global health dashboard
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Tables tagged with `monitor_enabled = 'true'` OR defined in `monitored_tables`
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
# MAGIC DPO supports two ways to specify tables:
# MAGIC - **monitored_tables**: Explicit per-table config in YAML (recommended)
# MAGIC - **include_tagged_tables**: Discover tables via UC tags
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
    AlertConfig,
    MonitoredTableConfig,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration for INFERENCE Profile (ML Models) - Config-Driven

# COMMAND ----------

config_inference = OrchestratorConfig(
    catalog_name="prod",  # Required: Target catalog
    warehouse_id="YOUR_WAREHOUSE_ID",  # Required: Your SQL Warehouse ID
    mode="full",
    include_tagged_tables=False,  # Only use monitored_tables
    
    # Per-table configuration
    monitored_tables={
        "prod.ml.churn_predictions": MonitoredTableConfig(
            baseline_table_name="prod.ml.churn_baseline",
            label_column="churned",
            prediction_column="churn_probability",
            timestamp_column="prediction_time",
            problem_type="PROBLEM_TYPE_CLASSIFICATION",
            granularity="1 day",
            slicing_exprs=["region", "customer_segment"],
        ),
        "prod.ml.fraud_detection": MonitoredTableConfig(
            label_column="is_fraud",
            prediction_column="fraud_score",
        ),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        granularity="1 day",
        slicing_exprs=["model_version"],
        max_slicing_cardinality=50,
        create_builtin_dashboard=False,  # Don't create per-monitor dashboards
        problem_type="PROBLEM_TYPE_CLASSIFICATION",
        prediction_column="prediction",
        timestamp_column="timestamp",
        label_column="label",
        model_id_column="model_version",
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

print("INFERENCE Configuration (Config-Driven)")
print(f"  Catalog: {config_inference.catalog_name}")
print(f"  Profile Type: {config_inference.profile_defaults.profile_type}")
print(f"  Tables: {list(config_inference.monitored_tables.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration for Tag-Based Discovery

# COMMAND ----------

config_discovery = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    mode="full",
    include_tagged_tables=True,  # Discover via tags
    
    discovery=DiscoveryConfig(
        include_tags={"monitor_enabled": "true"},
        exclude_schemas=["information_schema", "tmp_*"],
    ),
    
    # Can combine with monitored_tables (YAML wins for overlapping tables)
    monitored_tables={},
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        granularity="1 day",
        problem_type="PROBLEM_TYPE_CLASSIFICATION",
        prediction_column="prediction",
        timestamp_column="timestamp",
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,
    ),
    dry_run=True,
)

print("\nTag-Based Discovery Configuration")
print(f"  include_tagged_tables: {config_discovery.include_tagged_tables}")
print(f"  Tags: {config_discovery.discovery.include_tags}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration for SNAPSHOT Profile (Static Data Quality)

# COMMAND ----------

config_snapshot = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    mode="full",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.dimensions.customers": MonitoredTableConfig(),
        "prod.dimensions.products": MonitoredTableConfig(),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="SNAPSHOT",
        output_schema_name="monitoring_results",
        granularity="1 day",  # Refresh schedule
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
# MAGIC ### Configuration for TIMESERIES Profile (Event Data)

# COMMAND ----------

config_timeseries = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    mode="full",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.events.page_views": MonitoredTableConfig(
            timestamp_column="event_time",
            slicing_exprs=["region", "platform"],
        ),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="TIMESERIES",
        output_schema_name="monitoring_results",
        granularity="1 day",
        timeseries_timestamp_column="event_time",
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
print(f"  Timestamp Column: {config_timeseries.profile_defaults.timeseries_timestamp_column}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run Orchestration (Dry Run)
# MAGIC 
# MAGIC First, run with `dry_run=True` to preview changes without executing.

# COMMAND ----------

# Select which config to use
config = config_inference  # or config_snapshot, config_timeseries, config_discovery

from dpo import run_orchestration

# Run orchestration
report = run_orchestration(config)

print("\n" + "=" * 60)
print("ORCHESTRATION REPORT")
print("=" * 60)
print(f"Tables processed: {report.tables_discovered}")
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
    catalog=config.catalog_name,
    schema=config.profile_defaults.output_schema_name,
    warehouse_id=config.warehouse_id,
)
print("Permission check passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Discovery (if using tags)

# COMMAND ----------

# Only needed if include_tagged_tables=True
if config.include_tagged_tables and config.discovery:
    discovery = TableDiscovery(w, config.discovery, config.catalog_name)
    tables = discovery.discover()
    print(f"Discovered {len(tables)} tables via tags")

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
else:
    print("Using monitored_tables config (tag discovery disabled)")
    print(f"Tables: {list(config.monitored_tables.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Provisioning

# COMMAND ----------

# Note: The full run_orchestration handles building the table list
# This is just for demonstration
# provisioner = ProfileProvisioner(w, config)

# Dry run
# results = provisioner.dry_run_all(tables)

# Or execute for real:
# results = provisioner.provision_all(tables)

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
# MAGIC -- FROM prod.global_monitoring.unified_drift_metrics_default
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
# MAGIC -- FROM prod.global_monitoring.unified_profile_metrics_default
# MAGIC -- WHERE null_rate > 0.1  -- High null columns
# MAGIC -- ORDER BY null_rate DESC
# MAGIC -- LIMIT 100
