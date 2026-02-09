# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Monitor Groups Demo
# MAGIC 
# MAGIC This notebook demonstrates how to use **Monitor Groups** for per-team/department monitoring.
# MAGIC 
# MAGIC **What you'll learn:**
# MAGIC - How to tag tables with `monitor_group` for separate aggregation
# MAGIC - How DPO creates separate views, alerts, and dashboards per group
# MAGIC - How to configure per-group alert routing
# MAGIC - How stale artifacts are automatically cleaned up

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.77.0 pyyaml pydantic tenacity tabulate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understanding Monitor Groups
# MAGIC 
# MAGIC Monitor Groups allow you to segment your monitoring by team, department, or use case:
# MAGIC 
# MAGIC | Group | Tables | Gets Their Own |
# MAGIC |-------|--------|----------------|
# MAGIC | `ml_team` | churn_model, fraud_model | View, Alert, Dashboard |
# MAGIC | `data_eng` | sales_data, inventory | View, Alert, Dashboard |
# MAGIC | `default` | (tables without group tag) | View, Alert, Dashboard |
# MAGIC 
# MAGIC ### How It Works
# MAGIC 
# MAGIC ```
# MAGIC Tables with monitor_group: "ml_team"  ──┐
# MAGIC                                         ├──► unified_drift_metrics_ml_team
# MAGIC Tables with monitor_group: "ml_team"  ──┘
# MAGIC 
# MAGIC Tables with monitor_group: "data_eng" ──┐
# MAGIC                                         ├──► unified_drift_metrics_data_eng
# MAGIC Tables with monitor_group: "data_eng" ──┘
# MAGIC 
# MAGIC Tables without monitor_group tag      ──► unified_drift_metrics_default
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tagging Tables with Monitor Groups
# MAGIC 
# MAGIC Tag your tables with `monitor_group` to assign them to a group:
# MAGIC 
# MAGIC ```sql
# MAGIC -- ML Team tables
# MAGIC ALTER TABLE prod.ml.churn_predictions 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'monitor_group' = 'ml_team',  -- Group assignment
# MAGIC     'owner' = 'ml-team@company.com'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE prod.ml.fraud_detection 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'monitor_group' = 'ml_team',  -- Same group
# MAGIC     'owner' = 'ml-team@company.com'
# MAGIC );
# MAGIC 
# MAGIC -- Data Engineering tables
# MAGIC ALTER TABLE prod.warehouse.sales_data 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'monitor_group' = 'data_eng',  -- Different group
# MAGIC     'owner' = 'data-eng@company.com'
# MAGIC );
# MAGIC 
# MAGIC -- Table without group (goes to "default")
# MAGIC ALTER TABLE prod.shared.reference_data 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true'
# MAGIC     -- No monitor_group tag = goes to "default" group
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration with Per-Group Alert Routing

# COMMAND ----------

from dpo import run_orchestration, OrchestratorConfig
from dpo.config import DiscoveryConfig, ProfileConfig, AlertConfig, MonitoredTableConfig

# Full config with monitor groups and per-group alert routing
config = OrchestratorConfig(
    mode="full",
    catalog_name="prod",
    monitor_group_tag="monitor_group",  # Tag name to use for grouping
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=True,  # Discover via tags
    
    discovery=DiscoveryConfig(
        include_tags={"monitor_enabled": "true"},
    ),
    
    # Can also define tables in config (YAML wins for overlapping tables)
    monitored_tables={},
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        granularity="1 day",
        prediction_column="prediction",
        label_column="label",
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,
        # Default notifications (fallback for groups without specific routing)
        default_notifications=[
            "mlops-alerts@company.com",
        ],
        # Per-group notification routing
        group_notifications={
            "ml_team": [
                "ml-team@company.com",
                "ml-oncall@company.com",
            ],
            "data_eng": [
                "data-eng@company.com",
            ],
            # "default" group uses default_notifications
        },
    ),
    deploy_aggregated_dashboard=True,
    dashboard_parent_path="/Workspace/Shared/DPO",
    dry_run=True,
)

print("Configuration:")
print(f"  Mode: {config.mode}")
print(f"  Group tag: {config.monitor_group_tag}")
print(f"\nPer-group notifications:")
for group, notifs in config.alerting.group_notifications.items():
    print(f"  {group}: {notifs}")
print(f"  default: {config.alerting.default_notifications}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Orchestration with Groups

# COMMAND ----------

# Preview what would happen
report = run_orchestration(config)

print(f"\n{'='*60}")
print("DRY RUN COMPLETE")
print(f"{'='*60}")
print(f"Tables discovered: {report.tables_discovered}")
print(f"\nUnified views per group:")
for group, view in report.unified_drift_views.items():
    print(f"  {group}: {view}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute with Groups

# COMMAND ----------

# When ready, disable dry_run
config.dry_run = False

# Run full orchestration
report = run_orchestration(config)

print(f"\n{'='*60}")
print("ORCHESTRATION COMPLETE")
print(f"{'='*60}")
print(f"Tables discovered: {report.tables_discovered}")
print(f"Monitors created: {report.monitors_created}")

print(f"\n--- Per-Group Outputs ---")
print(f"\nUnified Drift Views:")
for group, view in report.unified_drift_views.items():
    print(f"  {group}: {view}")

print(f"\nUnified Profile Views:")
for group, view in report.unified_profile_views.items():
    print(f"  {group}: {view}")

print(f"\nDrift Alerts:")
for group, alert_id in report.drift_alert_ids.items():
    print(f"  {group}: {alert_id}")

print(f"\nDashboards:")
for group, dashboard_id in report.dashboard_ids.items():
    print(f"  {group}: {dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Querying Per-Group Views
# MAGIC 
# MAGIC Each group has its own unified view. Query them separately for group-specific analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query ML team's drift metrics
# MAGIC -- SELECT 
# MAGIC --     source_table_name,
# MAGIC --     column_name,
# MAGIC --     js_divergence,
# MAGIC --     window_end
# MAGIC -- FROM prod.global_monitoring.unified_drift_metrics_ml_team
# MAGIC -- WHERE js_divergence > 0.1
# MAGIC -- ORDER BY js_divergence DESC
# MAGIC -- LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query Data Engineering team's drift metrics
# MAGIC -- SELECT 
# MAGIC --     source_table_name,
# MAGIC --     column_name,
# MAGIC --     js_divergence,
# MAGIC --     window_end
# MAGIC -- FROM prod.global_monitoring.unified_drift_metrics_data_eng
# MAGIC -- WHERE js_divergence > 0.1
# MAGIC -- ORDER BY js_divergence DESC
# MAGIC -- LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Automatic Cleanup of Stale Artifacts
# MAGIC 
# MAGIC When you rename a monitor group (e.g., "marketing" → "growth"), DPO automatically:
# MAGIC 
# MAGIC 1. Creates new views/alerts/dashboards for the new group
# MAGIC 2. **Drops stale views** from the old group
# MAGIC 3. **Trashes stale dashboards** from the old group
# MAGIC 
# MAGIC This prevents:
# MAGIC - Stale views cluttering your schema
# MAGIC - Confusing dashboards referencing old groups
# MAGIC - Wasted compute on unused views

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Customizing the Group Tag Name
# MAGIC 
# MAGIC You can use a different tag name for grouping:

# COMMAND ----------

# Use "department" instead of "monitor_group"
config_custom_tag = OrchestratorConfig(
    mode="full",
    catalog_name="prod",
    monitor_group_tag="department",  # Custom tag name
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=True,
    
    discovery=DiscoveryConfig(
        include_tags={"monitor_enabled": "true"},
    ),
    
    monitored_tables={},
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        prediction_column="prediction",
    ),
    dry_run=True,
)

print(f"Using custom group tag: {config_custom_tag.monitor_group_tag}")
print("\nTables would be grouped by their 'department' tag value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Safe Group Names (Sanitization)
# MAGIC 
# MAGIC DPO automatically sanitizes group names for use in SQL identifiers:
# MAGIC 
# MAGIC | Tag Value | Sanitized | View Name |
# MAGIC |-----------|-----------|-----------|
# MAGIC | `ml_team` | `ml_team` | `unified_drift_metrics_ml_team` |
# MAGIC | `ML Team` | `ml_team` | `unified_drift_metrics_ml_team` |
# MAGIC | `Marketing & Sales` | `marketing_sales` | `unified_drift_metrics_marketing_sales` |
# MAGIC | `North America (NA)` | `north_america_na` | `unified_drift_metrics_north_america_na` |
# MAGIC 
# MAGIC This prevents SQL errors from special characters in tag values.

# COMMAND ----------

from dpo.utils import sanitize_sql_identifier

# Test sanitization
test_names = [
    "ml_team",
    "ML Team",
    "Marketing & Sales",
    "North America (NA)",
    "Team-123",
    "",
]

print("Sanitization Examples:")
for name in test_names:
    safe = sanitize_sql_identifier(name)
    print(f"  '{name}' → '{safe}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC - **02_full_orchestration** - Basic full mode without groups
# MAGIC - **05_bulk_provisioning** - Bulk mode for quick onboarding
# MAGIC - **03_alerting_setup** - Deep dive into alert configuration
