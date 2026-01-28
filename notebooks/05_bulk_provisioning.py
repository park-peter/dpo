# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Bulk Provisioning Mode
# MAGIC 
# MAGIC This notebook demonstrates the Bulk Provisioning mode of the Data Profiling Orchestrator.
# MAGIC 
# MAGIC **Bulk Provisioning Mode** creates monitors for all tagged tables without:
# MAGIC - Unified aggregation views
# MAGIC - Centralized alerts
# MAGIC - Dashboards
# MAGIC 
# MAGIC **When to use Bulk Mode:**
# MAGIC - You have 100+ tables and want the fastest setup
# MAGIC - You prefer using Databricks' native per-table UI for results
# MAGIC - You don't need cross-table unified observability yet
# MAGIC - You want the simplest possible onboarding path

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.68.0 pyyaml pydantic tenacity tabulate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Minimal Configuration for Bulk Mode

# COMMAND ----------

from dpo import run_orchestration, OrchestratorConfig
from dpo.config import DiscoveryConfig, ProfileConfig, AlertConfig

# Minimal config for bulk provisioning
config = OrchestratorConfig(
    mode="bulk_provision_only",  # Key setting for bulk mode
    warehouse_id="YOUR_WAREHOUSE_ID",  # Replace with your warehouse ID
    discovery=DiscoveryConfig(
        catalog_name="prod",  # Change to your catalog
        include_tags={"monitor_enabled": "true"},
        exclude_schemas=["information_schema", "tmp_*", "dev_*"],
    ),
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        granularity="1 day",
    ),
    # These are ignored in bulk mode, but included for completeness
    alerting=AlertConfig(enable_aggregated_alerts=False),
    deploy_aggregated_dashboard=False,
    dry_run=True,  # Start with dry run to preview
)

print(f"Mode: {config.mode}")
print(f"Catalog: {config.discovery.catalog_name}")
print(f"Dry run: {config.dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run Dry Run to Preview

# COMMAND ----------

# Preview what would happen
report = run_orchestration(config)

print(f"\n{'='*60}")
print("DRY RUN COMPLETE - No changes made")
print(f"{'='*60}")
print(f"Tables discovered: {report.tables_discovered}")
print(f"Monitors would be created: {report.monitors_created}")
print(f"Monitors would be updated: {report.monitors_updated}")
print(f"Monitors skipped: {report.monitors_skipped}")
print(f"\n(Set dry_run=False to execute)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute Bulk Provisioning

# COMMAND ----------

# When ready, disable dry_run
config.dry_run = False

# Run bulk provisioning
report = run_orchestration(config)

print(f"\n{'='*60}")
print("BULK PROVISIONING COMPLETE")
print(f"{'='*60}")
print(f"Tables discovered: {report.tables_discovered}")
print(f"Monitors created: {report.monitors_created}")
print(f"Monitors updated: {report.monitors_updated}")
print(f"Monitors failed: {report.monitors_failed}")
print(f"Orphans cleaned: {report.orphans_cleaned}")

# In bulk mode, these are empty
print(f"\nUnified views: {report.unified_drift_views}")  # Empty dict
print(f"Alerts: {report.drift_alert_ids}")  # Empty dict
print(f"Dashboards: {report.dashboard_ids}")  # Empty dict

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Where to Find Results
# MAGIC 
# MAGIC In bulk provisioning mode, each table gets its own monitor with separate output tables.
# MAGIC 
# MAGIC ### Per-Table Output Tables
# MAGIC 
# MAGIC For each monitored table, Data Profiling creates:
# MAGIC 
# MAGIC | Output Table | Description |
# MAGIC |-------------|-------------|
# MAGIC | `{output_schema}.{table_name}_profile_metrics` | Summary statistics per column |
# MAGIC | `{output_schema}.{table_name}_drift_metrics` | Drift statistics over time |
# MAGIC 
# MAGIC ### Viewing Results in the UI
# MAGIC 
# MAGIC 1. Navigate to **Catalog** > **Your Catalog** > **Your Schema** > **Your Table**
# MAGIC 2. Click on the **"Data"** tab
# MAGIC 3. Click on **"Profile"** to see the monitor status and metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema-Level Filtering with include_schemas
# MAGIC 
# MAGIC You can target specific schemas using `include_schemas`:

# COMMAND ----------

# Only scan specific schemas
config_with_schema_filter = OrchestratorConfig(
    mode="bulk_provision_only",
    warehouse_id="YOUR_WAREHOUSE_ID",
    discovery=DiscoveryConfig(
        catalog_name="prod",
        include_tags={"monitor_enabled": "true"},
        include_schemas=["ml_models", "data_warehouse_*"],  # Only these schemas
        exclude_schemas=["information_schema"],
    ),
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
    ),
    dry_run=True,
)

print("Schema filter active:")
print(f"  include_schemas: {config_with_schema_filter.discovery.include_schemas}")
print(f"  exclude_schemas: {config_with_schema_filter.discovery.exclude_schemas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Upgrading to Full Mode
# MAGIC 
# MAGIC When you're ready for unified observability, simply change the mode:
# MAGIC 
# MAGIC ```python
# MAGIC config = OrchestratorConfig(
# MAGIC     mode="full",  # Changed from "bulk_provision_only"
# MAGIC     # ... rest of config
# MAGIC     alerting=AlertConfig(enable_aggregated_alerts=True),
# MAGIC     deploy_aggregated_dashboard=True,
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC This will:
# MAGIC - Keep all existing monitors (no re-creation needed)
# MAGIC - Create unified drift and profile views
# MAGIC - Set up centralized alerts
# MAGIC - Deploy a global health dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC - **02_full_orchestration** - Run with full mode for unified observability
# MAGIC - **06_monitor_groups_demo** - Learn about per-team/group monitoring
