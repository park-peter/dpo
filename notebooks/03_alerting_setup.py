# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Alerting Setup
# MAGIC 
# MAGIC This notebook demonstrates how to configure and deploy alerts:
# MAGIC 
# MAGIC **Alert Types:**
# MAGIC 1. **Drift Alerts** - Monitor JS divergence for feature drift
# MAGIC 2. **Data Quality Alerts** - Monitor null rates, row counts, cardinality
# MAGIC 
# MAGIC **Features:**
# MAGIC - Single unified alert per type (not 50+ individual alerts)
# MAGIC - Tiered thresholds (Warning/Critical)
# MAGIC - Custom alert DDL generation for complex business logic

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.77.0 pyyaml pydantic tenacity tabulate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from dpo.config import (
    OrchestratorConfig,
    DiscoveryConfig,
    ProfileConfig,
    AlertConfig,
    MonitoredTableConfig,
)
from dpo.alerting import AlertProvisioner

w = WorkspaceClient()

config = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.ml.churn_predictions": MonitoredTableConfig(
            label_column="churned",
            prediction_column="prediction",
        ),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        prediction_column="prediction",
        label_column="label",
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,
        null_rate_threshold=0.1,
        row_count_min=1000,
        distinct_count_min=None,
        default_notifications=["mlops@company.com"],
    ),
)

alerter = AlertProvisioner(w, config)

print("Alert Configuration:")
print(f"  Drift Threshold: {config.alerting.drift_threshold}")
print(f"  Null Rate Threshold: {config.alerting.null_rate_threshold}")
print(f"  Min Row Count: {config.alerting.row_count_min}")
print(f"  Notifications: {config.alerting.default_notifications}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drift Alert Setup
# MAGIC 
# MAGIC Creates a single unified alert that monitors ALL tables for feature drift.

# COMMAND ----------

unified_drift_view = "prod.global_monitoring.unified_drift_metrics_default"
catalog = "prod"

# Preview the alert query
threshold = config.alerting.drift_threshold
warning_threshold = threshold / 2

print("Drift Alert Query Preview:")
print(f"""
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
""")

# COMMAND ----------

# Create the drift alert (uncomment to execute)
# alert_id = alerter.create_unified_drift_alert(unified_drift_view, catalog)
# print(f"Drift Alert created: {alert_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Alert Setup
# MAGIC 
# MAGIC Creates an alert for data quality issues from profile_metrics.

# COMMAND ----------

unified_profile_view = "prod.global_monitoring.unified_profile_metrics_default"

# Preview the data quality alert conditions
null_threshold = config.alerting.null_rate_threshold
row_min = config.alerting.row_count_min
distinct_min = config.alerting.distinct_count_min

print("Data Quality Alert Query Preview:")
print(f"""
SELECT 
    source_table_name,
    owner,
    department,
    column_name,
    null_rate,
    record_count,
    distinct_count,
    CASE 
        WHEN null_rate > {null_threshold or 'N/A'} THEN 'HIGH_NULL_RATE'
        WHEN record_count < {row_min or 'N/A'} THEN 'LOW_ROW_COUNT'
        WHEN distinct_count < {distinct_min or 'N/A'} THEN 'LOW_CARDINALITY'
    END as issue_type,
    window_start,
    window_end
FROM {unified_profile_view}
WHERE null_rate > {null_threshold or 0} 
   OR record_count < {row_min or 0}
   {f'OR distinct_count < {distinct_min}' if distinct_min else ''}
ORDER BY null_rate DESC, record_count ASC
LIMIT 100
""")

# COMMAND ----------

# Create the data quality alert (uncomment to execute)
# quality_alert_id = alerter.create_data_quality_alert(unified_profile_view, catalog)
# print(f"Data Quality Alert created: {quality_alert_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tiered Alert Queries
# MAGIC 
# MAGIC Separate Warning and Critical queries for more granular control.

# COMMAND ----------

queries = alerter.generate_tiered_alert_queries(unified_drift_view, catalog)

for name, sql in queries.items():
    print(f"\n{'='*60}")
    print(f"Query: {name}")
    print("="*60)
    print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Custom Alert DDL
# MAGIC 
# MAGIC Generate DDL for complex business rules (created paused for DS review).

# COMMAND ----------

# Example: Alert for specific table with 60-day rolling window
custom_ddl = alerter.generate_custom_alert_ddl(
    unified_view=unified_drift_view,
    table_name="prod.retail.customer_predictions",
    business_rule="window_end >= current_date() - INTERVAL 60 DAYS AND js_divergence >= 0.15",
    alert_name="Custom: 60-Day Retail Drift Alert",
)

print(custom_ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Alert Status Dashboard Query
# MAGIC 
# MAGIC Query to see which tables have active alerts.

# COMMAND ----------

status_query = alerter.get_alert_status_query(unified_drift_view)
print("Alert Status Query:")
print(status_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Combined Alert Summary
# MAGIC 
# MAGIC A query that combines drift and quality issues into one view:

# COMMAND ----------

combined_query = f"""
-- Combined Alert Summary
WITH drift_issues AS (
    SELECT 
        source_table_name,
        owner,
        'DRIFT' as issue_category,
        column_name as affected_column,
        js_divergence as metric_value,
        'JS Divergence' as metric_name,
        CASE WHEN js_divergence >= {threshold} THEN 'CRITICAL' ELSE 'WARNING' END as severity
    FROM {unified_drift_view}
    WHERE js_divergence >= {warning_threshold}
),
quality_issues AS (
    SELECT 
        source_table_name,
        owner,
        'QUALITY' as issue_category,
        column_name as affected_column,
        null_rate as metric_value,
        'Null Rate' as metric_name,
        'WARNING' as severity
    FROM {unified_profile_view}
    WHERE null_rate > {null_threshold or 0.1}
)
SELECT * FROM drift_issues
UNION ALL
SELECT * FROM quality_issues
ORDER BY severity DESC, metric_value DESC
"""

print("Combined Alert Summary Query:")
print(combined_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. How to Manually Create Alerts
# MAGIC 
# MAGIC If you prefer to create alerts manually in the UI:
# MAGIC 
# MAGIC ### Drift Alerts:
# MAGIC 1. Go to **Databricks SQL** > **Queries**
# MAGIC 2. Create a new query with the drift alert SQL from Section 2
# MAGIC 3. Go to **Alerts** > **Create Alert**
# MAGIC 4. Select your query
# MAGIC 5. Configure:
# MAGIC    - **Trigger when**: `js_divergence` >= 0.1
# MAGIC    - **Refresh**: Every 1 hour
# MAGIC    - **Notifications**: Add email/webhook destinations
# MAGIC 
# MAGIC ### Data Quality Alerts:
# MAGIC 1. Create a new query with the data quality SQL from Section 3
# MAGIC 2. Create alert with:
# MAGIC    - **Trigger when**: `null_rate` > 0.1 OR `record_count` < 1000
# MAGIC    - **Refresh**: Every 1 hour
# MAGIC 
# MAGIC ### Custom Business Logic Alerts:
# MAGIC 1. Copy the DDL from Section 5
# MAGIC 2. Create the query
# MAGIC 3. Create the alert in **PAUSED** state
# MAGIC 4. Review and activate when ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Notification Destinations
# MAGIC 
# MAGIC Set up notification destinations in Databricks SQL before creating alerts:
# MAGIC 
# MAGIC ```sql
# MAGIC -- List existing destinations
# MAGIC SHOW ALERT DESTINATIONS;
# MAGIC 
# MAGIC -- Alerts can be sent to:
# MAGIC -- - Email
# MAGIC -- - Slack
# MAGIC -- - PagerDuty
# MAGIC -- - Webhooks
# MAGIC ```
# MAGIC 
# MAGIC Configure in **Databricks SQL** > **SQL Admin Console** > **Alert Destinations**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC - Configure notification destinations in Databricks SQL
# MAGIC - Set up Slack/PagerDuty integrations
# MAGIC - Create custom dashboards for specific departments
# MAGIC - Consider creating separate alerts per department using tag filtering
