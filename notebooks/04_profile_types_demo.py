# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Profile Types Demo
# MAGIC 
# MAGIC This notebook demonstrates the three Data Profiling profile types supported by DPO:
# MAGIC 
# MAGIC | Profile Type | Use Case | Key Features |
# MAGIC |-------------|----------|--------------|
# MAGIC | **SNAPSHOT** | Static data quality checks | Simplest - no time windows required |
# MAGIC | **TIMESERIES** | Time-windowed monitoring | Requires timestamp column |
# MAGIC | **INFERENCE** | ML model monitoring | Drift detection + model quality metrics |
# MAGIC 
# MAGIC **When to Use Each:**
# MAGIC - **SNAPSHOT**: Reference tables, dimension tables, slowly changing data
# MAGIC - **TIMESERIES**: Event logs, transactions, streaming data
# MAGIC - **INFERENCE**: ML model predictions, classification/regression outputs

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.68.0 pyyaml pydantic tenacity tabulate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from dpo.config import (
    OrchestratorConfig,
    DiscoveryConfig,
    ProfileConfig,
    AlertConfig,
    MonitoredTableConfig,
    CustomMetricConfig,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SNAPSHOT Profile
# MAGIC 
# MAGIC **Best for:** Static tables, dimension tables, lookup tables
# MAGIC 
# MAGIC **What it monitors:**
# MAGIC - Column statistics (null count, distinct count, mean, stddev, min, max)
# MAGIC - Data distribution changes between refreshes
# MAGIC - No time-windowed aggregation
# MAGIC 
# MAGIC **Configuration:** Simplest - no special settings required

# COMMAND ----------

config_snapshot = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.dimensions.customers": MonitoredTableConfig(),
        "prod.dimensions.products": MonitoredTableConfig(),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="SNAPSHOT",
        output_schema_name="monitoring_results",
        granularity="1 day",  # Refresh frequency
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        null_rate_threshold=0.01,  # Strict for reference data
        row_count_min=10000,  # Alert if dimension table shrinks
    ),
    dry_run=True,
)

print("SNAPSHOT Profile Configuration:")
print(f"  Profile Type: {config_snapshot.profile_defaults.profile_type}")
print(f"  Tables: {list(config_snapshot.monitored_tables.keys())}")
print(f"  No timestamp required!")
print(f"  Monitors: Column statistics, distribution changes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example SNAPSHOT Tables
# MAGIC 
# MAGIC ```sql
# MAGIC -- Tag dimension tables for SNAPSHOT monitoring
# MAGIC ALTER TABLE prod.dimensions.customers 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'snapshot',
# MAGIC     'owner' = 'data-team'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE prod.dimensions.products
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'snapshot'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. TIMESERIES Profile
# MAGIC 
# MAGIC **Best for:** Event tables, transaction logs, streaming data
# MAGIC 
# MAGIC **What it monitors:**
# MAGIC - Time-windowed statistics (hourly, daily, weekly aggregations)
# MAGIC - Trends over time
# MAGIC - Consecutive window drift
# MAGIC 
# MAGIC **Configuration:** Requires `timeseries_timestamp_column`

# COMMAND ----------

config_timeseries = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.events.page_views": MonitoredTableConfig(
            timestamp_column="event_time",
            slicing_exprs=["region", "platform"],
        ),
        "prod.transactions.orders": MonitoredTableConfig(
            timestamp_column="order_timestamp",
        ),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="TIMESERIES",
        output_schema_name="monitoring_results",
        granularity="1 day",  # Aggregation window
        timeseries_timestamp_column="event_time",  # Required!
        slicing_exprs=["region"],  # Optional segmentation
        max_slicing_cardinality=50,
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,
        null_rate_threshold=0.1,
    ),
    dry_run=True,
)

print("TIMESERIES Profile Configuration:")
print(f"  Profile Type: {config_timeseries.profile_defaults.profile_type}")
print(f"  Default Timestamp Column: {config_timeseries.profile_defaults.timeseries_timestamp_column}")
print(f"  Granularity: {config_timeseries.profile_defaults.granularity}")
print(f"  Tables: {list(config_timeseries.monitored_tables.keys())}")
print(f"  Monitors: Time-windowed statistics, trend detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example TIMESERIES Tables
# MAGIC 
# MAGIC ```sql
# MAGIC -- Tag event tables for TIMESERIES monitoring
# MAGIC ALTER TABLE prod.events.page_views 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'timeseries',
# MAGIC     'owner' = 'analytics-team'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE prod.transactions.orders
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'timeseries'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. INFERENCE Profile
# MAGIC 
# MAGIC **Best for:** ML model predictions, scoring tables
# MAGIC 
# MAGIC **What it monitors:**
# MAGIC - Feature drift (JS divergence, KS statistic)
# MAGIC - Model performance metrics (if labels available)
# MAGIC - Prediction distribution shifts
# MAGIC - Optional: Baseline comparison against training data
# MAGIC 
# MAGIC **Configuration:** Requires inference settings (prediction_column, etc.)

# COMMAND ----------

config_inference = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.ml.customer_churn_predictions": MonitoredTableConfig(
            baseline_table_name="prod.ml.churn_baseline",
            label_column="churned",
            prediction_column="churn_probability",
            timestamp_column="prediction_time",
            problem_type="PROBLEM_TYPE_CLASSIFICATION",
            slicing_exprs=["segment", "region"],
        ),
        "prod.ml.demand_forecasts": MonitoredTableConfig(
            label_column="actual_demand",
            prediction_column="predicted_demand",
            problem_type="PROBLEM_TYPE_REGRESSION",
        ),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        granularity="1 day",
        problem_type="PROBLEM_TYPE_CLASSIFICATION",
        prediction_column="prediction",
        timestamp_column="inference_time",
        label_column="actual_label",
        model_id_column="model_version",
        slicing_exprs=["model_version", "segment"],
        max_slicing_cardinality=50,
    ),
    alerting=AlertConfig(
        enable_aggregated_alerts=True,
        drift_threshold=0.2,  # JS divergence threshold
        null_rate_threshold=0.1,
        row_count_min=1000,
    ),
    dry_run=True,
)

print("INFERENCE Profile Configuration:")
print(f"  Profile Type: {config_inference.profile_defaults.profile_type}")
print(f"  Default Problem Type: {config_inference.profile_defaults.problem_type}")
print(f"  Default Prediction Column: {config_inference.profile_defaults.prediction_column}")
print(f"  Default Label Column: {config_inference.profile_defaults.label_column}")
print(f"  Tables: {list(config_inference.monitored_tables.keys())}")
print(f"  Monitors: Feature drift, prediction distribution, model quality")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example INFERENCE Tables
# MAGIC 
# MAGIC ```sql
# MAGIC -- Tag ML prediction tables for INFERENCE monitoring
# MAGIC ALTER TABLE prod.ml.customer_churn_predictions 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'inference',
# MAGIC     'monitor_problem_type' = 'CLASSIFICATION',
# MAGIC     'monitor_priority' = '1',
# MAGIC     'owner' = 'ml-team',
# MAGIC     'department' = 'customer-success'
# MAGIC );
# MAGIC 
# MAGIC ALTER TABLE prod.ml.demand_forecasts
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'inference',
# MAGIC     'monitor_problem_type' = 'REGRESSION',
# MAGIC     'owner' = 'supply-chain-ml'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comparison: Output Tables
# MAGIC 
# MAGIC Each profile type generates similar output tables in your monitoring schema:
# MAGIC 
# MAGIC | Output Table | SNAPSHOT | TIMESERIES | INFERENCE |
# MAGIC |-------------|----------|------------|-----------|
# MAGIC | `*_profile_metrics` | ✅ Column stats | ✅ Time-windowed stats | ✅ Feature stats |
# MAGIC | `*_drift_metrics` | ✅ Distribution drift | ✅ Consecutive drift | ✅ JS divergence, KS stat |
# MAGIC | Model quality metrics | ❌ | ❌ | ✅ Accuracy, F1, AUC |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Custom Metrics (All Profile Types)
# MAGIC 
# MAGIC Add business-specific metrics to any profile type:

# COMMAND ----------

custom_metrics_example = [
    CustomMetricConfig(
        name="revenue_total",
        metric_type="aggregate",
        input_columns=["revenue"],
        definition="SUM(revenue)",
        output_type="double",
    ),
    CustomMetricConfig(
        name="high_value_ratio",
        metric_type="aggregate",
        input_columns=["order_value"],
        definition="AVG(CASE WHEN order_value > 100 THEN 1.0 ELSE 0.0 END)",
        output_type="double",
    ),
    CustomMetricConfig(
        name="null_ratio_trend",
        metric_type="derived",
        input_columns=["null_count", "count"],
        definition="{{null_count}} / NULLIF({{count}}, 0)",
        output_type="double",
    ),
]

for metric in custom_metrics_example:
    print(f"\nMetric: {metric.name}")
    print(f"  Type: {metric.metric_type}")
    print(f"  Input: {metric.input_columns}")
    print(f"  Definition: {metric.definition}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Per-Table Baseline Tables (INFERENCE Profile)
# MAGIC 
# MAGIC For ML models, you can compare current data against a baseline (e.g., training data)
# MAGIC on a per-table basis:

# COMMAND ----------

config_with_baselines = OrchestratorConfig(
    catalog_name="prod",
    warehouse_id="YOUR_WAREHOUSE_ID",
    include_tagged_tables=False,
    
    monitored_tables={
        "prod.ml.churn_model_v1": MonitoredTableConfig(
            baseline_table_name="prod.ml.training_features_v1",
            label_column="churned",
            prediction_column="prediction",
        ),
        "prod.ml.churn_model_v2": MonitoredTableConfig(
            baseline_table_name="prod.ml.training_features_v2",  # Different baseline!
            label_column="churned",
            prediction_column="prediction",
        ),
    },
    
    profile_defaults=ProfileConfig(
        profile_type="INFERENCE",
        output_schema_name="monitoring_results",
        prediction_column="prediction",
        timestamp_column="inference_time",
    ),
    alerting=AlertConfig(drift_threshold=0.2),
)

print("Per-Table Baselines:")
for table, table_config in config_with_baselines.monitored_tables.items():
    print(f"  {table}: {table_config.baseline_table_name}")
print("\nDrift will be computed against each table's specific training data!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Running Different Profile Types
# MAGIC 
# MAGIC Run DPO separately for different profile types or combine in one run:

# COMMAND ----------

from dpo import run_orchestration

# Run 1: Monitor all ML models with INFERENCE
# report_inference = run_orchestration(config_inference)

# Run 2: Monitor event tables with TIMESERIES  
# report_timeseries = run_orchestration(config_timeseries)

# Run 3: Monitor dimension tables with SNAPSHOT
# report_snapshot = run_orchestration(config_snapshot)

print("Run each config separately to monitor different table types!")
print("\nOr use monitored_tables to combine different settings in one config.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. Define your tables in `monitored_tables` with appropriate settings
# MAGIC 2. Start with `dry_run=True` to preview monitor creation
# MAGIC 3. Execute with `dry_run=False` to create monitors
# MAGIC 4. Check the unified views for aggregated metrics
# MAGIC 5. Set up alerts for drift and data quality issues
