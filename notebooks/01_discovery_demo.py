# Databricks notebook source
# MAGIC %md
# MAGIC # DPO Discovery Demo
# MAGIC 
# MAGIC This notebook demonstrates the Discovery phase of the Data Profiling Orchestrator.
# MAGIC 
# MAGIC **What you'll learn:**
# MAGIC - How to scan Unity Catalog for tables tagged for monitoring
# MAGIC - How tag filtering and schema exclusion work
# MAGIC - How priority sorting ensures critical tables are provisioned first
# MAGIC - How to tag tables for different profile types

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.77.0 pyyaml pydantic tenacity tabulate

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup Configuration

# COMMAND ----------

from dpo.config import DiscoveryConfig
from dpo.discovery import TableDiscovery
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Target catalog
catalog = "prod"  # Change to your catalog

config = DiscoveryConfig(
    include_tags={"monitor_enabled": "true"},
    exclude_schemas=["information_schema", "tmp_*", "dev_*"],
)

print(f"Target catalog: {catalog}")
print(f"Required tags: {config.include_tags}")
print(f"Excluded schemas: {config.exclude_schemas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run Discovery

# COMMAND ----------

# Pass catalog separately to TableDiscovery
discovery = TableDiscovery(w, config, catalog)
tables = discovery.discover()

print(f"\nDiscovered {len(tables)} tables tagged for monitoring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect Discovered Tables

# COMMAND ----------

import pandas as pd

# Convert to DataFrame for display
if tables:
    df = pd.DataFrame([
        {
            "Table": t.full_name,
            "Priority": t.priority,
            "Has PK": t.has_primary_key,
            "Type": t.table_type,
            "Owner": t.tags.get("owner", "unknown"),
            "Department": t.tags.get("department", "unknown"),
            "Profile Type": t.tags.get("profile_type", "inference"),  # Default to inference
        }
        for t in tables
    ])
    display(df.sort_values("Priority"))
else:
    print("No tables found. Make sure you have tables tagged with:")
    print("  monitor_enabled = 'true'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview Table Details

# COMMAND ----------

# Show details of first table (if any)
if tables:
    table = tables[0]
    print(f"Table: {table.full_name}")
    print(f"Priority: {table.priority}")
    print(f"Has Primary Key: {table.has_primary_key}")
    print(f"\nTags:")
    for k, v in table.tags.items():
        print(f"  {k}: {v}")
    print(f"\nColumns ({len(table.columns)}):")
    for col in table.columns[:10]:
        print(f"  {col.name}: {col.type_text}")
    if len(table.columns) > 10:
        print(f"  ... and {len(table.columns) - 10} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. How to Tag Tables for Monitoring
# MAGIC 
# MAGIC ### Basic Tags (Required)
# MAGIC 
# MAGIC ```sql
# MAGIC -- Enable monitoring on a table
# MAGIC ALTER TABLE prod.retail.customer_predictions 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'monitor_priority' = '1',  -- Lower = more important (provisioned first)
# MAGIC     'owner' = 'ml-team',
# MAGIC     'department' = 'retail'
# MAGIC );
# MAGIC ```
# MAGIC 
# MAGIC ### Profile Type Tags (Optional)
# MAGIC 
# MAGIC DPO supports three profile types. Tag your tables to specify which type to use:
# MAGIC 
# MAGIC | Profile Type | Use Case | Example Tag |
# MAGIC |-------------|----------|-------------|
# MAGIC | **INFERENCE** | ML predictions | `'profile_type' = 'inference'` |
# MAGIC | **TIMESERIES** | Event/transaction data | `'profile_type' = 'timeseries'` |
# MAGIC | **SNAPSHOT** | Dimension/reference tables | `'profile_type' = 'snapshot'` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Tagging for Different Profile Types
# MAGIC 
# MAGIC ```sql
# MAGIC -- ML Prediction Table (INFERENCE profile)
# MAGIC ALTER TABLE prod.ml.churn_predictions 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'inference',
# MAGIC     'monitor_problem_type' = 'CLASSIFICATION',  -- or REGRESSION
# MAGIC     'monitor_priority' = '1',
# MAGIC     'owner' = 'ml-team'
# MAGIC );
# MAGIC 
# MAGIC -- Event Table (TIMESERIES profile)
# MAGIC ALTER TABLE prod.events.user_actions 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'timeseries',
# MAGIC     'timestamp_col' = 'event_time',  -- Optional override
# MAGIC     'owner' = 'analytics-team'
# MAGIC );
# MAGIC 
# MAGIC -- Dimension Table (SNAPSHOT profile)
# MAGIC ALTER TABLE prod.dimensions.products 
# MAGIC SET TAGS (
# MAGIC     'monitor_enabled' = 'true',
# MAGIC     'profile_type' = 'snapshot',
# MAGIC     'owner' = 'data-team'
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking Tags on a Table
# MAGIC 
# MAGIC **Important:** Unity Catalog Tags are NOT the same as Delta table properties!
# MAGIC - **UC Tags** (what DPO uses): Set with `ALTER TABLE ... SET TAGS`
# MAGIC - **Table Properties**: Set with `ALTER TABLE ... SET TBLPROPERTIES`
# MAGIC 
# MAGIC Use INFORMATION_SCHEMA to verify your tags:
# MAGIC 
# MAGIC ```sql
# MAGIC -- Query UC tags (recommended - this is what DPO uses)
# MAGIC SELECT catalog_name, schema_name, table_name, tag_name, tag_value
# MAGIC FROM prod.information_schema.table_tags
# MAGIC WHERE schema_name = 'retail'
# MAGIC   AND table_name = 'customer_predictions';
# MAGIC 
# MAGIC -- Check all tables with monitor_enabled tag
# MAGIC SELECT catalog_name, schema_name, table_name, tag_name, tag_value
# MAGIC FROM prod.information_schema.table_tags
# MAGIC WHERE tag_name = 'monitor_enabled' AND tag_value = 'true';
# MAGIC ```
# MAGIC 
# MAGIC **Note:** `DESCRIBE EXTENDED` shows table properties, NOT Unity Catalog tags.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Filtering by Profile Type
# MAGIC 
# MAGIC You can configure discovery to only find tables of a specific profile type:

# COMMAND ----------

# Find only INFERENCE tables
inference_config = DiscoveryConfig(
    include_tags={
        "monitor_enabled": "true",
        "profile_type": "inference",
    },
)

# Find only TIMESERIES tables
timeseries_config = DiscoveryConfig(
    include_tags={
        "monitor_enabled": "true",
        "profile_type": "timeseries",
    },
)

# Find only SNAPSHOT tables
snapshot_config = DiscoveryConfig(
    include_tags={
        "monitor_enabled": "true",
        "profile_type": "snapshot",
    },
)

print("Profile-specific discovery configs created!")
print("Use these to run DPO separately for each profile type.")

# Example: Run discovery for INFERENCE tables only
# inference_discovery = TableDiscovery(w, inference_config, catalog)
# inference_tables = inference_discovery.discover()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Proceed to:
# MAGIC - **02_full_orchestration** - Provision monitors, create views, set up alerts
# MAGIC - **03_alerting_setup** - Deep dive into drift and data quality alerts
# MAGIC - **04_profile_types_demo** - Learn about SNAPSHOT, TIMESERIES, and INFERENCE profiles
