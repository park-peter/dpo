# Data Profiling Orchestrator (DPO)

A "Declare and Deploy" solution for automating Databricks Data Profiling at scale across Unity Catalog.

## Overview

DPO eliminates manual UI-based configuration by providing:
- **Automated Discovery**: Scan Unity Catalog and find tables tagged for monitoring
- **Idempotent Provisioning**: Create/update Data Profiling monitors with smart diffing
- **All Profile Types**: Support for Snapshot, TimeSeries, and Inference profiles
- **Two Operation Modes**: Full mode for unified observability, Bulk mode for quick onboarding
- **Monitor Groups**: Per-team/department aggregation with separate views, alerts, and dashboards
- **Unified Metrics Views**: Single "Management Plane" aggregating all profile and drift metrics
- **Custom Metrics**: Define business-specific metrics alongside standard profiles
- **Alerting**: Tiered drift alerts and data quality alerts with per-group routing
- **Dashboard**: Auto-deployed Lakeview dashboard for global health visibility

## Installation

```bash
pip install -e .
```

For Databricks notebooks:
```python
%pip install databricks-sdk>=0.68.0 pyyaml pydantic tenacity tabulate
```

## Quick Start

1. **Tag your tables** in Unity Catalog:
   ```sql
   ALTER TABLE prod.retail.customer_predictions SET TAGS ('monitor_enabled' = 'true');
   ```

2. **Create a config file** (`configs/my_config.yaml`):
   ```yaml
   mode: "full"  # or "bulk_provision_only"
   warehouse_id: "your-warehouse-id"
   
   discovery:
     catalog_name: "prod"
     include_tags:
       monitor_enabled: "true"
   
   profile_defaults:
     profile_type: "INFERENCE"  # or SNAPSHOT, TIMESERIES
     output_schema_name: "monitoring_results"
     inference_settings:
       prediction_col: "prediction"
       timestamp_col: "timestamp"
   ```

3. **Run the orchestrator**:
   ```python
   from dpo import run_orchestration, load_config
   
   config = load_config("configs/my_config.yaml")
   report = run_orchestration(config)
   print(f"Created {report.monitors_created} monitors")
   ```

## Operation Modes

### Full Mode (default)

Complete pipeline with unified observability:

```yaml
mode: "full"
```

Creates:
- Individual monitors per table
- Unified views aggregating all metrics (per group)
- SQL Alerts (per group)
- Lakeview dashboards (per group)

### Bulk Provisioning Mode

Fastest onboarding path - creates monitors only:

```yaml
mode: "bulk_provision_only"
```

Use when:
- You have 100+ tables and want the fastest setup
- You prefer using Databricks' native per-table UI for results
- You don't need cross-table unified observability yet

## Monitor Groups

Segment monitoring by team, department, or use case. Each group gets separate views, alerts, and dashboards.

### Tagging Tables with Groups

```sql
-- ML Team tables
ALTER TABLE prod.ml.churn_predictions SET TAGS (
    'monitor_enabled' = 'true',
    'monitor_group' = 'ml_team'
);

-- Data Engineering tables  
ALTER TABLE prod.warehouse.sales_data SET TAGS (
    'monitor_enabled' = 'true',
    'monitor_group' = 'data_eng'
);

-- Tables without monitor_group go to "default" group
ALTER TABLE prod.shared.reference_data SET TAGS ('monitor_enabled' = 'true');
```

### Per-Group Alert Routing

Configure different notification destinations per group:

```yaml
alerting:
  enable_standard_alerts: true
  
  # Default notifications (fallback for groups without specific routing)
  default_notifications:
    - "mlops-alerts@company.com"
  
  # Per-group notification routing
  group_notifications:
    ml_team:
      - "ml-team@company.com"
      - "ml-oncall@company.com"
    data_eng:
      - "data-eng@company.com"
```

### Automatic Artifact Cleanup

When you rename a monitor group (e.g., "marketing" → "growth"), DPO automatically:
- Creates new views/alerts/dashboards for the new group
- Drops stale views from the old group
- Trashes stale dashboards from the old group

## Schema Filtering

Control which schemas to scan with `include_schemas` and `exclude_schemas`:

```yaml
discovery:
  catalog_name: "prod"
  
  # Only scan these schemas (glob patterns supported)
  include_schemas:
    - "ml_models"
    - "data_warehouse_*"
  
  # Exclude these schemas
  exclude_schemas:
    - "information_schema"
    - "tmp_*"
    - "dev_*"
```

## Profile Types

DPO supports all three Databricks Data Profiling types:

| Type | Use Case | Key Config |
|------|----------|------------|
| **SNAPSHOT** | Static data quality checks on any table | None (simplest) |
| **TIMESERIES** | Time-windowed data quality monitoring | `timeseries_settings.timestamp_col` |
| **INFERENCE** | ML model monitoring with drift + model quality | `inference_settings.*` (prediction, label cols) |

### Snapshot Profile

Best for static tables where you want to track data quality over time:

```yaml
profile_defaults:
  profile_type: "SNAPSHOT"
  output_schema_name: "monitoring_results"
```

### TimeSeries Profile

Best for event data with timestamps:

```yaml
profile_defaults:
  profile_type: "TIMESERIES"
  granularity: "1 day"
  output_schema_name: "monitoring_results"
  timeseries_settings:
    timestamp_col: "event_time"
```

### Inference Profile

Best for ML model inference tables:

```yaml
profile_defaults:
  profile_type: "INFERENCE"
  granularity: "1 day"
  output_schema_name: "monitoring_results"
  inference_settings:
    problem_type: "PROBLEM_TYPE_CLASSIFICATION"
    prediction_col: "prediction"
    prediction_score_col: "prediction_proba"
    label_col: "label"
    timestamp_col: "timestamp"
```

## Custom Metrics

Define business-specific metrics computed alongside standard profiles:

```yaml
profile_defaults:
  custom_metrics:
    - name: "revenue_per_order"
      metric_type: "aggregate"
      input_columns: ["revenue", "order_count"]
      definition: "SUM(revenue) / COUNT(order_count)"
      output_type: "double"
    - name: "null_ratio_change"
      metric_type: "derived"
      input_columns: ["null_count", "count"]
      definition: "{{null_count}} / {{count}}"
      output_type: "double"
```

## Baseline Tables

Optionally specify a reference dataset for drift comparison:

```yaml
profile_defaults:
  baseline_table: "prod.ml.training_data_v2"
```

When specified, drift is computed relative to the baseline (e.g., training data) instead of just consecutive time windows.

## Alerting

DPO creates two types of alerts:

1. **Drift Alerts**: Jensen-Shannon divergence thresholds (Warning/Critical)
2. **Data Quality Alerts**: Null rate, row count, cardinality checks

```yaml
alerting:
  drift_threshold: 0.2
  null_rate_threshold: 0.1
  row_count_min: 1000
  distinct_count_min: 10
  default_notifications:
    - "mlops-alerts@company.com"
```

## Configuration Reference

See `configs/default_profile.yaml` for full configuration options.

### Key Settings

| Setting | Description |
|---------|-------------|
| `mode` | `full` or `bulk_provision_only` |
| `monitor_group_tag` | Tag name for grouping (default: "monitor_group") |
| `warehouse_id` | SQL Warehouse for statement execution |
| `discovery.catalog_name` | Target catalog to scan |
| `discovery.include_tags` | Tags required to enroll a table |
| `discovery.include_schemas` | Only scan these schemas (optional) |
| `discovery.exclude_schemas` | Schemas to skip (optional) |
| `profile_defaults.profile_type` | INFERENCE, SNAPSHOT, or TIMESERIES |
| `profile_defaults.output_schema_name` | Where metric tables are created |
| `profile_defaults.baseline_table` | Optional baseline for drift comparison |
| `profile_defaults.custom_metrics` | Business-specific metrics |
| `alerting.drift_threshold` | JS divergence threshold for alerts |
| `alerting.default_notifications` | Default notification destinations |
| `alerting.group_notifications` | Per-group notification routing |
| `dry_run` | Preview changes without executing |
| `cleanup_orphans` | Remove monitors for untagged tables |

## Project Structure

```
DPO/
├── src/
│   └── dpo/
│       ├── __init__.py       # Main entry point + run_orchestration()
│       ├── config.py         # Pydantic config models
│       ├── discovery.py      # Phase 1: UC table discovery
│       ├── provisioning.py   # Phase 2: Monitor lifecycle
│       ├── aggregator.py     # Phase 3: Unified views (per group)
│       ├── alerting.py       # Phase 4: SQL Alerts (per group)
│       ├── dashboard.py      # Phase 5: Lakeview dashboard (per group)
│       └── utils.py          # Shared utilities
├── configs/
│   └── default_profile.yaml
├── notebooks/
│   ├── 01_discovery_demo.py
│   ├── 02_full_orchestration.py
│   ├── 03_alerting_setup.py
│   ├── 04_profile_types_demo.py
│   ├── 05_bulk_provisioning.py
│   └── 06_monitor_groups_demo.py
├── tests/
├── pyproject.toml
└── README.md
```

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Unity Catalog  │────▶│    Discovery     │────▶│  Target Tables  │
│    (Tables)     │     │  (Tag Filtering) │     │  (by Group)     │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                         │
                       ┌──────────────────┐              │
                       │   Provisioning   │◀─────────────┘
                       │  (Parallel/Rate  │
                       │    Limited)      │
                       └────────┬─────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  drift_metrics  │     │ profile_metrics  │     │   Dashboard     │
│    (per table)  │     │   (per table)    │     │  (per group)    │
└────────┬────────┘     └────────┬─────────┘     └─────────────────┘
        │                       │
        ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Per-Group Aggregation                        │
├─────────────────┬──────────────────┬──────────────────┐        │
│    ml_team      │    data_eng      │    default       │        │
├─────────────────┼──────────────────┼──────────────────┤        │
│ Drift View      │ Drift View       │ Drift View       │        │
│ Profile View    │ Profile View     │ Profile View     │        │
│ Drift Alert     │ Drift Alert      │ Drift Alert      │        │
│ Quality Alert   │ Quality Alert    │ Quality Alert    │        │
│ Dashboard       │ Dashboard        │ Dashboard        │        │
└─────────────────┴──────────────────┴──────────────────┴────────┘
```

## API Reference

### Core Functions

```python
from dpo import (
    run_orchestration,      # Full or bulk mode based on config
    run_bulk_provisioning,  # Explicit bulk mode
    load_config,
    OrchestrationReport,
)
from dpo.discovery import TableDiscovery
from dpo.provisioning import ProfileProvisioner
from dpo.aggregator import MetricsAggregator
from dpo.alerting import AlertProvisioner
from dpo.dashboard import DashboardProvisioner
from dpo.utils import sanitize_sql_identifier
```

### OrchestrationReport Fields

| Field | Type | Description |
|-------|------|-------------|
| `tables_discovered` | `int` | Number of tables found with monitor tags |
| `monitors_created` | `int` | Number of new monitors created |
| `monitors_updated` | `int` | Number of existing monitors updated |
| `monitors_skipped` | `int` | Number of tables skipped (quota, cardinality) |
| `monitors_failed` | `int` | Number of failed provisioning attempts |
| `orphans_cleaned` | `int` | Number of orphaned monitors removed |
| `unified_drift_views` | `Dict[str, str]` | Group → drift view name |
| `unified_profile_views` | `Dict[str, str]` | Group → profile view name |
| `drift_alert_ids` | `Dict[str, str]` | Group → drift alert ID |
| `quality_alert_ids` | `Dict[str, str]` | Group → quality alert ID |
| `dashboard_ids` | `Dict[str, str]` | Group → dashboard ID |

## Requirements

- Databricks workspace with Unity Catalog enabled
- `databricks-sdk>=0.68.0` (uses new `data_quality` API)
- SQL Warehouse for statement execution
- Appropriate permissions on target catalog/schema

## License

GPL v3
