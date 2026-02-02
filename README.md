# Data Profiling Orchestrator (DPO)

A "Declare and Deploy" solution for automating Databricks Data Profiling at scale across Unity Catalog.

## Overview

DPO eliminates manual UI-based configuration by providing:
- **Config-Driven Tables**: Define tables explicitly in YAML with per-table settings
- **Optional Tag Discovery**: Discover additional tables via Unity Catalog tags
- **Per-Table Configuration**: Baseline tables, label columns, granularity per table
- **All Profile Types**: Support for Snapshot, TimeSeries, and Inference profiles
- **Two Operation Modes**: Full mode for unified observability, Bulk mode for quick onboarding
- **Monitor Groups**: Per-team/department aggregation with separate views, alerts, and dashboards
- **Schema Validation**: Pre-flight column existence checks before creating monitors
- **Unified Metrics Views**: Single "Management Plane" aggregating all profile and drift metrics

## Installation

```bash
pip install -e .
```

For Databricks notebooks:
```python
%pip install databricks-sdk>=0.68.0 pyyaml pydantic tenacity tabulate
```

## Quick Start

### Option 1: Config-Driven (Recommended)

Define tables explicitly in YAML - version-controlled and reproducible:

```yaml
# configs/my_config.yaml
catalog_name: "prod"
warehouse_id: "your-warehouse-id"
mode: "full"

profile_defaults:
  profile_type: "INFERENCE"
  output_schema_name: "monitoring_results"
  prediction_column: "prediction"
  label_column: "label"
  timestamp_column: "timestamp"

monitored_tables:
  prod.ml.churn_predictions:
    baseline_table_name: "prod.ml.churn_baseline"
    label_column: "churned"
    prediction_column: "churn_probability"
    
  prod.ml.fraud_detection:
    label_column: "is_fraud"
    granularity: "1 hour"
```

```python
from dpo import run_orchestration, load_config

config = load_config("configs/my_config.yaml")
report = run_orchestration(config)
print(f"Created {report.monitors_created} monitors")
```

### Option 2: Tag-Based Discovery

Discover tables via Unity Catalog tags:

```sql
ALTER TABLE prod.ml.churn_predictions SET TAGS ('monitor_enabled' = 'true');
```

```yaml
catalog_name: "prod"
warehouse_id: "your-warehouse-id"
include_tagged_tables: true

discovery:
  include_tags:
    monitor_enabled: "true"
  exclude_schemas:
    - "tmp_*"

profile_defaults:
  profile_type: "INFERENCE"
  output_schema_name: "monitoring_results"

monitored_tables: {}  # Can be empty when using discovery
```

### Option 3: Hybrid Mode

Use both - YAML config takes precedence for tables that appear in both:

```yaml
catalog_name: "prod"
include_tagged_tables: true  # Discover tagged tables

discovery:
  include_tags: {monitor_enabled: "true"}

monitored_tables:
  # These settings override any tags for this specific table
  prod.ml.churn_predictions:
    baseline_table_name: "prod.ml.churn_baseline"
    label_column: "churned"
```

## Per-Table Configuration

Each table in `monitored_tables` can override global defaults:

```yaml
monitored_tables:
  prod.ml.churn_model:
    baseline_table_name: "prod.ml.churn_baseline"
    label_column: "churned"
    prediction_column: "churn_probability"
    timestamp_column: "prediction_time"
    problem_type: "PROBLEM_TYPE_CLASSIFICATION"
    granularity: "1 day"
    slicing_exprs:
      - "region"
      - "customer_segment"
```

Available per-table settings (all optional, inherit from `profile_defaults`):

| Setting | Description |
|---------|-------------|
| `baseline_table_name` | Baseline table for drift comparison |
| `label_column` | Ground truth label column |
| `prediction_column` | Model prediction column |
| `timestamp_column` | Timestamp column |
| `model_id_column` | Model version/ID column |
| `problem_type` | CLASSIFICATION or REGRESSION |
| `granularity` | Aggregation window ("1 hour", "1 day", etc.) |
| `slicing_exprs` | Columns to slice metrics by |

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

Segment monitoring by team, department, or use case:

```sql
ALTER TABLE prod.ml.churn_predictions SET TAGS (
    'monitor_enabled' = 'true',
    'monitor_group' = 'ml_team'
);
```

### Per-Group Alert Routing

```yaml
alerting:
  enable_aggregated_alerts: true
  default_notifications:
    - "mlops-alerts@company.com"
  group_notifications:
    ml_team:
      - "ml-team@company.com"
    data_eng:
      - "data-eng@company.com"
```

## Profile Types

| Type | Use Case | Key Config |
|------|----------|------------|
| **SNAPSHOT** | Static data quality checks | None (simplest) |
| **TIMESERIES** | Time-windowed data quality | `timeseries_timestamp_column` |
| **INFERENCE** | ML model monitoring with drift | `prediction_column`, `label_column` |

### Inference Profile

```yaml
profile_defaults:
  profile_type: "INFERENCE"
  problem_type: "PROBLEM_TYPE_CLASSIFICATION"
  prediction_column: "prediction"
  label_column: "label"
  timestamp_column: "timestamp"
```

### TimeSeries Profile

```yaml
profile_defaults:
  profile_type: "TIMESERIES"
  timeseries_timestamp_column: "event_time"
  granularity: "1 day"
```

### Snapshot Profile

```yaml
profile_defaults:
  profile_type: "SNAPSHOT"
```

## Schema Validation

DPO validates that configured columns exist before creating monitors:

```
ValueError: Column 'label_v1' (from label_column) not found in table 
'prod.ml.churn'. Available columns: ['id', 'prediction', 'actual_label', 'ts']
```

This prevents cryptic API errors and provides clear guidance.

## Configuration Reference

### Top-Level Settings

| Setting | Required | Description |
|---------|----------|-------------|
| `catalog_name` | Yes | Target catalog - all tables must be in this catalog |
| `warehouse_id` | Yes | SQL Warehouse for statement execution |
| `mode` | No | `full` (default) or `bulk_provision_only` |
| `include_tagged_tables` | No | If true, discover tables via UC tags (default: false) |
| `monitored_tables` | Conditional | Required if `include_tagged_tables` is false |
| `discovery` | Conditional | Required if `include_tagged_tables` is true |

### Profile Defaults

| Setting | Description |
|---------|-------------|
| `profile_type` | INFERENCE, SNAPSHOT, or TIMESERIES |
| `output_schema_name` | Where metric tables are created |
| `granularity` | Default aggregation window |
| `prediction_column` | Default prediction column (inference) |
| `label_column` | Default label column (inference) |
| `timestamp_column` | Default timestamp column |
| `problem_type` | Default problem type (inference) |
| `slicing_exprs` | Default slicing expressions |
| `create_builtin_dashboard` | Create per-monitor dashboards |

### Alerting

| Setting | Description |
|---------|-------------|
| `enable_aggregated_alerts` | Create alerts on unified views |
| `drift_threshold` | JS divergence threshold (default: 0.2) |
| `null_rate_threshold` | Alert if null rate exceeds threshold |
| `row_count_min` | Alert if row count falls below |
| `default_notifications` | Default notification destinations |
| `group_notifications` | Per-group notification routing |

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

## API Reference

```python
from dpo import (
    run_orchestration,
    run_bulk_provisioning,
    load_config,
    OrchestrationReport,
    get_monitor_statuses,
    wait_for_monitors,
    print_monitor_statuses,
)
```

### OrchestrationReport Fields

| Field | Type | Description |
|-------|------|-------------|
| `tables_discovered` | `int` | Number of tables processed |
| `monitors_created` | `int` | New monitors created |
| `monitors_updated` | `int` | Existing monitors updated |
| `monitors_skipped` | `int` | Tables skipped (quota, validation) |
| `monitors_failed` | `int` | Failed provisioning attempts |
| `monitor_statuses` | `List[MonitorStatus]` | Final status of each monitor |
| `unified_drift_views` | `Dict[str, str]` | Group → drift view name |
| `unified_profile_views` | `Dict[str, str]` | Group → profile view name |
| `drift_alert_ids` | `Dict[str, str]` | Group → drift alert ID |
| `dashboard_ids` | `Dict[str, str]` | Group → dashboard ID |

## Requirements

- Databricks workspace with Unity Catalog enabled
- `databricks-sdk>=0.68.0` (uses `data_quality` API)
- SQL Warehouse for statement execution
- Appropriate permissions on target catalog/schema

## License

GPL v3
