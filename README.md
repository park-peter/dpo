# Data Profiling Orchestrator (DPO)

A "Declare and Deploy" solution for automating Databricks Data Profiling at scale across Unity Catalog.

## Overview

DPO eliminates manual UI-based configuration by providing:
- **CLI-First Workflow**: `dpo validate`, `dpo dry-run`, `dpo run`, `dpo coverage` via a `click`-based CLI
- **Config-Driven Tables**: Define tables explicitly in YAML with per-table settings
- **Optional Tag Discovery**: Discover additional tables via Unity Catalog tags
- **Per-Table Configuration**: Baseline tables, label columns, granularity, enrichment metadata per table
- **All Profile Types**: Support for Snapshot, TimeSeries, and Inference profiles
- **Two Operation Modes**: Full mode for unified observability, Bulk mode for quick onboarding
- **Monitor Groups**: Per-team/department aggregation with separate views, alerts, and dashboards
- **Schema Validation**: Pre-flight column existence checks before creating monitors
- **Policy Governance**: Configurable naming rules, required tags, and structural constraints
- **Coverage Governance**: Detect unmonitored tables, stale monitors, and orphaned monitors
- **Enriched Alerting**: Alerts carry owner, runbook URL, and lineage URL for actionable triage
- **Unified Metrics Views**: Single "Management Plane" aggregating all profile and drift metrics

## Installation

```bash
pip install databricks-dpo
```

For local development:
```bash
pip install -e .
```

Or from a git clone:
```bash
make install
```

For Databricks notebooks (Python API execution path):
```python
%pip install databricks-dpo
```

## Execution Contexts

- **Local shell / CI/CD runners**: use the `dpo` CLI (`validate`, `dry-run`, `run`, `coverage`).
- **Databricks notebooks / notebook jobs**: use the Python API (`load_config`, `run_orchestration`).

The notebook path is API-driven; running the CLI inside notebook cells is not the recommended operating model.

## Quick Start

### Option 1: CLI (Local + CI/CD Recommended)

The `dpo` CLI is safe by default - validate first, then preview, then execute:

```bash
# 1. Validate config syntax and policy rules
dpo validate configs/my_config.yaml

# 2. Preview what would change (no mutations)
dpo dry-run configs/my_config.yaml

# 3. Execute for real (requires explicit --confirm)
dpo run configs/my_config.yaml --confirm

# 4. Check governance coverage
dpo coverage configs/my_config.yaml
```

### Option 2: Config-Driven (Python API for Notebooks/Jobs)

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

### Option 3: Tag-Based Discovery

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

### Option 4: Hybrid Mode

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
| `owner` | Table owner for alert routing (falls back to UC tag `owner`) |
| `runbook_url` | Runbook URL included in alert payloads |
| `lineage_url` | Lineage URL included in alert payloads |
| `drift_threshold` | Per-table JS divergence threshold (overrides global `alerting.drift_threshold`) |

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

## CLI Reference

DPO ships a `click`-based CLI installed as the `dpo` console script.
Use this CLI from a local shell or CI/CD runner.
For Databricks notebooks/jobs, use the Python API shown above.

| Command | Description | Safe by Default |
|---------|-------------|-----------------|
| `dpo validate <config>` | Schema + policy validation (offline by default, `--check-workspace` for workspace checks) | Yes |
| `dpo dry-run <config>` | Preview planned actions with a structured Impact Report | Yes |
| `dpo run <config> --confirm` | Execute orchestration (requires explicit `--confirm` flag) | Yes |
| `dpo coverage <config>` | Report unmonitored, stale, and orphaned monitors | Yes |

All commands support `--verbose` for DEBUG logging and `--format json` for machine-readable output.

`dpo dry-run` always enforces dry-run mode and `dpo run --confirm` always enforces live mode, regardless of the YAML `dry_run` value.

**Exit codes:** `0` success, `1` validation failure, `2` usage error, `3` runtime error.

## Policy Governance

Define structural rules that all configurations must satisfy:

```yaml
# In your config or a standalone policy file
policy:
  naming_patterns:
    - "^prod\\..*"                # Tables must be in prod catalog
  required_tags:
    - "owner"
    - "team"
  forbidden_patterns:
    - ".*_tmp$"                   # No temporary tables
    - ".*_test$"                  # No test tables
  require_baseline: true          # All inference monitors need a baseline
  require_slicing: true           # At least one slicing expression per table
  max_tables_per_config: 500      # Limit config scope
```

Policy templates are available in `configs/policies/`:
- `default_policy.yaml` - Permissive defaults
- `strict_policy.yaml` - Production-grade constraints

## Coverage Governance

The `dpo coverage` command analyzes monitoring gaps:

| Metric | Description |
|--------|-------------|
| **Unmonitored tables** | Tables in the catalog that have no profiling monitor |
| **Stale monitors** | Monitors that haven't refreshed in `stale_monitor_days` (default 30) |
| **Orphan monitors** | Monitors whose source tables no longer exist |

```bash
dpo coverage configs/production.yaml --format json
```

The Lakeview dashboard includes a dedicated **Coverage Governance** page backed by `CoverageAnalyzer` output from the latest orchestration run, including a snapshot timestamp plus unmonitored/stale/orphan monitor detail tables.

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
| `policy` | No | Policy governance rules (see Policy Governance section) |
| `stale_monitor_days` | No | Days without refresh before a monitor is considered stale (default: 30) |

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
| `alert_cron_schedule` | Quartz cron schedule for alert evaluation (default hourly) |
| `alert_timezone` | Timezone for alert schedule evaluation (default `UTC`) |
| `default_notifications` | Default notification destinations |
| `group_notifications` | Per-group notification routing |

## Project Structure

```
DPO/
├── src/
│   └── dpo/
│       ├── __init__.py       # Main entry point + run_orchestration()
│       ├── __main__.py       # Module entry point (python -m dpo)
│       ├── cli.py            # click-based CLI (dpo validate/dry-run/run/coverage)
│       ├── config.py         # Pydantic config models (incl. PolicyConfig)
│       ├── coverage.py       # Coverage governance (unmonitored/stale/orphan)
│       ├── discovery.py      # UC table discovery + enrichment resolution
│       ├── provisioning.py   # Monitor lifecycle + structured ImpactReport
│       ├── aggregator.py     # Unified views with enrichment metadata
│       ├── alerting.py       # SQL Alerts with owner/runbook/lineage
│       ├── dashboard.py      # Lakeview dashboard + coverage page
│       └── utils.py          # Shared utilities
├── configs/
│   ├── default_profile.yaml
│   └── policies/
│       ├── default_policy.yaml
│       └── strict_policy.yaml
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
    ImpactReport,
    CoverageAnalyzer,
    CoverageReport,
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
| `impact_report` | `Optional[ImpactReport]` | Structured dry-run impact report |
| `coverage_report` | `Optional[CoverageReport]` | Coverage governance analysis |

## Requirements

- Databricks workspace with Unity Catalog enabled
- `databricks-sdk>=0.77.0` (uses `data_quality` API)
- `click>=8.0` (CLI framework)
- SQL Warehouse for statement execution
- Appropriate permissions on target catalog/schema

## License

GPL v3
