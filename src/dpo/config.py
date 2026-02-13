"""Configuration models for DPO."""

import logging
import re
from typing import Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

logger = logging.getLogger(__name__)

ALLOWED_GRANULARITIES = {
    "5 minutes",
    "30 minutes",
    "1 hour",
    "1 day",
    "1 week",
    "2 weeks",
    "3 weeks",
    "4 weeks",
    "1 month",
    "1 year",
}

KNOWN_TEMPLATE_VARIABLES = {
    "input_column",
    "prediction_col",
    "label_col",
    "current_df",
    "base_df",
}

INFERENCE_ONLY_VARS = {"prediction_col", "label_col"}
INFERENCE_VAR_REQUIREMENTS = {
    "prediction_col": "prediction_column",
    "label_col": "label_column",
}


class DiscoveryConfig(BaseModel):
    """Configuration for finding tables in Unity Catalog via tags."""

    include_tags: Dict[str, str] = Field(
        default={"monitor_enabled": "true"},
        description="Key-value pair of tags required to enroll a table",
    )
    exclude_schemas: List[str] = Field(
        default=["information_schema", "tmp_*", "dev_*"],
        description="List of schemas (glob patterns supported) to skip",
    )
    include_schemas: Optional[List[str]] = Field(
        None,
        description="If specified, ONLY scan these schemas (glob patterns supported). "
        "When None, scans all schemas except those in exclude_schemas.",
    )


class PolicyConfig(BaseModel):
    """Lightweight policy checks for config validation."""

    naming_patterns: List[str] = Field(
        default=[],
        description="Regex patterns that table names must match (e.g., '^prod\\\\..*')",
    )
    required_tags: List[str] = Field(
        default=[],
        description="Tag keys that must be present on monitored tables (e.g., ['owner', 'department'])",
    )
    forbidden_patterns: List[str] = Field(
        default=[],
        description="Regex patterns that table names must NOT match (e.g., '.*_tmp$', '.*_test$')",
    )
    require_baseline: bool = Field(
        False,
        description="Require baseline_table_name for all inference monitors",
    )
    require_slicing: bool = Field(
        False,
        description="Require at least one slicing_expr per table",
    )
    max_tables_per_config: Optional[int] = Field(
        None,
        ge=1,
        description="Maximum number of tables allowed in a single config file",
    )


class AlertConfig(BaseModel):
    """Defaults for generated alerts."""

    enable_aggregated_alerts: bool = True
    drift_threshold: float = Field(
        0.2,
        ge=0.0,
        le=1.0,
        description="JS divergence threshold for CRITICAL alerts",
    )
    null_rate_threshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Alert if null rate exceeds this threshold",
    )
    row_count_min: Optional[int] = Field(
        None,
        ge=0,
        description="Alert if row count falls below this minimum",
    )
    distinct_count_min: Optional[int] = Field(
        None,
        ge=0,
        description="Alert if distinct count falls below this minimum",
    )
    default_notifications: List[str] = Field(
        default=[],
        description="Default notification destinations for groups without specific routing",
    )
    group_notifications: Dict[str, List[str]] = Field(
        default={},
        description="Per-group notification routing. Key is group name, value is list of destinations.",
    )
    alert_cron_schedule: str = Field(
        "0 0 * * * ?",
        description="Quartz cron expression for alert evaluation frequency (default: hourly)",
    )
    alert_timezone: str = Field(
        "UTC",
        description="Timezone for alert schedule evaluation",
    )


class CustomMetricConfig(BaseModel):
    """Custom metric definition per Databricks Data Profiling docs.

    Definitions use Databricks Jinja syntax -- DPO passes them through verbatim.
    Supported template variables: {{input_column}}, {{prediction_col}},
    {{label_col}}, {{current_df}}, {{base_df}}, and input_columns names.
    """

    name: str = Field(..., description="Unique metric name")
    metric_type: Literal["aggregate", "derived", "drift"] = Field(
        ..., description="Type of metric calculation"
    )
    input_columns: List[str] = Field(
        ..., description="Columns used in calculation"
    )
    definition: str = Field(
        ..., description="Jinja-templated SQL expression"
    )
    output_type: Literal["double", "string"] = Field(
        "double", description="Output data type"
    )

    @model_validator(mode="after")
    def _warn_unknown_template_vars(self) -> "CustomMetricConfig":
        """Warn about template variables not in the known Databricks set or input_columns."""
        found = set(re.findall(r"\{\{\s*(\w+)\s*\}\}", self.definition))
        allowed = KNOWN_TEMPLATE_VARIABLES | set(self.input_columns or [])
        unknown = found - allowed
        if unknown:
            logger.warning(
                "Definition contains template variables not in the known Databricks set "
                "or input_columns: %s. Databricks-supported variables: %s. "
                "input_columns for this metric: %s",
                unknown, KNOWN_TEMPLATE_VARIABLES, self.input_columns,
            )
        return self


class ObjectiveFunctionConfig(BaseModel):
    """Reusable objective function definition.

    The dict key in objective_functions serves as the unique identifier.
    Complex logic should be implemented as UC SQL functions (CREATE FUNCTION),
    then referenced in the metric definition Jinja template.
    Definitions use Databricks Jinja syntax -- DPO passes them through verbatim.
    Supported template variables: {{input_column}}, {{prediction_col}},
    {{label_col}}, {{current_df}}, {{base_df}}, and input_columns names.
    """

    version: str = Field("1.0", description="Semantic version for tracking changes")
    owner: Optional[str] = Field(None, description="Team or individual owning this objective")
    description: Optional[str] = Field(None, description="Human-readable description")
    uc_function_name: Optional[str] = Field(
        None,
        description="Fully-qualified UC SQL function name (e.g., 'catalog.schema.roc_auc_func'). "
        "DPO validates this function exists during 'dpo validate --check-workspace' and at run start.",
    )
    metric: CustomMetricConfig = Field(
        ..., description="The custom metric definition that implements this objective"
    )


class MonitoredTableConfig(BaseModel):
    """Per-table configuration. Parameter names mirror SDK API."""

    baseline_table_name: Optional[str] = Field(
        None, description="Baseline table for drift comparison"
    )
    slicing_exprs: Optional[List[str]] = Field(
        None, description="Slicing expressions for this table"
    )
    granularity: Optional[str] = Field(
        None, description="Aggregation granularity (e.g., '1 day', '1 hour')"
    )
    granularities: Optional[List[str]] = Field(
        None,
        description="Multiple aggregation granularities (e.g., ['1 day', '1 month']). "
        "Takes precedence over singular 'granularity' field.",
    )
    problem_type: Optional[
        Literal["PROBLEM_TYPE_CLASSIFICATION", "PROBLEM_TYPE_REGRESSION"]
    ] = Field(None, description="Problem type for inference monitors")
    prediction_column: Optional[str] = Field(
        None, description="Column containing predictions"
    )
    label_column: Optional[str] = Field(
        None, description="Column containing ground truth labels"
    )
    timestamp_column: Optional[str] = Field(
        None, description="Column containing timestamps"
    )
    model_id_column: Optional[str] = Field(
        None, description="Column containing model version/ID"
    )
    owner: Optional[str] = Field(
        None, description="Table owner (overrides UC tag fallback)"
    )
    runbook_url: Optional[str] = Field(
        None, description="Runbook URL for alert actionability"
    )
    lineage_url: Optional[str] = Field(
        None, description="Lineage URL for data provenance context"
    )
    drift_threshold: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Per-table JS divergence threshold (overrides alerting.drift_threshold)",
    )
    objective_function_ids: Optional[List[str]] = Field(
        None,
        description="List of objective function ids from the registry to apply to this table.",
    )
    custom_metrics: Optional[List[CustomMetricConfig]] = Field(
        None,
        description="Per-table custom metrics. Merged with profile_defaults.custom_metrics "
        "and resolved objective_function metrics at provisioning time.",
    )

    @field_validator("granularity")
    @classmethod
    def validate_granularity(cls, v: Optional[str]) -> Optional[str]:
        """Validate optional per-table granularity values."""
        if v is None:
            return v
        if v not in ALLOWED_GRANULARITIES:
            allowed = ", ".join(sorted(ALLOWED_GRANULARITIES))
            raise ValueError(
                f"Unsupported granularity '{v}'. Allowed values: {allowed}"
            )
        return v

    @field_validator("granularities")
    @classmethod
    def validate_granularities_list(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate each entry in the granularities list."""
        if v is None:
            return v
        for g in v:
            if g not in ALLOWED_GRANULARITIES:
                allowed = ", ".join(sorted(ALLOWED_GRANULARITIES))
                raise ValueError(
                    f"Unsupported granularity '{g}' in granularities list. Allowed values: {allowed}"
                )
        return v

    @model_validator(mode="after")
    def _check_within_layer_duplicates(self) -> "MonitoredTableConfig":
        """Disallow duplicate metric names within per-table custom_metrics."""
        if self.custom_metrics:
            names = [m.name for m in self.custom_metrics]
            dupes = [n for n in names if names.count(n) > 1]
            if dupes:
                raise ValueError(f"Duplicate metric names within custom_metrics: {set(dupes)}")
        return self


class ProfileConfig(BaseModel):
    """The master template for a Data Profiling monitor with SDK-style names."""

    profile_type: Literal["INFERENCE", "SNAPSHOT", "TIMESERIES"] = "INFERENCE"
    granularity: str = "1 day"
    granularities: Optional[List[str]] = Field(
        None,
        description="Multiple aggregation granularities (e.g., ['1 day', '1 month']). "
        "Takes precedence over singular 'granularity' field.",
    )
    output_schema_name: str = Field(
        ..., description="Schema where metric tables will be created"
    )
    slicing_exprs: List[str] = Field(
        default=[],
        description="List of slicing expressions",
    )
    max_slicing_cardinality: int = Field(
        50,
        description="Skip slicing columns with more distinct values",
    )
    create_builtin_dashboard: bool = Field(
        True,
        description="If False, skips creating per-monitor Databricks dashboards",
    )
    problem_type: Optional[
        Literal["PROBLEM_TYPE_CLASSIFICATION", "PROBLEM_TYPE_REGRESSION"]
    ] = Field("PROBLEM_TYPE_CLASSIFICATION", description="Default problem type")
    prediction_column: Optional[str] = Field(
        "prediction", description="Default prediction column"
    )
    label_column: Optional[str] = Field(
        None, description="Default label column"
    )
    timestamp_column: Optional[str] = Field(
        None, description="Default timestamp column"
    )
    model_id_column: Optional[str] = Field(
        None, description="Default model ID column"
    )
    timeseries_timestamp_column: Optional[str] = Field(
        None, description="Timestamp column for TIMESERIES profile type"
    )
    custom_metrics: List[CustomMetricConfig] = Field(
        default=[], description="Custom metrics to compute"
    )

    @field_validator("granularity")
    @classmethod
    def validate_granularity(cls, v: str) -> str:
        """Validate default granularity value."""
        if v not in ALLOWED_GRANULARITIES:
            allowed = ", ".join(sorted(ALLOWED_GRANULARITIES))
            raise ValueError(
                f"Unsupported granularity '{v}'. Allowed values: {allowed}"
            )
        return v

    @field_validator("granularities")
    @classmethod
    def validate_granularities_list(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate each entry in the granularities list."""
        if v is None:
            return v
        for g in v:
            if g not in ALLOWED_GRANULARITIES:
                allowed = ", ".join(sorted(ALLOWED_GRANULARITIES))
                raise ValueError(
                    f"Unsupported granularity '{g}' in granularities list. Allowed values: {allowed}"
                )
        return v

    @model_validator(mode="after")
    def _check_within_layer_duplicates(self) -> "ProfileConfig":
        """Disallow duplicate metric names within profile_defaults custom_metrics."""
        if self.custom_metrics:
            names = [m.name for m in self.custom_metrics]
            dupes = [n for n in names if names.count(n) > 1]
            if dupes:
                raise ValueError(f"Duplicate metric names within custom_metrics: {set(dupes)}")
        return self


class OrchestratorConfig(BaseModel):
    """Root configuration object for the DPO Application."""

    catalog_name: str = Field(..., description="Target catalog - all tables must be in this catalog")
    warehouse_id: str = Field(..., description="SQL Warehouse ID for pre-flight checks")
    mode: Literal["full", "bulk_provision_only"] = Field(
        "full",
        description="'full' runs complete pipeline; 'bulk_provision_only' creates monitors only",
    )
    include_tagged_tables: bool = Field(
        False,
        description="If True, discover tables via UC tags. If False, only use monitored_tables.",
    )
    discovery: Optional[DiscoveryConfig] = Field(
        None,
        description="Discovery config. Required when include_tagged_tables=True.",
    )
    monitored_tables: Dict[str, MonitoredTableConfig] = Field(
        default={},
        description="Per-table configuration. Key is full table name (catalog.schema.table).",
    )
    monitor_group_tag: str = Field(
        "monitor_group",
        description="Tag name used to group tables for separate aggregation/alerting. "
        "Tables without this tag go to 'default' group.",
    )
    profile_defaults: ProfileConfig
    alerting: AlertConfig = Field(default_factory=AlertConfig)
    policy: Optional[PolicyConfig] = Field(
        None,
        description="Policy checks applied during validation (naming, tags, forbidden patterns)",
    )
    dry_run: bool = Field(False, description="Preview changes only")
    cleanup_orphans: bool = Field(
        False, description="Delete monitors for untagged tables"
    )
    deploy_aggregated_dashboard: bool = Field(
        True, description="Auto-deploy aggregated Lakeview dashboard"
    )
    deploy_executive_rollup: bool = Field(
        True,
        description="Deploy a cross-group executive rollup dashboard when multiple groups exist",
    )
    dashboard_parent_path: str = Field(
        "/Workspace/Shared/DPO", description="Path for dashboard deployment"
    )
    wait_for_monitors: bool = Field(
        True,
        description="Wait for all monitors to reach ACTIVE status before aggregation",
    )
    wait_timeout_seconds: int = Field(
        1200, description="Timeout in seconds when waiting for monitors"
    )
    wait_poll_interval: int = Field(
        20, description="Seconds between status checks when waiting"
    )
    stale_monitor_days: int = Field(
        30,
        ge=1,
        description="Number of days without a refresh before a monitor is considered stale",
    )
    objective_functions: Dict[str, ObjectiveFunctionConfig] = Field(
        default={},
        description="Registry of reusable objective function definitions. "
        "Key is the objective function identifier.",
    )

    @field_validator("profile_defaults")
    @classmethod
    def validate_output_location(cls, v: ProfileConfig) -> ProfileConfig:
        """Validate output_schema_name format."""
        if "." in v.output_schema_name:
            raise ValueError(
                "output_schema_name should be simple name, not catalog.schema"
            )
        return v

    @field_validator("profile_defaults")
    @classmethod
    def validate_profile_settings(cls, v: ProfileConfig) -> ProfileConfig:
        """Validate profile-type-specific settings are provided."""
        if v.profile_type == "TIMESERIES" and v.timeseries_timestamp_column is None:
            raise ValueError(
                "timeseries_timestamp_column required when profile_type is TIMESERIES"
            )
        if v.profile_type == "INFERENCE":
            if v.prediction_column is None:
                raise ValueError(
                    "prediction_column required when profile_type is INFERENCE"
                )
            if v.timestamp_column is None:
                raise ValueError(
                    "timestamp_column required when profile_type is INFERENCE"
                )
        return v

    @model_validator(mode="after")
    def validate_config(self) -> "OrchestratorConfig":
        """Validate cross-field configuration requirements."""
        if self.include_tagged_tables and self.discovery is None:
            raise ValueError("discovery required when include_tagged_tables=True")

        if not self.include_tagged_tables and not self.monitored_tables:
            raise ValueError("monitored_tables required when include_tagged_tables=False")

        for table_name in self.monitored_tables.keys():
            parts = table_name.split(".")
            if len(parts) != 3:
                raise ValueError(
                    f"Table '{table_name}' must be 3-level namespace: catalog.schema.table"
                )
            if parts[0] != self.catalog_name:
                raise ValueError(
                    f"Table '{table_name}' catalog '{parts[0]}' does not match "
                    f"config catalog_name '{self.catalog_name}'"
                )

        # --- Objective function cross-reference checks ---
        tables_with_objectives = {
            name: cfg
            for name, cfg in self.monitored_tables.items()
            if cfg.objective_function_ids
        }

        if tables_with_objectives:
            for table_name, table_cfg in tables_with_objectives.items():
                for obj_id in table_cfg.objective_function_ids:
                    if obj_id not in self.objective_functions:
                        raise ValueError(
                            f"Table '{table_name}' references objective function '{obj_id}' "
                            f"which does not exist in the objective_functions registry. "
                            f"Available: {list(self.objective_functions.keys())}"
                        )

            # Resolved-objective duplicate detection per table
            for table_name, table_cfg in tables_with_objectives.items():
                obj_metric_names = []
                for obj_id in table_cfg.objective_function_ids:
                    obj = self.objective_functions[obj_id]
                    obj_metric_names.append(obj.metric.name)
                dupes = [n for n in obj_metric_names if obj_metric_names.count(n) > 1]
                if dupes:
                    raise ValueError(
                        f"Table '{table_name}' has multiple objectives resolving to the same "
                        f"metric name: {set(dupes)}. Use distinct metric names or remove duplicates."
                    )

            # Template compatibility validation (per-variable)
            for table_name, table_cfg in tables_with_objectives.items():
                for obj_id in table_cfg.objective_function_ids:
                    obj = self.objective_functions[obj_id]
                    definition = obj.metric.definition
                    found_vars = set(re.findall(r"\{\{\s*(\w+)\s*\}\}", definition))
                    used_inference_vars = found_vars & INFERENCE_ONLY_VARS
                    if not used_inference_vars:
                        continue

                    effective_profile_type = (
                        getattr(table_cfg, "profile_type", None)
                        or self.profile_defaults.profile_type
                    )
                    if effective_profile_type != "INFERENCE":
                        raise ValueError(
                            f"Objective '{obj_id}' uses inference template variables "
                            f"{used_inference_vars} but table '{table_name}' resolves to "
                            f"profile_type='{effective_profile_type}', not 'INFERENCE'."
                        )

                    for tpl_var, config_field in INFERENCE_VAR_REQUIREMENTS.items():
                        if tpl_var not in found_vars:
                            continue
                        has_config = (
                            getattr(table_cfg, config_field, None) is not None
                            or getattr(self.profile_defaults, config_field, None) is not None
                        )
                        if not has_config:
                            raise ValueError(
                                f"Objective '{obj_id}' uses {{{{{tpl_var}}}}} "
                                f"but table '{table_name}' has no {config_field} configured "
                                f"(checked table-level and profile_defaults)."
                            )

        return self


def load_config(
    config_path: str = "configs/default_profile.yaml",
) -> OrchestratorConfig:
    """Load and validate YAML configuration.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Validated OrchestratorConfig instance.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValidationError: If config validation fails.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        config = OrchestratorConfig(**raw_config)
        logging.info("Configuration loaded successfully from %s", config_path)
        return config

    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Configuration file not found at: {config_path}"
        ) from exc
    except ValidationError as e:
        logging.error("Configuration Validation Failed:\n%s", e)
        raise
