"""Configuration models for DPO."""

import logging
from typing import Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

ALLOWED_GRANULARITIES = {
    "5 minutes",
    "30 minutes",
    "1 hour",
    "1 day",
    "1 week",
    "1 month",
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

    Args:
        name: Unique name for the metric.
        metric_type: Either 'aggregate' (SQL expression) or 'derived' (Jinja template).
        input_columns: List of columns used in the metric calculation.
        definition: SQL expression for aggregate, Jinja template for derived.
        output_type: Data type of the metric output.
    """

    name: str = Field(..., description="Unique metric name")
    metric_type: Literal["aggregate", "derived"] = Field(
        ..., description="Type of metric calculation"
    )
    input_columns: List[str] = Field(
        ..., description="Columns used in calculation"
    )
    definition: str = Field(
        ..., description="SQL for aggregate, Jinja for derived"
    )
    output_type: Literal["double", "string"] = Field(
        "double", description="Output data type"
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


class ProfileConfig(BaseModel):
    """The master template for a Data Profiling monitor with SDK-style names."""

    profile_type: Literal["INFERENCE", "SNAPSHOT", "TIMESERIES"] = "INFERENCE"
    granularity: str = "1 day"
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
    dry_run: bool = Field(False, description="Preview changes only")
    cleanup_orphans: bool = Field(
        False, description="Delete monitors for untagged tables"
    )
    deploy_aggregated_dashboard: bool = Field(
        True, description="Auto-deploy aggregated Lakeview dashboard"
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
