"""Configuration models for DPO."""

import logging
from typing import Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, ValidationError


class DiscoveryConfig(BaseModel):
    """Configuration for finding tables in Unity Catalog."""

    catalog_name: str = Field(..., description="Target catalog to scan")
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


class InferenceConfig(BaseModel):
    """Specific settings for Model Inference monitors."""

    problem_type: Optional[
        Literal["PROBLEM_TYPE_CLASSIFICATION", "PROBLEM_TYPE_REGRESSION"]
    ] = "PROBLEM_TYPE_CLASSIFICATION"
    prediction_col: str = "prediction"
    prediction_score_col: Optional[str] = None
    label_col: Optional[str] = None
    model_id_col: Optional[str] = None
    timestamp_col: Optional[str] = None


class TimeSeriesConfig(BaseModel):
    """Settings for TimeSeries profile type."""

    timestamp_col: str = Field(..., description="Column containing timestamps")


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


class ProfileConfig(BaseModel):
    """The master template for a Data Profiling monitor."""

    profile_type: Literal["INFERENCE", "SNAPSHOT", "TIMESERIES"] = "INFERENCE"
    granularity: str = "1 day"
    output_schema_name: str = Field(
        ..., description="Schema where metric tables will be created"
    )
    baseline_table: Optional[str] = Field(
        None, description="Optional baseline table for drift comparison"
    )
    slicing_columns: List[str] = Field(
        default=[],
        description="List of column names to slice by",
    )
    max_slicing_cardinality: int = Field(
        50,
        description="Skip slicing columns with more distinct values",
    )
    create_builtin_dashboard: bool = Field(
        True,
        description="If False, skips creating per-monitor Databricks dashboards",
    )
    inference_settings: Optional[InferenceConfig] = Field(
        default_factory=InferenceConfig
    )
    timeseries_settings: Optional[TimeSeriesConfig] = Field(
        None, description="Settings for TIMESERIES profile type"
    )
    custom_metrics: List[CustomMetricConfig] = Field(
        default=[], description="Custom metrics to compute"
    )


class OrchestratorConfig(BaseModel):
    """Root configuration object for the DPO Application."""

    mode: Literal["full", "bulk_provision_only"] = Field(
        "full",
        description="'full' runs complete pipeline; 'bulk_provision_only' creates monitors only",
    )
    monitor_group_tag: str = Field(
        "monitor_group",
        description="Tag name used to group tables for separate aggregation/alerting. "
        "Tables without this tag go to 'default' group.",
    )
    warehouse_id: str = Field(
        ..., description="SQL Warehouse ID for pre-flight checks"
    )
    discovery: DiscoveryConfig
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
        1200, description="Timeout in seconds when waiting for monitors (default 10 min)"
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
        if v.profile_type == "TIMESERIES" and v.timeseries_settings is None:
            raise ValueError(
                "timeseries_settings required when profile_type is TIMESERIES"
            )
        return v


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
