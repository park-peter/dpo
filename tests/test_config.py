"""Tests for DPO configuration module."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from dpo.config import (
    AlertConfig,
    CustomMetricConfig,
    DiscoveryConfig,
    MonitoredTableConfig,
    OrchestratorConfig,
    ProfileConfig,
    load_config,
)


class TestConfigLoading:
    """Tests for configuration loading."""

    def test_load_valid_config(self, tmp_path):
        """Test loading a valid YAML configuration."""
        config_content = """
catalog_name: "prod"
warehouse_id: "test_warehouse"
include_tagged_tables: false
monitored_tables:
  prod.ml.predictions:
    label_column: "label"
profile_defaults:
  profile_type: "INFERENCE"
  output_schema_name: "monitoring"
  prediction_column: "pred"
  timestamp_column: "ts"
alerting:
  drift_threshold: 0.2
"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        config = load_config(str(config_file))

        assert config.warehouse_id == "test_warehouse"
        assert config.catalog_name == "prod"
        assert config.profile_defaults.profile_type == "INFERENCE"

    def test_load_missing_file(self):
        """Test loading a non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent_config.yaml")

    def test_load_invalid_yaml(self, tmp_path):
        """Test loading invalid YAML raises ValidationError."""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: content:")

        with pytest.raises(Exception):
            load_config(str(config_file))

    def test_load_validation_error(self, tmp_path):
        """Test valid YAML with invalid config schema raises ValidationError."""
        config_content = """
catalog_name: "prod"
warehouse_id: "test_warehouse"
include_tagged_tables: false
monitored_tables:
  prod.ml.predictions: {}
profile_defaults:
  profile_type: "INFERENCE"
  output_schema_name: "monitoring"
  prediction_column: "pred"
"""
        config_file = tmp_path / "invalid_config.yaml"
        config_file.write_text(config_content)

        with pytest.raises(ValidationError):
            load_config(str(config_file))

    def test_default_profile_yaml_uses_safe_dry_run_default(self):
        """Repository starter config should default to dry_run=true."""
        repo_root = Path(__file__).resolve().parents[1]
        config = load_config(str(repo_root / "configs" / "default_profile.yaml"))

        assert config.dry_run is True


class TestDiscoveryConfig:
    """Tests for DiscoveryConfig validation."""

    def test_default_values(self):
        """Test default values are applied."""
        config = DiscoveryConfig()

        assert config.include_tags == {"monitor_enabled": "true"}
        assert "information_schema" in config.exclude_schemas

    def test_custom_tags(self):
        """Test custom include tags."""
        config = DiscoveryConfig(
            include_tags={"env": "prod", "team": "ml"},
        )

        assert config.include_tags["env"] == "prod"
        assert config.include_tags["team"] == "ml"


class TestMonitoredTableConfig:
    """Tests for MonitoredTableConfig validation."""

    def test_default_values(self):
        """Test default values are None."""
        config = MonitoredTableConfig()

        assert config.baseline_table_name is None
        assert config.label_column is None
        assert config.prediction_column is None
        assert config.granularity is None

    def test_full_config(self):
        """Test setting all values."""
        config = MonitoredTableConfig(
            baseline_table_name="cat.sch.baseline",
            label_column="label",
            prediction_column="prediction",
            timestamp_column="ts",
            problem_type="PROBLEM_TYPE_REGRESSION",
            granularity="1 hour",
            slicing_exprs=["region", "segment"],
        )

        assert config.baseline_table_name == "cat.sch.baseline"
        assert config.problem_type == "PROBLEM_TYPE_REGRESSION"
        assert len(config.slicing_exprs) == 2

    def test_invalid_granularity(self):
        """Test invalid per-table granularity raises validation error."""
        with pytest.raises(ValidationError):
            MonitoredTableConfig(granularity="2 days")

    def test_none_granularity_is_allowed(self):
        """Test explicit None granularity is accepted."""
        config = MonitoredTableConfig(granularity=None)
        assert config.granularity is None


class TestProfileConfig:
    """Tests for ProfileConfig validation."""

    def test_inference_profile(self):
        """Test inference profile configuration."""
        config = ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring",
            problem_type="PROBLEM_TYPE_CLASSIFICATION",
            prediction_column="pred",
            timestamp_column="ts",
        )

        assert config.profile_type == "INFERENCE"
        assert config.problem_type == "PROBLEM_TYPE_CLASSIFICATION"

    def test_timeseries_profile(self):
        """Test timeseries profile configuration."""
        config = ProfileConfig(
            profile_type="TIMESERIES",
            output_schema_name="monitoring",
            timeseries_timestamp_column="ts",
        )

        assert config.profile_type == "TIMESERIES"
        assert config.timeseries_timestamp_column == "ts"

    def test_snapshot_profile(self):
        """Test snapshot profile configuration."""
        config = ProfileConfig(
            profile_type="SNAPSHOT",
            output_schema_name="monitoring",
        )

        assert config.profile_type == "SNAPSHOT"

    def test_invalid_granularity(self):
        """Test invalid default granularity raises validation error."""
        with pytest.raises(ValidationError):
            ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
                granularity="2 days",
            )

    def test_custom_metrics(self):
        """Test custom metrics configuration."""
        config = ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring",
            prediction_column="prediction",
            timestamp_column="timestamp",
            custom_metrics=[
                CustomMetricConfig(
                    name="test_metric",
                    metric_type="aggregate",
                    input_columns=["col1"],
                    definition="SUM(col1)",
                    output_type="double",
                ),
            ],
        )

        assert len(config.custom_metrics) == 1
        assert config.custom_metrics[0].name == "test_metric"


class TestAlertConfig:
    """Tests for AlertConfig validation."""

    def test_default_values(self):
        """Test default alert configuration."""
        config = AlertConfig()

        assert config.enable_aggregated_alerts is True
        assert config.drift_threshold == 0.2
        assert config.null_rate_threshold is None

    def test_data_quality_thresholds(self):
        """Test data quality threshold configuration."""
        config = AlertConfig(
            drift_threshold=0.3,
            null_rate_threshold=0.1,
            row_count_min=500,
            distinct_count_min=10,
        )

        assert config.drift_threshold == 0.3
        assert config.null_rate_threshold == 0.1
        assert config.row_count_min == 500
        assert config.distinct_count_min == 10

    def test_drift_threshold_bounds(self):
        """Test drift threshold must be between 0 and 1."""
        with pytest.raises(ValidationError):
            AlertConfig(drift_threshold=1.5)

        with pytest.raises(ValidationError):
            AlertConfig(drift_threshold=-0.1)

    def test_alert_schedule_defaults(self):
        """Alert schedule should default to hourly UTC."""
        config = AlertConfig()

        assert config.alert_cron_schedule == "0 0 * * * ?"
        assert config.alert_timezone == "UTC"

    def test_alert_schedule_custom_values(self):
        """Custom schedule/timezone values should be accepted."""
        config = AlertConfig(
            alert_cron_schedule="0 0 6 * * ?",
            alert_timezone="America/Chicago",
        )

        assert config.alert_cron_schedule == "0 0 6 * * ?"
        assert config.alert_timezone == "America/Chicago"


class TestOrchestratorConfig:
    """Tests for OrchestratorConfig validation."""

    def test_output_schema_validation(self):
        """Test output_schema_name cannot contain dots."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="catalog.schema",  # Invalid!
                    prediction_column="prediction",
                    timestamp_column="timestamp",
                ),
            )

    def test_timeseries_requires_timestamp(self):
        """Test TIMESERIES profile requires timeseries_timestamp_column."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
                profile_defaults=ProfileConfig(
                    profile_type="TIMESERIES",
                    output_schema_name="monitoring",
                    # Missing timeseries_timestamp_column!
                ),
            )

    def test_inference_requires_prediction_column(self):
        """Test INFERENCE profile requires prediction_column."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column=None,
                    timestamp_column="timestamp",
                ),
            )

    def test_inference_requires_timestamp_column(self):
        """Test INFERENCE profile requires timestamp_column."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column="prediction",
                    timestamp_column=None,
                ),
            )

    def test_include_tagged_tables_requires_discovery(self):
        """Test include_tagged_tables=True requires discovery config."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=True,  # Enabled
                discovery=None,  # Missing!
                monitored_tables={},
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column="prediction",
                    timestamp_column="timestamp",
                ),
            )

    def test_no_tables_requires_monitored_tables(self):
        """Test include_tagged_tables=False requires monitored_tables."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,  # Disabled
                monitored_tables={},  # Empty!
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column="prediction",
                    timestamp_column="timestamp",
                ),
            )

    def test_catalog_name_mismatch(self):
        """Test tables must match catalog_name."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="prod",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={
                    "different_catalog.sch.tbl": MonitoredTableConfig(),  # Wrong catalog!
                },
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column="prediction",
                    timestamp_column="timestamp",
                ),
            )

    def test_table_name_must_be_3_level(self):
        """Test table names must be 3-level namespace."""
        with pytest.raises(ValidationError):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={
                    "schema.table": MonitoredTableConfig(),  # Only 2 levels!
                },
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column="prediction",
                    timestamp_column="timestamp",
                ),
            )

    def test_full_config(self, sample_config):
        """Test a complete valid configuration."""
        assert sample_config.warehouse_id == "test_warehouse_123"
        assert sample_config.catalog_name == "test_catalog"
        assert sample_config.profile_defaults.profile_type == "INFERENCE"
        assert sample_config.alerting.drift_threshold == 0.2

    def test_valid_config_with_discovery(self):
        """Test valid config with tag discovery."""
        config = OrchestratorConfig(
            catalog_name="prod",
            warehouse_id="test",
            include_tagged_tables=True,
            discovery=DiscoveryConfig(
                include_tags={"monitor_enabled": "true"},
            ),
            monitored_tables={},  # Empty is OK when using discovery
            profile_defaults=ProfileConfig(
                profile_type="INFERENCE",
                output_schema_name="monitoring",
                prediction_column="prediction",
                timestamp_column="timestamp",
            ),
        )

        assert config.include_tagged_tables is True
        assert config.discovery is not None


class TestCustomMetricConfig:
    """Tests for CustomMetricConfig validation."""

    def test_aggregate_metric(self):
        """Test aggregate metric configuration."""
        metric = CustomMetricConfig(
            name="sum_revenue",
            metric_type="aggregate",
            input_columns=["revenue"],
            definition="SUM(revenue)",
            output_type="double",
        )

        assert metric.metric_type == "aggregate"
        assert metric.definition == "SUM(revenue)"

    def test_derived_metric(self):
        """Test derived metric configuration."""
        metric = CustomMetricConfig(
            name="null_ratio",
            metric_type="derived",
            input_columns=["null_count", "count"],
            definition="{{null_count}} / {{count}}",
            output_type="double",
        )

        assert metric.metric_type == "derived"
        assert "{{null_count}}" in metric.definition

    def test_invalid_metric_type(self):
        """Test invalid metric type raises error."""
        with pytest.raises(ValidationError):
            CustomMetricConfig(
                name="test",
                metric_type="invalid",  # Invalid!
                input_columns=["col"],
                definition="test",
                output_type="double",
            )
