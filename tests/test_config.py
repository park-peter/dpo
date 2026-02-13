"""Tests for DPO configuration module."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from dpo.config import (
    AlertConfig,
    CustomMetricConfig,
    DiscoveryConfig,
    MonitoredTableConfig,
    ObjectiveFunctionConfig,
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


class TestObjectiveFunctionConfig:
    """Tests for ObjectiveFunctionConfig model."""

    def test_basic_objective_function(self):
        """Test creating a basic objective function."""
        obj = ObjectiveFunctionConfig(
            version="1.0",
            owner="ml_team",
            description="ROC AUC for churn",
            uc_function_name="catalog.schema.roc_auc_func",
            metric=CustomMetricConfig(
                name="rocauc_30d",
                metric_type="aggregate",
                input_columns=[":table"],
                definition="catalog.schema.roc_auc_func({{prediction_col}}, {{label_col}})",
                output_type="double",
            ),
        )
        assert obj.version == "1.0"
        assert obj.metric.name == "rocauc_30d"

    def test_no_id_field(self):
        """ObjectiveFunctionConfig should not have an 'id' field."""
        obj = ObjectiveFunctionConfig(
            metric=CustomMetricConfig(
                name="test", metric_type="aggregate",
                input_columns=["col"], definition="SUM(col)",
            ),
        )
        assert not hasattr(obj, "id") or "id" not in obj.model_fields

    def test_drift_metric_type_accepted(self):
        """Test drift metric type is accepted."""
        metric = CustomMetricConfig(
            name="drift_metric",
            metric_type="drift",
            input_columns=[":table"],
            definition="custom_drift({{current_df}}, {{base_df}})",
        )
        assert metric.metric_type == "drift"


class TestTemplateVariableValidator:
    """Tests for template variable model_validator on CustomMetricConfig."""

    def test_known_vars_pass_silently(self):
        """Known Databricks variables should not trigger warnings."""
        metric = CustomMetricConfig(
            name="test", metric_type="aggregate",
            input_columns=[":table"],
            definition="func({{prediction_col}}, {{label_col}})",
        )
        assert metric.definition  # no error

    def test_input_columns_treated_as_valid(self):
        """Variables matching input_columns names should be allowed."""
        metric = CustomMetricConfig(
            name="test", metric_type="derived",
            input_columns=["null_count", "count"],
            definition="{{null_count}} / {{count}}",
        )
        assert metric.definition

    def test_unknown_vars_warn(self, caplog):
        """Unknown template variables should emit a warning."""
        import logging
        with caplog.at_level(logging.WARNING, logger="dpo.config"):
            CustomMetricConfig(
                name="test", metric_type="aggregate",
                input_columns=["col"],
                definition="{{totally_unknown_var}}",
            )
        assert "totally_unknown_var" in caplog.text


class TestWithinLayerDuplicates:
    """Tests for within-layer duplicate metric name detection."""

    def test_profile_defaults_duplicates_raise(self):
        """Duplicate names within profile_defaults.custom_metrics should error."""
        with pytest.raises(ValidationError, match="Duplicate metric names"):
            ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
                custom_metrics=[
                    CustomMetricConfig(name="dup", metric_type="aggregate", input_columns=["a"], definition="SUM(a)"),
                    CustomMetricConfig(name="dup", metric_type="aggregate", input_columns=["b"], definition="SUM(b)"),
                ],
            )

    def test_per_table_duplicates_raise(self):
        """Duplicate names within per-table custom_metrics should error."""
        with pytest.raises(ValidationError, match="Duplicate metric names"):
            MonitoredTableConfig(
                custom_metrics=[
                    CustomMetricConfig(name="dup", metric_type="aggregate", input_columns=["a"], definition="SUM(a)"),
                    CustomMetricConfig(name="dup", metric_type="aggregate", input_columns=["b"], definition="SUM(b)"),
                ],
            )

    def test_cross_layer_duplicates_allowed(self):
        """Cross-layer duplicates (overrides) should be allowed."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={
                "test.sch.tbl": MonitoredTableConfig(
                    custom_metrics=[
                        CustomMetricConfig(name="metric_a", metric_type="aggregate", input_columns=["x"], definition="SUM(x)"),
                    ],
                ),
            },
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
                custom_metrics=[
                    CustomMetricConfig(name="metric_a", metric_type="aggregate", input_columns=["y"], definition="AVG(y)"),
                ],
            ),
        )
        assert config is not None  # no error


class TestObjectiveResolution:
    """Tests for objective function cross-reference and resolution."""

    def test_missing_objective_id_raises(self):
        """Referencing non-existent objective ID should error."""
        with pytest.raises(ValidationError, match="does not exist"):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={
                    "test.sch.tbl": MonitoredTableConfig(
                        objective_function_ids=["nonexistent"],
                    ),
                },
                profile_defaults=ProfileConfig(
                    profile_type="SNAPSHOT",
                    output_schema_name="monitoring",
                ),
                objective_functions={},
            )

    def test_resolved_objective_duplicate_names_raise(self):
        """Two objectives resolving to same metric name on one table should error."""
        with pytest.raises(ValidationError, match="same metric name"):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={
                    "test.sch.tbl": MonitoredTableConfig(
                        objective_function_ids=["obj1", "obj2"],
                    ),
                },
                profile_defaults=ProfileConfig(
                    profile_type="SNAPSHOT",
                    output_schema_name="monitoring",
                ),
                objective_functions={
                    "obj1": ObjectiveFunctionConfig(
                        metric=CustomMetricConfig(name="same_name", metric_type="aggregate", input_columns=["a"], definition="SUM(a)"),
                    ),
                    "obj2": ObjectiveFunctionConfig(
                        metric=CustomMetricConfig(name="same_name", metric_type="aggregate", input_columns=["b"], definition="AVG(b)"),
                    ),
                },
            )

    def test_template_compatibility_non_inference_raises(self):
        """Using {{prediction_col}} on non-INFERENCE profile_type should error."""
        with pytest.raises(ValidationError, match="SNAPSHOT"):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={
                    "test.sch.tbl": MonitoredTableConfig(
                        objective_function_ids=["obj1"],
                    ),
                },
                profile_defaults=ProfileConfig(
                    profile_type="SNAPSHOT",
                    output_schema_name="monitoring",
                ),
                objective_functions={
                    "obj1": ObjectiveFunctionConfig(
                        metric=CustomMetricConfig(
                            name="metric", metric_type="aggregate",
                            input_columns=[":table"],
                            definition="func({{prediction_col}})",
                        ),
                    ),
                },
            )

    def test_template_compatibility_missing_prediction_column_raises(self):
        """Using {{prediction_col}} without prediction_column configured should error."""
        with pytest.raises(ValidationError, match="prediction_column"):
            OrchestratorConfig(
                catalog_name="test",
                warehouse_id="test",
                include_tagged_tables=False,
                monitored_tables={
                    "test.sch.tbl": MonitoredTableConfig(
                        objective_function_ids=["obj1"],
                    ),
                },
                profile_defaults=ProfileConfig(
                    profile_type="INFERENCE",
                    output_schema_name="monitoring",
                    prediction_column=None,
                    timestamp_column="ts",
                ),
                objective_functions={
                    "obj1": ObjectiveFunctionConfig(
                        metric=CustomMetricConfig(
                            name="metric", metric_type="aggregate",
                            input_columns=[":table"],
                            definition="func({{prediction_col}})",
                        ),
                    ),
                },
            )

    def test_valid_objective_config_passes(self):
        """A valid objective function configuration should pass."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={
                "test.sch.tbl": MonitoredTableConfig(
                    objective_function_ids=["obj1"],
                    prediction_column="pred",
                    label_column="label",
                ),
            },
            profile_defaults=ProfileConfig(
                profile_type="INFERENCE",
                output_schema_name="monitoring",
                prediction_column="pred",
                timestamp_column="ts",
            ),
            objective_functions={
                "obj1": ObjectiveFunctionConfig(
                    version="1.0",
                    owner="ml_team",
                    uc_function_name="cat.sch.func",
                    metric=CustomMetricConfig(
                        name="rocauc", metric_type="aggregate",
                        input_columns=[":table"],
                        definition="cat.sch.func({{prediction_col}}, {{label_col}})",
                    ),
                ),
            },
        )
        assert "obj1" in config.objective_functions


class TestZeroObjectivePath:
    """Tests for zero-objective path behavior."""

    def test_no_objectives_passes_validation(self):
        """Config with no objectives should pass validation."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
        )
        assert config.objective_functions == {}

    def test_empty_objectives_no_validation_noise(self):
        """Empty objective_functions should trigger no objective-related checks."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
            objective_functions={},
        )
        assert config.objective_functions == {}


class TestExtendedGranularity:
    """Tests for extended granularity support."""

    @pytest.mark.parametrize("granularity", ["2 weeks", "3 weeks", "4 weeks", "1 year"])
    def test_new_granularity_values_accepted(self, granularity):
        """New granularity values should be accepted."""
        config = MonitoredTableConfig(granularity=granularity)
        assert config.granularity == granularity

    def test_multi_granularity_accepted(self):
        """Multiple granularities should be accepted."""
        config = MonitoredTableConfig(granularities=["1 day", "1 month"])
        assert config.granularities == ["1 day", "1 month"]

    def test_invalid_granularity_in_list_raises(self):
        """Invalid granularity in list should raise error."""
        with pytest.raises(ValidationError, match="Unsupported granularity"):
            MonitoredTableConfig(granularities=["1 day", "invalid"])

    def test_profile_multi_granularity(self):
        """ProfileConfig should accept multi-granularity."""
        config = ProfileConfig(
            profile_type="SNAPSHOT",
            output_schema_name="monitoring",
            granularities=["1 day", "1 week", "1 month"],
        )
        assert len(config.granularities) == 3
