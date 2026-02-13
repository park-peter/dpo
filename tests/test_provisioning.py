"""Tests for DPO provisioning module."""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.dataquality import (
    AggregationGranularity,
    DataProfilingConfig,
    InferenceLogConfig,
    InferenceProblemType,
)

from dpo.config import MonitoredTableConfig
from dpo.discovery import DiscoveredTable
from dpo.provisioning import ImpactReport, ProfileProvisioner, ProvisioningResult
from tests.conftest import create_mock_column


class TestImpactReport:
    """Tests for ImpactReport class."""

    def test_add_and_count_actions(self):
        """Test adding actions and counting them."""
        report = ImpactReport()

        report.add("table1", "create", "")
        report.add("table2", "create", "")
        report.add("table3", "update", "config drift")
        report.add("table4", "skip_quota", "quota reached")

        actions = [p["action"] for p in report.planned_actions]

        assert actions.count("create") == 2
        assert actions.count("update") == 1
        assert actions.count("skip_quota") == 1

    def test_print_summary_with_more_than_20_entries(self, capsys):
        """Test summary includes overflow count when >20 planned actions."""
        report = ImpactReport()
        for i in range(25):
            report.add(f"cat.sch.tbl_{i}", "create", "")

        report.print_summary()
        output = capsys.readouterr().out

        assert "DPO DRY RUN - IMPACT REPORT" in output
        assert "... and 5 more tables" in output


class TestProfileProvisioner:
    """Tests for ProfileProvisioner class."""

    def test_granularity_map(self, mock_workspace_client, sample_config):
        """Test granularity mapping."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        assert "1 day" in provisioner.GRANULARITY_MAP
        assert "1 hour" in provisioner.GRANULARITY_MAP
        assert provisioner.GRANULARITY_MAP["1 day"] == (
            AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY
        )

    def test_resolve_setting_from_monitored_tables(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test _resolve_setting uses monitored_tables first."""
        sample_config.monitored_tables["test_catalog.test_schema.test_table"] = (
            MonitoredTableConfig(
                label_column="custom_label",
                granularity="1 hour",
            )
        )
        sample_config.profile_defaults.label_column = "default_label"
        sample_config.profile_defaults.granularity = "1 day"

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        label = provisioner._resolve_setting(sample_discovered_table, "label_column")
        assert label == "custom_label"

        granularity = provisioner._resolve_setting(sample_discovered_table, "granularity")
        assert granularity == "1 hour"

    def test_resolve_setting_falls_back_to_defaults(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test _resolve_setting falls back to profile_defaults."""
        sample_config.monitored_tables["test_catalog.test_schema.test_table"] = (
            MonitoredTableConfig()  # Empty - no overrides
        )
        sample_config.profile_defaults.label_column = "default_label"

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        label = provisioner._resolve_setting(sample_discovered_table, "label_column")
        assert label == "default_label"

    def test_resolve_setting_for_unknown_table(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test _resolve_setting for table not in monitored_tables."""
        sample_discovered_table.full_name = "other_catalog.other_schema.other_table"
        sample_config.profile_defaults.label_column = "default_label"

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        label = provisioner._resolve_setting(sample_discovered_table, "label_column")
        assert label == "default_label"

    def test_validate_columns_exist_success(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test column validation passes when columns exist."""
        sample_config.profile_defaults.label_column = "label"
        sample_config.profile_defaults.prediction_column = "prediction"
        sample_config.profile_defaults.timestamp_column = "timestamp"

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        # Should not raise
        provisioner._validate_columns_exist(sample_discovered_table)

    def test_validate_columns_exist_failure(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test column validation raises error for missing column."""
        # Clear the monitored_tables entry to use profile_defaults
        sample_config.monitored_tables.pop(sample_discovered_table.full_name, None)
        sample_config.profile_defaults.label_column = "nonexistent_column"

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        with pytest.raises(ValueError) as exc_info:
            provisioner._validate_columns_exist(sample_discovered_table)

        assert "nonexistent_column" in str(exc_info.value)
        assert "label_column" in str(exc_info.value)

    def test_validate_columns_exist_uses_resolved_settings(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test validation uses resolved settings (per-table over defaults)."""
        sample_config.monitored_tables["test_catalog.test_schema.test_table"] = (
            MonitoredTableConfig(
                label_column="label",  # Exists
            )
        )
        sample_config.profile_defaults.label_column = "nonexistent"  # Would fail

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        # Should not raise because per-table config uses existing column
        provisioner._validate_columns_exist(sample_discovered_table)

    def test_validate_columns_exist_snapshot_skips_inference_columns(
        self, mock_workspace_client, sample_snapshot_config
    ):
        """SNAPSHOT profile should not require prediction/timestamp/label columns."""
        table = DiscoveredTable(
            full_name="test_catalog.dimensions.products",
            columns=[create_mock_column("product_id", "STRING")],
        )

        provisioner = ProfileProvisioner(mock_workspace_client, sample_snapshot_config)

        # Should not raise
        provisioner._validate_columns_exist(table)

    def test_resolve_problem_type_from_monitored_tables(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type resolution from monitored_tables config."""
        sample_config.monitored_tables["test_catalog.test_schema.test_table"] = (
            MonitoredTableConfig(
                problem_type="PROBLEM_TYPE_REGRESSION",
            )
        )

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

    def test_resolve_problem_type_from_tag(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type resolution from table tag."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        sample_discovered_table.tags["monitor_problem_type"] = "REGRESSION"

        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

    def test_resolve_problem_type_from_config(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type resolution from config."""
        sample_config.profile_defaults.problem_type = "PROBLEM_TYPE_REGRESSION"
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        sample_discovered_table.tags.pop("monitor_problem_type", None)

        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

    def test_resolve_problem_type_default(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type defaults to classification."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        sample_discovered_table.tags.pop("monitor_problem_type", None)
        sample_config.profile_defaults.problem_type = "PROBLEM_TYPE_CLASSIFICATION"

        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION

    def test_build_config_dict_inference(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test building config dict for INFERENCE profile."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        config_dict = provisioner._build_config_dict(sample_discovered_table)

        assert config_dict["profile_type"] == "INFERENCE"
        assert config_dict["output_schema_name"] == "monitoring_results"
        assert config_dict["granularity"] == "1 day"
        assert "prediction_column" in config_dict

    def test_build_config_dict_timeseries(
        self,
        mock_workspace_client,
        sample_timeseries_config,
        sample_discovered_table,
    ):
        """Test building config dict for TIMESERIES profile."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, sample_timeseries_config
        )

        config_dict = provisioner._build_config_dict(sample_discovered_table)

        assert config_dict["profile_type"] == "TIMESERIES"
        assert config_dict["timestamp_column"] == "event_time"

    def test_build_config_dict_snapshot(
        self,
        mock_workspace_client,
        sample_snapshot_config,
        sample_discovered_table,
    ):
        """Test building config dict for SNAPSHOT profile."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, sample_snapshot_config
        )

        config_dict = provisioner._build_config_dict(sample_discovered_table)

        assert config_dict["profile_type"] == "SNAPSHOT"

    def test_validate_slicing_columns_filters_missing(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test that missing slicing columns are filtered out."""
        sample_config.profile_defaults.slicing_exprs = [
            "region",  # Exists
            "nonexistent_column",  # Doesn't exist
        ]

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        safe_columns = provisioner._validate_slicing_columns(sample_discovered_table)

        assert len(safe_columns) == 1
        assert "`region`" in safe_columns

    def test_validate_slicing_columns_filters_high_cardinality(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test that high-cardinality columns are filtered out."""
        sample_config.profile_defaults.slicing_exprs = ["region"]
        sample_config.profile_defaults.max_slicing_cardinality = 5

        result = MagicMock()
        result.result.data_array = [[100]]  # 100 distinct values
        mock_workspace_client.statement_execution.execute_statement.return_value = result

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        safe_columns = provisioner._validate_slicing_columns(sample_discovered_table)

        assert len(safe_columns) == 0

    def test_get_monitor_count(self, mock_workspace_client, sample_config):
        """Test getting current monitor count."""
        mock_workspace_client.data_quality.list_monitor.return_value = [
            MagicMock(),
            MagicMock(),
            MagicMock(),
        ]

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        count = provisioner._get_monitor_count()

        assert count == 3

    def test_get_monitor_count_handles_error(
        self, mock_workspace_client, sample_config
    ):
        """Test monitor count returns 0 on error."""
        mock_workspace_client.data_quality.list_monitor.side_effect = Exception(
            "API Error"
        )

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        count = provisioner._get_monitor_count()

        assert count == 0

    def test_quota_handling(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test graceful quota handling."""
        mock_workspace_client.data_quality.list_monitor.return_value = [
            MagicMock() for _ in range(99)
        ]

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        results = provisioner.provision_all(sample_discovered_tables)

        skipped = [r for r in results if r.action == "skipped_quota"]
        assert len(skipped) == 1

    def test_dry_run_all(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test dry run mode validates columns and reports correctly."""
        sample_config.dry_run = True

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        results, impact = provisioner.dry_run_all(sample_discovered_tables)

        # First table (with correct columns) should be dry_run,
        # second table (only 'value' column) should be skipped_column_missing
        assert any(r.action == "dry_run" for r in results)
        assert any(r.action == "skipped_column_missing" for r in results)
        assert impact is not None

        mock_workspace_client.data_quality.create_monitor.assert_not_called()

    def test_provision_single_no_change_skips_update(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Existing in-sync monitor should return no_change and avoid update call."""
        existing_monitor = MagicMock()
        existing_monitor.data_profiling_config = DataProfilingConfig(
            output_schema_id="schema_123",
            inference_log=InferenceLogConfig(
                problem_type=InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION,
                prediction_column="prediction",
                timestamp_column="timestamp",
                label_column="label",
                model_id_column="model_version",
                granularities=[AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY],
            ),
            slicing_exprs=["region"],
            baseline_table_name=None,
            custom_metrics=[],
        )
        mock_workspace_client.data_quality.get_monitor.return_value = existing_monitor

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        result = provisioner._provision_single(sample_discovered_table)

        assert result.action == "no_change"
        mock_workspace_client.data_quality.update_monitor.assert_not_called()


class TestProfileProvisionerProfileTypes:
    """Tests for different profile type configurations."""

    def test_build_inference_config(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test building DataProfilingConfig for INFERENCE."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        config = provisioner._build_data_profiling_config(
            sample_discovered_table, "schema_123", []
        )

        assert config.inference_log is not None
        assert config.time_series is None
        assert config.snapshot is None
        assert config.output_schema_id == "schema_123"

    def test_build_timeseries_config(
        self, mock_workspace_client, sample_timeseries_config, sample_discovered_table
    ):
        """Test building DataProfilingConfig for TIMESERIES."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, sample_timeseries_config
        )

        config = provisioner._build_data_profiling_config(
            sample_discovered_table, "schema_123", []
        )

        assert config.time_series is not None
        assert config.inference_log is None
        assert config.snapshot is None
        assert config.time_series.timestamp_column == "event_time"

    def test_build_snapshot_config(
        self, mock_workspace_client, sample_snapshot_config, sample_discovered_table
    ):
        """Test building DataProfilingConfig for SNAPSHOT."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, sample_snapshot_config
        )

        config = provisioner._build_data_profiling_config(
            sample_discovered_table, "schema_123", []
        )

        assert config.snapshot is not None
        assert config.time_series is None
        assert config.inference_log is None

    def test_per_table_baseline_included(
        self, mock_workspace_client, config_with_per_table_baseline
    ):
        """Test per-table baseline table is included in config."""
        columns = [
            create_mock_column("churned", "STRING"),
            create_mock_column("churn_prob", "DOUBLE"),
        ]
        table = DiscoveredTable(
            full_name="test_catalog.ml.churn_model",
            columns=columns,
        )

        provisioner = ProfileProvisioner(
            mock_workspace_client, config_with_per_table_baseline
        )

        config = provisioner._build_data_profiling_config(
            table, "schema_123", []
        )

        assert config.baseline_table_name == "test_catalog.ml.churn_baseline"


class TestProvisioningCustomMetrics:
    """Tests for custom metrics embedded in DataProfilingConfig."""

    def test_build_custom_metrics(
        self, mock_workspace_client, config_with_custom_metrics, sample_discovered_table
    ):
        """Test custom metrics are built into SDK objects."""
        from databricks.sdk.service.dataquality import DataProfilingCustomMetricType

        provisioner = ProfileProvisioner(
            mock_workspace_client, config_with_custom_metrics
        )

        metrics = provisioner._build_custom_metrics(sample_discovered_table)

        assert len(metrics) == 2
        assert metrics[0].name == "revenue_sum"
        assert metrics[0].type == DataProfilingCustomMetricType.DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE
        assert metrics[1].name == "null_ratio"
        assert metrics[1].type == DataProfilingCustomMetricType.DATA_PROFILING_CUSTOM_METRIC_TYPE_DERIVED

    def test_custom_metrics_included_in_config(
        self, mock_workspace_client, config_with_custom_metrics, sample_discovered_table
    ):
        """Test custom metrics are included in DataProfilingConfig."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, config_with_custom_metrics
        )

        config = provisioner._build_data_profiling_config(
            sample_discovered_table, "schema_123", []
        )

        assert config.custom_metrics is not None
        assert len(config.custom_metrics) == 2


class TestOrphanCleanup:
    """Tests for orphan monitor cleanup."""

    def test_cleanup_orphans_identifies_orphans(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test orphan identification."""
        orphan_monitor = MagicMock()
        orphan_monitor.object_id = "orphan_table_id"
        mock_workspace_client.data_quality.list_monitor.return_value = [orphan_monitor]

        orphan_table_info = MagicMock()
        orphan_table_info.full_name = "test_catalog.other_schema.orphan_table"
        mock_workspace_client.tables.get.return_value = orphan_table_info

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        orphans = provisioner.cleanup_orphans(sample_discovered_tables)

        assert len(orphans) == 1
        assert "orphan_table" in orphans[0]

    def test_cleanup_orphans_deletes_when_enabled(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test orphans are deleted when cleanup_orphans is True."""
        sample_config.cleanup_orphans = True

        orphan_monitor = MagicMock()
        orphan_monitor.object_id = "orphan_table_id"
        mock_workspace_client.data_quality.list_monitor.return_value = [orphan_monitor]

        orphan_table_info = MagicMock()
        orphan_table_info.full_name = "test_catalog.other_schema.orphan_table"
        mock_workspace_client.tables.get.return_value = orphan_table_info

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner.cleanup_orphans(sample_discovered_tables)

        mock_workspace_client.data_quality.delete_monitor.assert_called_once()


class TestProvisioningEdgeCases:
    """Additional edge-case tests for uncovered provisioning branches."""

    def test_init_falls_back_to_service_username(self, mock_workspace_client, sample_config):
        """If current_user API fails, provisioner should use service fallback username."""
        mock_workspace_client.current_user.me.side_effect = RuntimeError("no user context")

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        assert provisioner.username == "dpo_service"

    def test_provision_single_returns_failed_when_table_not_found(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Missing table metadata should produce failed result."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_table_info = MagicMock(return_value=None)

        result = provisioner._provision_single(sample_discovered_table)

        assert result.action == "failed"
        assert result.error_message == "Table not found"

    def test_provision_single_returns_failed_when_schema_not_found(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Missing output schema should produce failed result."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_table_info = MagicMock(return_value=MagicMock(table_id="table_123"))
        provisioner._get_schema_info = MagicMock(return_value=None)

        result = provisioner._provision_single(sample_discovered_table)

        assert result.action == "failed"
        assert result.error_message == "Output schema not found"

    def test_provision_single_maps_quota_errors_to_skipped_quota(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Quota/limit failures should map to skipped_quota action."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_table_info = MagicMock(return_value=MagicMock(table_id="table_123"))
        provisioner._get_schema_info = MagicMock(return_value=MagicMock(schema_id="schema_123"))
        provisioner._validate_slicing_columns = MagicMock(return_value=[])
        provisioner._get_existing_monitor = MagicMock(return_value=None)
        provisioner._build_data_profiling_config = MagicMock(return_value=DataProfilingConfig(output_schema_id="schema_123"))
        mock_workspace_client.data_quality.create_monitor.side_effect = RuntimeError(
            "monitor quota limit reached"
        )

        result = provisioner._provision_single(sample_discovered_table)

        assert result.action == "skipped_quota"
        assert result.success is False

    def test_provision_all_converts_worker_exceptions_to_failed_results(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Exceptions from worker futures should be converted to failed results."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_monitor_count = MagicMock(return_value=0)

        def flaky(table):
            if table.full_name.endswith("test_table"):
                raise RuntimeError("worker boom")
            return ProvisioningResult(table_name=table.full_name, action="created", success=True)

        provisioner._provision_single = MagicMock(side_effect=flaky)

        results = provisioner.provision_all(sample_discovered_tables)

        assert len(results) == 2
        assert any(r.action == "failed" and "worker boom" in (r.error_message or "") for r in results)
        assert any(r.action == "created" for r in results)

    def test_dry_run_all_records_update_and_no_change(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Dry run should evaluate both update and no-change paths for existing monitors."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_monitor_count = MagicMock(return_value=0)
        provisioner._validate_columns_exist = MagicMock()  # bypass column checks
        provisioner._get_existing_monitor = MagicMock(side_effect=[MagicMock(), MagicMock()])
        provisioner._has_config_drift = MagicMock(side_effect=[True, False])
        provisioner._build_config_dict = MagicMock(return_value={"granularity": "1 day"})

        results, impact = provisioner.dry_run_all(sample_discovered_tables)

        assert len(results) == 2
        assert all(r.action == "dry_run" for r in results)
        assert all(r.config_hash is not None for r in results)

    def test_normalize_granularity_variants(self, mock_workspace_client, sample_config):
        """Granularity normalization should handle string, enum value, enum object, and unknown."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        assert provisioner._normalize_granularity(None) is None
        assert provisioner._normalize_granularity("1 hour") == "1 hour"
        assert (
            provisioner._normalize_granularity(
                AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY.value
            )
            == "1 day"
        )
        assert (
            provisioner._normalize_granularity(
                AggregationGranularity.AGGREGATION_GRANULARITY_1_MONTH
            )
            == "1 month"
        )
        assert provisioner._normalize_granularity("unknown") is None

    def test_extract_existing_config_timeseries_with_custom_metrics(
        self, mock_workspace_client, sample_config
    ):
        """Existing monitor extraction should serialize time-series and custom metric fields."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        metric = MagicMock()
        metric.name = "m1"
        metric.type = MagicMock(value="DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE")
        metric.input_columns = ["revenue"]
        metric.definition = "SUM(revenue)"
        metric.output_data_type = "double"

        cfg = MagicMock()
        cfg.inference_log = None
        cfg.time_series = MagicMock(
            timestamp_column="event_time",
            granularities=[AggregationGranularity.AGGREGATION_GRANULARITY_1_HOUR],
        )
        cfg.slicing_exprs = ["region"]
        cfg.baseline_table_name = "test_catalog.ml.baseline"
        cfg.custom_metrics = [metric]

        monitor = MagicMock()
        monitor.data_profiling_config = cfg

        extracted = provisioner._extract_existing_config(monitor)

        assert extracted["profile_type"] == "TIMESERIES"
        assert extracted["timestamp_column"] == "event_time"
        assert extracted["granularity"] == "1 hour"
        assert extracted["custom_metrics"][0]["metric_type"] == "aggregate"

    def test_has_config_drift_returns_true_on_compare_error(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Config compare errors should safely assume drift."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._extract_existing_config = MagicMock(side_effect=RuntimeError("bad monitor"))

        assert provisioner._has_config_drift(MagicMock(), sample_discovered_table) is True

    def test_resolve_problem_type_auto_detects_numeric_label(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Numeric label columns should trigger regression auto-detection."""
        sample_config.profile_defaults.problem_type = None
        sample_config.profile_defaults.label_column = "label"
        sample_discovered_table.tags.pop("monitor_problem_type", None)
        sample_discovered_table.columns = [
            create_mock_column("label", "DOUBLE"),
            create_mock_column("prediction", "DOUBLE"),
        ]

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

    def test_validate_slicing_columns_keeps_column_if_cardinality_check_fails(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Cardinality-check failures should not drop valid slicing columns."""
        sample_config.profile_defaults.slicing_exprs = ["region"]
        mock_workspace_client.statement_execution.execute_statement.side_effect = RuntimeError(
            "query failed"
        )
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        safe_columns = provisioner._validate_slicing_columns(sample_discovered_table)

        assert safe_columns == ["`region`"]

    def test_get_existing_monitor_returns_none_on_error(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Errors while fetching existing monitor should return None."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_table_info = MagicMock(side_effect=RuntimeError("boom"))

        existing = provisioner._get_existing_monitor(sample_discovered_table)

        assert existing is None

    def test_get_table_name_from_monitor_returns_none_on_error(
        self, mock_workspace_client, sample_config
    ):
        """Errors while resolving table name from monitor should return None."""
        monitor = MagicMock()
        monitor.object_id = "table_123"
        mock_workspace_client.tables.get.side_effect = RuntimeError("not found")
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        table_name = provisioner._get_table_name_from_monitor(monitor)

        assert table_name is None

    def test_validate_columns_exist_timeseries_uses_fallback_timestamp(
        self, mock_workspace_client, sample_timeseries_config
    ):
        """TIMESERIES validation should use profile default timestamp when table override is unset."""
        table_name = "test_catalog.events.page_views"
        sample_timeseries_config.monitored_tables[table_name] = MonitoredTableConfig(
            timestamp_column=None
        )
        table = DiscoveredTable(
            full_name=table_name,
            columns=[create_mock_column("event_time", "TIMESTAMP")],
        )
        provisioner = ProfileProvisioner(mock_workspace_client, sample_timeseries_config)

        provisioner._validate_columns_exist(table)

    def test_dry_run_all_quota_skips_tables(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Dry run should mark tables as skipped when quota is exhausted."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_monitor_count = MagicMock(
            return_value=provisioner.MAX_MONITORS_PER_METASTORE
        )

        results, impact = provisioner.dry_run_all(sample_discovered_tables)

        assert all(r.action == "skipped_quota" for r in results)

    def test_provision_single_updates_existing_monitor(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Config drift on existing monitor should trigger update path."""
        table_info = MagicMock(table_id="table_123")
        schema_info = MagicMock(schema_id="schema_123")
        existing = MagicMock()
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_table_info = MagicMock(return_value=table_info)
        provisioner._get_schema_info = MagicMock(return_value=schema_info)
        provisioner._validate_slicing_columns = MagicMock(return_value=[])
        provisioner._get_existing_monitor = MagicMock(return_value=existing)
        provisioner._has_config_drift = MagicMock(return_value=True)
        provisioner._build_data_profiling_config = MagicMock(
            return_value=DataProfilingConfig(output_schema_id="schema_123")
        )

        result = provisioner._provision_single(sample_discovered_table)

        mock_workspace_client.data_quality.update_monitor.assert_called_once()
        assert result.action == "updated"
        assert result.success is True

    def test_provision_single_maps_column_validation_errors(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Column-validation ValueError should map to skipped_column_missing."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._validate_columns_exist = MagicMock(
            side_effect=ValueError("missing required column")
        )

        result = provisioner._provision_single(sample_discovered_table)

        assert result.action == "skipped_column_missing"
        assert "missing required column" in (result.error_message or "")

    def test_provision_single_re_raises_non_quota_runtime_errors(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Unexpected runtime errors should be re-raised (not mapped to quota skip)."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._get_table_info = MagicMock(return_value=MagicMock(table_id="table_123"))
        provisioner._get_schema_info = MagicMock(return_value=MagicMock(schema_id="schema_123"))
        provisioner._validate_slicing_columns = MagicMock(return_value=[])
        provisioner._get_existing_monitor = MagicMock(return_value=None)
        provisioner._build_data_profiling_config = MagicMock(
            return_value=DataProfilingConfig(output_schema_id="schema_123")
        )
        mock_workspace_client.data_quality.create_monitor.side_effect = RuntimeError(
            "transient backend failure"
        )

        with pytest.raises(RuntimeError, match="transient backend failure"):
            ProfileProvisioner._provision_single.__wrapped__(provisioner, sample_discovered_table)

    def test_build_custom_metrics_skips_unknown_metric_type(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Unknown custom metric types should be skipped."""
        bad_metric = MagicMock()
        bad_metric.name = "bad_metric"
        bad_metric.metric_type = "unknown"
        bad_metric.input_columns = ["col"]
        bad_metric.definition = "SUM(col)"
        bad_metric.output_type = "double"
        sample_config.profile_defaults.custom_metrics = [bad_metric]
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        metrics = provisioner._build_custom_metrics(sample_discovered_table)

        assert metrics == []

    def test_serialize_resolved_custom_metrics(
        self, mock_workspace_client, config_with_custom_metrics, sample_discovered_table
    ):
        """Resolved custom metrics (three-tier merge) should serialize to deterministic dict format."""
        provisioner = ProfileProvisioner(mock_workspace_client, config_with_custom_metrics)

        serialized = provisioner._serialize_resolved_custom_metrics(sample_discovered_table)

        assert len(serialized) == 2
        assert serialized[0]["name"] == "revenue_sum"
        assert serialized[1]["metric_type"] == "derived"

    def test_normalize_granularity_unknown_object_returns_none(
        self, mock_workspace_client, sample_config
    ):
        """Unknown granularity object types should normalize to None."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        assert provisioner._normalize_granularity(object()) is None

    def test_extract_existing_config_handles_missing_data_profiling_config(
        self, mock_workspace_client, sample_config
    ):
        """Monitors without profiling config should extract to empty config dict."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        monitor = MagicMock()
        monitor.data_profiling_config = None

        extracted = provisioner._extract_existing_config(monitor)

        assert extracted == {}

    def test_resolve_problem_type_table_config_classification(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Explicit table config classification should return classification."""
        sample_config.monitored_tables[sample_discovered_table.full_name] = MonitoredTableConfig(
            problem_type="PROBLEM_TYPE_CLASSIFICATION"
        )
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert (
            problem_type
            == InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION
        )

    def test_resolve_problem_type_tag_classification(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Classification tag should return classification when table config absent."""
        sample_config.monitored_tables[sample_discovered_table.full_name] = (
            MonitoredTableConfig()
        )
        sample_discovered_table.tags["monitor_problem_type"] = "CLASSIFICATION"
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert (
            problem_type
            == InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION
        )

    def test_resolve_problem_type_falls_back_to_default_classification(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """No signals should fall back to default classification."""
        sample_config.monitored_tables[sample_discovered_table.full_name] = (
            MonitoredTableConfig()
        )
        sample_config.profile_defaults.problem_type = None
        sample_config.profile_defaults.label_column = None
        sample_discovered_table.tags.pop("monitor_problem_type", None)
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        problem_type = provisioner._resolve_problem_type(sample_discovered_table)

        assert (
            problem_type
            == InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION
        )

    def test_get_approx_cardinality_returns_zero_when_result_missing(
        self, mock_workspace_client, sample_config
    ):
        """Missing query result payload should default cardinality to zero."""
        result = MagicMock()
        result.result = None
        mock_workspace_client.statement_execution.execute_statement.return_value = result
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        cardinality = provisioner._get_approx_cardinality("cat.sch.tbl", "region")

        assert cardinality == 0

    def test_get_table_info_returns_none_on_exception(
        self, mock_workspace_client, sample_config
    ):
        """Table info helper should return None when table API fails."""
        mock_workspace_client.tables.get.side_effect = RuntimeError("table api down")
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        assert provisioner._get_table_info("cat.sch.tbl") is None

    def test_get_schema_info_returns_none_on_exception(
        self, mock_workspace_client, sample_config
    ):
        """Schema info helper should return None when schema API fails."""
        mock_workspace_client.schemas.get.side_effect = RuntimeError("schema api down")
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        assert provisioner._get_schema_info() is None

    def test_cleanup_orphans_handles_delete_errors(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Delete failures should be logged and not abort orphan cleanup."""
        sample_config.cleanup_orphans = True
        orphan_monitor = MagicMock(object_id="orphan_id")
        mock_workspace_client.data_quality.list_monitor.return_value = [orphan_monitor]
        mock_workspace_client.tables.get.return_value = MagicMock(
            full_name="test_catalog.other_schema.orphan_table"
        )
        mock_workspace_client.data_quality.delete_monitor.side_effect = RuntimeError(
            "delete failed"
        )
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        orphans = provisioner.cleanup_orphans(sample_discovered_tables)

        assert orphans == ["test_catalog.other_schema.orphan_table"]

    def test_cleanup_orphans_handles_list_errors(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """List failures should return empty orphan list."""
        mock_workspace_client.data_quality.list_monitor.side_effect = RuntimeError(
            "list failed"
        )
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)

        orphans = provisioner.cleanup_orphans(sample_discovered_tables)

        assert orphans == []
