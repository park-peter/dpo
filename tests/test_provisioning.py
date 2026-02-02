"""Tests for DPO provisioning module."""

import pytest
from unittest.mock import MagicMock

from databricks.sdk.service.dataquality import (
    AggregationGranularity,
    InferenceProblemType,
)

from dpo.config import MonitoredTableConfig
from dpo.provisioning import ProfileProvisioner, ProvisioningResult, ImpactReport
from dpo.discovery import DiscoveredTable
from tests.conftest import create_mock_column


class TestProvisioningResult:
    """Tests for ProvisioningResult dataclass."""

    def test_created_result(self):
        """Test created result."""
        result = ProvisioningResult(
            table_name="cat.sch.tbl",
            action="created",
            success=True,
            monitor_id="mon_123",
        )
        
        assert result.success is True
        assert result.action == "created"
        assert result.monitor_id == "mon_123"

    def test_failed_result(self):
        """Test failed result with error message."""
        result = ProvisioningResult(
            table_name="cat.sch.tbl",
            action="failed",
            success=False,
            error_message="Permission denied",
        )
        
        assert result.success is False
        assert result.error_message == "Permission denied"

    def test_skipped_column_missing_result(self):
        """Test skipped due to missing column."""
        result = ProvisioningResult(
            table_name="cat.sch.tbl",
            action="skipped_column_missing",
            success=False,
            error_message="Column 'label' not found",
        )
        
        assert result.action == "skipped_column_missing"


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
        mock_workspace_client.data_quality.list_monitors.return_value = [
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
        mock_workspace_client.data_quality.list_monitors.side_effect = Exception(
            "API Error"
        )
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        count = provisioner._get_monitor_count()
        
        assert count == 0

    def test_quota_handling(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test graceful quota handling."""
        mock_workspace_client.data_quality.list_monitors.return_value = [
            MagicMock() for _ in range(99)
        ]
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        results = provisioner.provision_all(sample_discovered_tables)
        
        skipped = [r for r in results if r.action == "skipped_quota"]
        assert len(skipped) == 1

    def test_dry_run_all(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test dry run mode."""
        sample_config.dry_run = True
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        results = provisioner.dry_run_all(sample_discovered_tables)
        
        assert all(r.action == "dry_run" for r in results)
        
        mock_workspace_client.data_quality.create_monitor.assert_not_called()


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
    """Tests for custom metrics provisioning."""

    def test_provision_custom_metrics_called(
        self, mock_workspace_client, config_with_custom_metrics, sample_discovered_table
    ):
        """Test custom metrics are provisioned."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, config_with_custom_metrics
        )
        
        provisioner._provision_custom_metrics("table_123")
        
        assert mock_workspace_client.data_quality.create_metric.call_count == 2

    def test_provision_custom_metrics_handles_errors(
        self, mock_workspace_client, config_with_custom_metrics, sample_discovered_table
    ):
        """Test custom metric errors are handled gracefully."""
        mock_workspace_client.data_quality.create_metric.side_effect = Exception(
            "API Error"
        )
        
        provisioner = ProfileProvisioner(
            mock_workspace_client, config_with_custom_metrics
        )
        
        provisioner._provision_custom_metrics("table_123")


class TestOrphanCleanup:
    """Tests for orphan monitor cleanup."""

    def test_cleanup_orphans_identifies_orphans(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test orphan identification."""
        orphan_monitor = MagicMock()
        orphan_monitor.object_id = "orphan_table_id"
        mock_workspace_client.data_quality.list_monitors.return_value = [orphan_monitor]
        
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
        mock_workspace_client.data_quality.list_monitors.return_value = [orphan_monitor]
        
        orphan_table_info = MagicMock()
        orphan_table_info.full_name = "test_catalog.other_schema.orphan_table"
        mock_workspace_client.tables.get.return_value = orphan_table_info
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner.cleanup_orphans(sample_discovered_tables)
        
        mock_workspace_client.data_quality.delete_monitor.assert_called_once()
