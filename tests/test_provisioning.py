"""Tests for DPO provisioning module."""

import pytest
from unittest.mock import MagicMock, patch, call

from databricks.sdk.service.dataquality import (
    AggregationGranularity,
    InferenceProblemType,
)

from dpo.provisioning import ProfileProvisioner, ProvisioningResult, ImpactReport
from dpo.discovery import DiscoveredTable


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

    def test_resolve_problem_type_from_tag(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type resolution from table tag."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        # Add tag override
        sample_discovered_table.tags["monitor_problem_type"] = "REGRESSION"
        
        problem_type = provisioner._resolve_problem_type(sample_discovered_table)
        
        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

    def test_resolve_problem_type_from_config(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type resolution from config."""
        sample_config.profile_defaults.inference_settings.problem_type = (
            "PROBLEM_TYPE_REGRESSION"
        )
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        # Remove tag to fall through to config
        sample_discovered_table.tags.pop("monitor_problem_type", None)
        
        problem_type = provisioner._resolve_problem_type(sample_discovered_table)
        
        assert problem_type == InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

    def test_resolve_problem_type_default(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test problem type defaults to classification."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        # Clear tag and set config to classification
        sample_discovered_table.tags.pop("monitor_problem_type", None)
        sample_config.profile_defaults.inference_settings.problem_type = (
            "PROBLEM_TYPE_CLASSIFICATION"
        )
        
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
        assert "prediction_col" in config_dict

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
        assert config_dict["timestamp_col"] == "event_time"

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
        # Add a column that doesn't exist
        sample_config.profile_defaults.slicing_columns = [
            "region",  # Exists
            "nonexistent_column",  # Doesn't exist
        ]
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        safe_columns = provisioner._validate_slicing_columns(sample_discovered_table)
        
        # Only 'region' should be included
        assert len(safe_columns) == 1
        assert "`region`" in safe_columns

    def test_validate_slicing_columns_filters_high_cardinality(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test that high-cardinality columns are filtered out."""
        sample_config.profile_defaults.slicing_columns = ["region"]
        sample_config.profile_defaults.max_slicing_cardinality = 5
        
        # Mock high cardinality
        result = MagicMock()
        result.result.data_array = [[100]]  # 100 distinct values
        mock_workspace_client.statement_execution.execute_statement.return_value = result
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        safe_columns = provisioner._validate_slicing_columns(sample_discovered_table)
        
        # Should be filtered out due to high cardinality
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
        # Set up to have only 1 quota slot remaining
        mock_workspace_client.data_quality.list_monitors.return_value = [
            MagicMock() for _ in range(99)
        ]
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        
        # Provision 2 tables with only 1 quota slot
        results = provisioner.provision_all(sample_discovered_tables)
        
        # One should succeed, one should be skipped
        skipped = [r for r in results if r.action == "skipped_quota"]
        assert len(skipped) == 1

    def test_dry_run_all(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test dry run mode."""
        sample_config.dry_run = True
        
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        results = provisioner.dry_run_all(sample_discovered_tables)
        
        # All results should be dry_run
        assert all(r.action == "dry_run" for r in results)
        
        # No actual API calls should be made
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

    def test_baseline_table_included(
        self, mock_workspace_client, config_with_baseline, sample_discovered_table
    ):
        """Test baseline table is included in config."""
        provisioner = ProfileProvisioner(
            mock_workspace_client, config_with_baseline
        )
        
        config = provisioner._build_data_profiling_config(
            sample_discovered_table, "schema_123", []
        )
        
        assert config.baseline_table_name == "test_catalog.ml.training_baseline"


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
        
        # Should have been called twice (2 custom metrics)
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
        
        # Should not raise
        provisioner._provision_custom_metrics("table_123")


class TestOrphanCleanup:
    """Tests for orphan monitor cleanup."""

    def test_cleanup_orphans_identifies_orphans(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test orphan identification."""
        # Mock existing monitor for a table not in discovered list
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
