"""Tests for DPO orchestration entrypoints."""

from unittest.mock import MagicMock

import databricks.sdk as sdk

import dpo
from dpo.config import MonitoredTableConfig
from dpo.discovery import DiscoveredTable
from dpo.provisioning import MonitorStatus, ProvisioningResult


def _discovered_table(full_name: str, group: str | None = None) -> DiscoveredTable:
    tags = {"monitor_enabled": "true"}
    if group:
        tags["monitor_group"] = group
    return DiscoveredTable(
        full_name=full_name,
        tags=tags,
        columns=[],
        table_type="MANAGED",
    )


class TestOrchestrationHelpers:
    """Tests for table-list helper functions in orchestration."""

    def test_create_table_from_config_fetches_tags(self):
        """Test helper builds DiscoveredTable with table metadata and tags."""
        w = MagicMock()

        table_info = MagicMock()
        table_info.columns = []
        table_info.table_type = MagicMock(value="MANAGED")
        w.tables.get.return_value = table_info

        tag1 = MagicMock()
        tag1.tag_key = "owner"
        tag1.tag_value = "data_eng"
        tag2 = MagicMock()
        tag2.tag_key = "monitor_priority"
        tag2.tag_value = "3"
        w.entity_tag_assignments.list.return_value = [tag1, tag2]

        table = dpo._create_table_from_config(w, "test_catalog.ml.predictions")

        assert table.full_name == "test_catalog.ml.predictions"
        assert table.tags["owner"] == "data_eng"
        assert table.priority == 3
        assert table.table_type == "MANAGED"

    def test_create_table_from_config_handles_tag_fetch_error(self):
        """Test helper still returns table when tag lookup fails."""
        w = MagicMock()

        table_info = MagicMock()
        table_info.columns = []
        table_info.table_type = MagicMock(value="MANAGED")
        w.tables.get.return_value = table_info
        w.entity_tag_assignments.list.side_effect = RuntimeError("tag api down")

        table = dpo._create_table_from_config(w, "test_catalog.ml.predictions")

        assert table.full_name == "test_catalog.ml.predictions"
        assert table.tags == {}
        assert table.priority == 99

    def test_build_table_list_yaml_precedence(
        self, monkeypatch, sample_config_with_discovery
    ):
        """YAML entries should override overlapping tag-discovered tables."""
        config = sample_config_with_discovery
        config.monitored_tables = {
            "test_catalog.ml.model_a": MonitoredTableConfig(),
            "test_catalog.manual.model_b": MonitoredTableConfig(),
        }

        discovered_overlap = _discovered_table("test_catalog.ml.model_a")
        discovered_only = _discovered_table("test_catalog.tag_only.model_c")

        discovery = MagicMock()
        discovery.discover.return_value = [discovered_overlap, discovered_only]
        monkeypatch.setattr(dpo, "TableDiscovery", MagicMock(return_value=discovery))

        yaml_a = _discovered_table("test_catalog.ml.model_a")
        yaml_b = _discovered_table("test_catalog.manual.model_b")
        monkeypatch.setattr(
            dpo,
            "_create_table_from_config",
            MagicMock(side_effect=[yaml_a, yaml_b]),
        )

        tables = dpo._build_table_list(MagicMock(), config)
        names = [t.full_name for t in tables]

        assert names.count("test_catalog.ml.model_a") == 1
        assert "test_catalog.tag_only.model_c" in names
        assert "test_catalog.manual.model_b" in names

    def test_successful_tables_filters_expected_actions(self):
        """Only created/updated/no_change should be treated as healthy."""
        tables = [
            _discovered_table("test_catalog.sch.a"),
            _discovered_table("test_catalog.sch.b"),
            _discovered_table("test_catalog.sch.c"),
        ]
        results = [
            ProvisioningResult("test_catalog.sch.a", "created", True),
            ProvisioningResult("test_catalog.sch.b", "updated", True),
            ProvisioningResult("test_catalog.sch.c", "skipped_quota", False),
        ]

        healthy = dpo._successful_tables(tables, results)

        assert [t.full_name for t in healthy] == [
            "test_catalog.sch.a",
            "test_catalog.sch.b",
        ]


class TestBulkOrchestration:
    """Tests for bulk-provision-only orchestration mode."""

    def test_run_bulk_provisioning_happy_path(self, monkeypatch, sample_config):
        """Bulk mode should provision, wait, cleanup, and summarize correctly."""
        config = sample_config
        config.wait_for_monitors = True
        config.cleanup_orphans = True
        config.dry_run = False

        w = MagicMock()
        monkeypatch.setattr(sdk, "WorkspaceClient", lambda: w)

        tables = [
            _discovered_table("test_catalog.sch.a"),
            _discovered_table("test_catalog.sch.b"),
            _discovered_table("test_catalog.sch.c"),
            _discovered_table("test_catalog.sch.d"),
        ]
        monkeypatch.setattr(dpo, "_build_table_list", MagicMock(return_value=tables))

        results = [
            ProvisioningResult("test_catalog.sch.a", "created", True),
            ProvisioningResult("test_catalog.sch.b", "updated", True),
            ProvisioningResult("test_catalog.sch.c", "no_change", True),
            ProvisioningResult("test_catalog.sch.d", "skipped_quota", False),
            ProvisioningResult("test_catalog.sch.e", "failed", False),
        ]
        provisioner = MagicMock()
        provisioner.provision_all.return_value = results
        provisioner.cleanup_orphans.return_value = ["test_catalog.sch.orphan"]
        monkeypatch.setattr(dpo, "ProfileProvisioner", MagicMock(return_value=provisioner))

        wait_statuses = [MonitorStatus("test_catalog.sch.a", "ACTIVE")]
        wait_for_monitors = MagicMock(return_value=wait_statuses)
        monkeypatch.setattr(dpo, "wait_for_monitors", wait_for_monitors)
        verify_permissions = MagicMock()
        monkeypatch.setattr(dpo, "verify_output_schema_permissions", verify_permissions)

        report = dpo.run_bulk_provisioning(config)

        verify_permissions.assert_called_once()
        wait_for_monitors.assert_called_once()
        provisioner.cleanup_orphans.assert_called_once_with(tables)
        assert report.tables_discovered == 4
        assert report.monitors_created == 1
        assert report.monitors_updated == 1
        assert report.monitors_skipped == 1
        assert report.monitors_failed == 1
        assert report.orphans_cleaned == 1
        assert report.monitor_statuses == wait_statuses

    def test_run_bulk_provisioning_skips_wait_without_healthy_tables(
        self, monkeypatch, sample_config
    ):
        """Wait step should be skipped when no monitors were successfully provisioned."""
        config = sample_config
        config.wait_for_monitors = True
        config.cleanup_orphans = False
        config.dry_run = False

        monkeypatch.setattr(sdk, "WorkspaceClient", lambda: MagicMock())
        tables = [_discovered_table("test_catalog.sch.a")]
        monkeypatch.setattr(dpo, "_build_table_list", MagicMock(return_value=tables))

        provisioner = MagicMock()
        provisioner.provision_all.return_value = [
            ProvisioningResult("test_catalog.sch.a", "failed", False)
        ]
        monkeypatch.setattr(dpo, "ProfileProvisioner", MagicMock(return_value=provisioner))
        wait_for_monitors = MagicMock(return_value=[])
        monkeypatch.setattr(dpo, "wait_for_monitors", wait_for_monitors)
        monkeypatch.setattr(dpo, "verify_output_schema_permissions", MagicMock())

        report = dpo.run_bulk_provisioning(config)

        wait_for_monitors.assert_not_called()
        assert report.monitor_statuses == []

    def test_run_bulk_provisioning_uses_dry_run_results(
        self, monkeypatch, sample_config
    ):
        """Bulk mode should use dry_run_all when dry_run is enabled."""
        config = sample_config
        config.dry_run = True
        config.wait_for_monitors = True

        monkeypatch.setattr(sdk, "WorkspaceClient", lambda: MagicMock())
        tables = [_discovered_table("test_catalog.sch.a")]
        monkeypatch.setattr(dpo, "_build_table_list", MagicMock(return_value=tables))
        monkeypatch.setattr(dpo, "verify_output_schema_permissions", MagicMock())
        monkeypatch.setattr(dpo, "wait_for_monitors", MagicMock())

        provisioner = MagicMock()
        provisioner.dry_run_all.return_value = [
            ProvisioningResult("test_catalog.sch.a", "dry_run", True)
        ]
        monkeypatch.setattr(dpo, "ProfileProvisioner", MagicMock(return_value=provisioner))

        report = dpo.run_bulk_provisioning(config)

        provisioner.dry_run_all.assert_called_once_with(tables)
        provisioner.provision_all.assert_not_called()
        assert report.tables_discovered == 1


class TestFullOrchestration:
    """Tests for full orchestration mode."""

    def test_run_orchestration_dispatches_to_bulk(self, monkeypatch, sample_bulk_config):
        """Main entrypoint should dispatch bulk mode to bulk runner."""
        expected = dpo.OrchestrationReport(
            tables_discovered=1,
            monitors_created=1,
            monitors_updated=0,
            monitors_skipped=0,
            monitors_failed=0,
            orphans_cleaned=0,
        )
        bulk_runner = MagicMock(return_value=expected)
        monkeypatch.setattr(dpo, "run_bulk_provisioning", bulk_runner)

        result = dpo.run_orchestration(sample_bulk_config)

        bulk_runner.assert_called_once_with(sample_bulk_config)
        assert result == expected

    def test_run_orchestration_full_pipeline(self, monkeypatch, sample_config):
        """Full mode should wire provisioning, aggregation, alerting, and dashboards."""
        config = sample_config
        config.mode = "full"
        config.dry_run = False
        config.cleanup_orphans = True
        config.wait_for_monitors = True
        config.alerting.enable_aggregated_alerts = True
        config.deploy_aggregated_dashboard = True

        monkeypatch.setattr(sdk, "WorkspaceClient", lambda: MagicMock())
        verify_output = MagicMock()
        verify_view = MagicMock()
        monkeypatch.setattr(dpo, "verify_output_schema_permissions", verify_output)
        monkeypatch.setattr(dpo, "verify_view_permissions", verify_view)

        tables = [
            _discovered_table("test_catalog.sch.a", group="ML Team"),
            _discovered_table("test_catalog.sch.b", group="default"),
        ]
        monkeypatch.setattr(dpo, "_build_table_list", MagicMock(return_value=tables))

        provisioner = MagicMock()
        provisioner.provision_all.return_value = [
            ProvisioningResult("test_catalog.sch.a", "created", True),
            ProvisioningResult("test_catalog.sch.b", "updated", True),
        ]
        provisioner.cleanup_orphans.return_value = ["test_catalog.sch.orphan"]
        monkeypatch.setattr(dpo, "ProfileProvisioner", MagicMock(return_value=provisioner))

        wait_statuses = [MonitorStatus("test_catalog.sch.a", "ACTIVE")]
        monkeypatch.setattr(dpo, "wait_for_monitors", MagicMock(return_value=wait_statuses))

        views_by_group = {
            "ML Team": ("test_catalog.global_monitoring.unified_drift_metrics_ml_team", "test_catalog.global_monitoring.unified_profile_metrics_ml_team"),
            "default": ("test_catalog.global_monitoring.unified_drift_metrics_default", "test_catalog.global_monitoring.unified_profile_metrics_default"),
        }
        aggregator = MagicMock()
        aggregator.create_unified_views_by_group.return_value = views_by_group
        monkeypatch.setattr(dpo, "MetricsAggregator", MagicMock(return_value=aggregator))

        alerts_by_group = {
            "ML Team": ("drift_ml", None),
            "default": (None, "quality_default"),
        }
        alerter = MagicMock()
        alerter.create_alerts_by_group.return_value = alerts_by_group
        monkeypatch.setattr(dpo, "AlertProvisioner", MagicMock(return_value=alerter))

        dashboards_by_group = {"ML Team": "dash_ml", "default": "dash_default"}
        dashboard_provisioner = MagicMock()
        dashboard_provisioner.deploy_dashboards_by_group.return_value = dashboards_by_group
        monkeypatch.setattr(
            dpo,
            "DashboardProvisioner",
            MagicMock(return_value=dashboard_provisioner),
        )

        report = dpo.run_orchestration(config)

        verify_output.assert_called_once()
        verify_view.assert_called_once()
        aggregator.create_unified_views_by_group.assert_called_once()
        aggregator.cleanup_stale_views.assert_called_once_with(
            "test_catalog.global_monitoring", {"ml_team", "default"}
        )
        alerter.create_alerts_by_group.assert_called_once_with(
            views_by_group, "test_catalog"
        )
        dashboard_provisioner.cleanup_stale_dashboards.assert_called_once_with(
            config.dashboard_parent_path, {"ML Team", "default"}
        )
        assert report.unified_drift_views["ML Team"].endswith("_ml_team")
        assert report.drift_alert_ids == {"ML Team": "drift_ml"}
        assert report.quality_alert_ids == {"default": "quality_default"}
        assert report.dashboard_ids == dashboards_by_group
        assert report.monitor_statuses == wait_statuses

    def test_run_orchestration_dry_run_skips_downstream(self, monkeypatch, sample_config):
        """Dry run in full mode should skip aggregation, alerting, and dashboard deployment."""
        config = sample_config
        config.mode = "full"
        config.dry_run = True
        config.wait_for_monitors = True

        monkeypatch.setattr(sdk, "WorkspaceClient", lambda: MagicMock())
        monkeypatch.setattr(dpo, "verify_output_schema_permissions", MagicMock())
        monkeypatch.setattr(dpo, "verify_view_permissions", MagicMock())

        tables = [_discovered_table("test_catalog.sch.a")]
        monkeypatch.setattr(dpo, "_build_table_list", MagicMock(return_value=tables))

        provisioner = MagicMock()
        provisioner.dry_run_all.return_value = [
            ProvisioningResult("test_catalog.sch.a", "dry_run", True)
        ]
        monkeypatch.setattr(dpo, "ProfileProvisioner", MagicMock(return_value=provisioner))

        aggregator = MagicMock()
        monkeypatch.setattr(dpo, "MetricsAggregator", MagicMock(return_value=aggregator))
        alerter = MagicMock()
        monkeypatch.setattr(dpo, "AlertProvisioner", MagicMock(return_value=alerter))
        dashboard_provisioner = MagicMock()
        monkeypatch.setattr(
            dpo,
            "DashboardProvisioner",
            MagicMock(return_value=dashboard_provisioner),
        )

        report = dpo.run_orchestration(config)

        aggregator.create_unified_views_by_group.assert_not_called()
        alerter.create_alerts_by_group.assert_not_called()
        dashboard_provisioner.deploy_dashboards_by_group.assert_not_called()
        assert report.unified_drift_views == {}
        assert report.dashboard_ids == {}

    def test_run_orchestration_logs_skip_when_no_healthy_tables(
        self, monkeypatch, sample_config
    ):
        """Full mode should skip wait and downstream work when all provisions fail."""
        config = sample_config
        config.mode = "full"
        config.dry_run = False
        config.wait_for_monitors = True
        config.alerting.enable_aggregated_alerts = True
        config.deploy_aggregated_dashboard = True

        monkeypatch.setattr(sdk, "WorkspaceClient", lambda: MagicMock())
        monkeypatch.setattr(dpo, "verify_output_schema_permissions", MagicMock())
        monkeypatch.setattr(dpo, "verify_view_permissions", MagicMock())

        tables = [_discovered_table("test_catalog.sch.a")]
        monkeypatch.setattr(dpo, "_build_table_list", MagicMock(return_value=tables))

        provisioner = MagicMock()
        provisioner.provision_all.return_value = [
            ProvisioningResult("test_catalog.sch.a", "failed", False)
        ]
        monkeypatch.setattr(dpo, "ProfileProvisioner", MagicMock(return_value=provisioner))
        wait_for_monitors = MagicMock()
        monkeypatch.setattr(dpo, "wait_for_monitors", wait_for_monitors)

        aggregator = MagicMock()
        monkeypatch.setattr(dpo, "MetricsAggregator", MagicMock(return_value=aggregator))
        alerter = MagicMock()
        monkeypatch.setattr(dpo, "AlertProvisioner", MagicMock(return_value=alerter))
        dashboard_provisioner = MagicMock()
        monkeypatch.setattr(
            dpo,
            "DashboardProvisioner",
            MagicMock(return_value=dashboard_provisioner),
        )

        report = dpo.run_orchestration(config)

        wait_for_monitors.assert_not_called()
        aggregator.create_unified_views_by_group.assert_not_called()
        alerter.create_alerts_by_group.assert_called_once_with({}, "test_catalog")
        dashboard_provisioner.deploy_dashboards_by_group.assert_called_once_with(
            {}, config.dashboard_parent_path
        )
        assert report.unified_drift_views == {}
