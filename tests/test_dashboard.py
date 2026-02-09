"""Tests for DPO dashboard provisioning module."""

import json
from unittest.mock import MagicMock, call

import pytest

from dpo.dashboard import DashboardProvisioner


class TestDashboardProvisioner:
    """Tests for DashboardProvisioner behavior."""

    def test_initialization(self, mock_workspace_client, sample_config):
        """Test provisioner stores workspace client and config-derived catalog."""
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        assert provisioner.w is mock_workspace_client
        assert provisioner.catalog == "test_catalog"

    def test_deploy_dashboard_creates_when_missing(
        self, mock_workspace_client, sample_config
    ):
        """Test create flow when dashboard does not already exist."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_123"
        )

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        dashboard_id = provisioner.deploy_dashboard(
            unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics",
            parent_path="/Workspace/Shared/DPO",
        )

        assert dashboard_id == "dash_123"
        mock_workspace_client.lakeview.create.assert_called_once()
        mock_workspace_client.lakeview.update.assert_not_called()

        dashboard_payload = mock_workspace_client.lakeview.create.call_args.kwargs[
            "dashboard"
        ]
        serialized = json.loads(dashboard_payload.serialized_dashboard)
        assert dashboard_payload.parent_path == "/Workspace/Shared/DPO"
        assert serialized["displayName"] == "DPO Global Health - test_catalog"

        # Drift and profile datasets are both present
        ds_by_name = {ds["name"]: ds for ds in serialized["datasets"]}
        assert "unified_drift" in ds_by_name
        assert "unified_profile" in ds_by_name
        assert (
            ds_by_name["unified_drift"]["query"]
            == "SELECT * FROM test_catalog.global_monitoring.unified_drift_metrics"
        )

    def test_deploy_dashboard_updates_when_existing(
        self, mock_workspace_client, sample_config
    ):
        """Test update flow when existing dashboard with same name is found."""
        existing = MagicMock()
        existing.display_name = "DPO Global Health - test_catalog"
        existing.dashboard_id = "dash_existing"
        mock_workspace_client.lakeview.list.return_value = [existing]
        mock_workspace_client.lakeview.update.return_value = MagicMock(
            dashboard_id="dash_existing"
        )

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        dashboard_id = provisioner.deploy_dashboard(
            unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics",
            parent_path="/Workspace/Shared/DPO",
        )

        assert dashboard_id == "dash_existing"
        mock_workspace_client.lakeview.update.assert_called_once()
        mock_workspace_client.lakeview.create.assert_not_called()

    def test_deploy_dashboard_uses_configured_thresholds(
        self, mock_workspace_client, sample_config
    ):
        """Dashboard counters should use config-driven drift/warning thresholds."""
        sample_config.alerting.drift_threshold = 0.4
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_123"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        provisioner.deploy_dashboard(
            unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics",
            parent_path="/Workspace/Shared/DPO",
        )

        dashboard_payload = mock_workspace_client.lakeview.create.call_args.kwargs[
            "dashboard"
        ]
        serialized = json.loads(dashboard_payload.serialized_dashboard)
        overview_page = serialized["pages"][0]
        summary_widget = overview_page["layout"][0]["widget"]
        fields = summary_widget["queries"][0]["query"]["fields"]
        fields_by_name = {field["name"]: field["expression"] for field in fields}

        assert ">= 0.4" in fields_by_name["critical_alerts"]
        assert ">= 0.2" in fields_by_name["warning_alerts"]
        assert "< 0.4" in fields_by_name["warning_alerts"]

    def test_deploy_dashboard_raises_when_create_fails(
        self, mock_workspace_client, sample_config
    ):
        """Create failures should be logged and re-raised."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.side_effect = RuntimeError("create failed")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        with pytest.raises(RuntimeError, match="create failed"):
            provisioner.deploy_dashboard(
                unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics",
                parent_path="/Workspace/Shared/DPO",
            )

    def test_find_existing_dashboard_returns_none_on_error(
        self, mock_workspace_client, sample_config
    ):
        """Test dashboard lookup returns None when list API errors."""
        mock_workspace_client.lakeview.list.side_effect = RuntimeError("boom")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        found = provisioner._find_existing_dashboard(
            "DPO Global Health - test_catalog", "/Workspace/Shared/DPO"
        )

        assert found is None

    def test_get_dashboard_url_uses_host(self, mock_workspace_client, sample_config):
        """Test URL builder uses workspace host when available."""
        mock_workspace_client.config.host = "https://example.cloud.databricks.com"
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        url = provisioner.get_dashboard_url("dash_123")

        assert url == "https://example.cloud.databricks.com/dashboards/dash_123"

    def test_get_dashboard_url_falls_back_on_error(
        self, mock_workspace_client, sample_config
    ):
        """Test URL builder falls back to relative URL when host is unavailable."""
        mock_workspace_client.config = None
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        url = provisioner.get_dashboard_url("dash_123")

        assert url == "/dashboards/dash_123"

    def test_deploy_dashboards_by_group(self, mock_workspace_client, sample_config):
        """Test per-group deployment delegates and returns group-id map."""
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_dashboard = MagicMock(side_effect=["dash_ml", "dash_default"])

        views_by_group = {
            "ml_team": ("cat.sch.drift_ml", "cat.sch.profile_ml"),
            "default": ("cat.sch.drift_default", "cat.sch.profile_default"),
        }
        results = provisioner.deploy_dashboards_by_group(
            views_by_group, "/Workspace/Shared/DPO"
        )

        assert results == {"ml_team": "dash_ml", "default": "dash_default"}
        assert provisioner.deploy_dashboard.call_count == 2
        provisioner.deploy_dashboard.assert_has_calls(
            [
                call(
                    "cat.sch.drift_ml",
                    "/Workspace/Shared/DPO",
                    unified_profile_view="cat.sch.profile_ml",
                    dashboard_name="DPO Health - ml_team",
                ),
                call(
                    "cat.sch.drift_default",
                    "/Workspace/Shared/DPO",
                    unified_profile_view="cat.sch.profile_default",
                    dashboard_name="DPO Health - default",
                ),
            ]
        )

    def test_cleanup_stale_dashboards_deletes_inactive_groups(
        self, mock_workspace_client, sample_config
    ):
        """Test stale dashboards are trashed while active and non-DPO dashboards are kept."""
        active = MagicMock(display_name="DPO Health - ml_team", dashboard_id="dash_active")
        stale = MagicMock(display_name="DPO Health - old_team", dashboard_id="dash_stale")
        external = MagicMock(display_name="Other Dashboard", dashboard_id="dash_other")
        mock_workspace_client.lakeview.list.return_value = [active, stale, external]

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        deleted = provisioner.cleanup_stale_dashboards(
            "/Workspace/Shared/DPO", {"ml_team"}
        )

        mock_workspace_client.lakeview.trash.assert_called_once_with("dash_stale")
        assert deleted == ["DPO Health - old_team"]

    def test_cleanup_stale_dashboards_handles_list_error(
        self, mock_workspace_client, sample_config
    ):
        """Test stale cleanup returns empty list when dashboard listing fails."""
        mock_workspace_client.lakeview.list.side_effect = RuntimeError("cannot list")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        deleted = provisioner.cleanup_stale_dashboards(
            "/Workspace/Shared/DPO", {"ml_team"}
        )

        assert deleted == []

    def test_cleanup_stale_dashboards_handles_trash_error(
        self, mock_workspace_client, sample_config
    ):
        """Trash failures for stale dashboards should be swallowed."""
        stale = MagicMock(display_name="DPO Health - old_team", dashboard_id="dash_stale")
        mock_workspace_client.lakeview.list.return_value = [stale]
        mock_workspace_client.lakeview.trash.side_effect = RuntimeError("cannot trash")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        deleted = provisioner.cleanup_stale_dashboards(
            "/Workspace/Shared/DPO", {"ml_team"}
        )

        assert deleted == []

    def test_generate_custom_dashboard_template(
        self, mock_workspace_client, sample_config
    ):
        """Test custom template generator embeds table and view values."""
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        template = provisioner.generate_custom_dashboard_template(
            unified_view="test_catalog.global_monitoring.unified_drift_metrics",
            table_name="test_catalog.ml.predictions",
            dashboard_name="Prediction Drift",
        )

        assert template["displayName"] == "Prediction Drift"
        assert "predictions" in template["pages"][0]["displayName"]
        assert (
            "test_catalog.ml.predictions"
            in template["datasets"][0]["query"]
        )
