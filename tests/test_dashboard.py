"""Tests for DPO dashboard provisioning module."""

import json
from unittest.mock import MagicMock, call

import pytest
from databricks.sdk.errors import ResourceAlreadyExists

from dpo.dashboard import DashboardProvisioner
from dpo.naming import build_group_artifact_names


class TestDashboardProvisioner:
    """Tests for DashboardProvisioner behavior."""

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
        assert "unified_performance" in ds_by_name
        assert "perf_vs_drift" in ds_by_name
        assert not any(name.startswith("coverage_") for name in ds_by_name)
        assert ds_by_name["unified_drift"]["queryLines"] == [
            "SELECT * FROM test_catalog.global_monitoring.unified_drift_metrics"
        ]
        assert ds_by_name["unified_performance"]["queryLines"] == [
            "SELECT * FROM test_catalog.global_monitoring.unified_performance_metrics"
        ]
        assert "{unified_drift_view}" not in ds_by_name["perf_vs_drift"]["queryLines"][0]
        assert "{unified_performance_view}" not in ds_by_name["perf_vs_drift"]["queryLines"][0]

        # warehouse_id is forwarded from config
        assert dashboard_payload.warehouse_id == "test_warehouse_123"

    def test_deploy_dashboard_uses_empty_perf_datasets_when_perf_view_unavailable(
        self, mock_workspace_client, sample_config
    ):
        """If no performance view is provided/resolvable, performance datasets should be valid empty queries."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_123"
        )
        mock_workspace_client.tables.get.side_effect = RuntimeError("not found")

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        dashboard_id = provisioner.deploy_dashboard(
            unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics_default",
            parent_path="/Workspace/Shared/DPO",
            unified_performance_view=None,
        )

        assert dashboard_id == "dash_123"
        dashboard_payload = mock_workspace_client.lakeview.create.call_args.kwargs[
            "dashboard"
        ]
        serialized = json.loads(dashboard_payload.serialized_dashboard)
        ds_by_name = {ds["name"]: ds for ds in serialized["datasets"]}
        perf_query = ds_by_name["unified_performance"]["queryLines"][0]
        perf_vs_drift_query = ds_by_name["perf_vs_drift"]["queryLines"][0]
        assert "WHERE 1=0" in perf_query
        assert "WHERE 1=0" in perf_vs_drift_query
        assert "unified_performance_metrics_default" not in perf_query

    def test_deploy_dashboard_uses_empty_profile_dataset_when_profile_view_unavailable(
        self, mock_workspace_client, sample_config
    ):
        """If profile view is not provided/resolvable, unified_profile dataset should remain valid."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_123"
        )

        def _tables_get(**kwargs):
            full_name = kwargs.get("full_name")
            if full_name and "_profile" in full_name:
                raise RuntimeError("profile view not found")
            info = MagicMock()
            info.full_name = full_name or "test_catalog.global_monitoring.unified_performance_metrics_default"
            return info

        mock_workspace_client.tables.get.side_effect = _tables_get

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        dashboard_id = provisioner.deploy_dashboard(
            unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics_default",
            parent_path="/Workspace/Shared/DPO",
            unified_profile_view=None,
            unified_performance_view=None,
        )

        assert dashboard_id == "dash_123"
        dashboard_payload = mock_workspace_client.lakeview.create.call_args.kwargs[
            "dashboard"
        ]
        serialized = json.loads(dashboard_payload.serialized_dashboard)
        ds_by_name = {ds["name"]: ds for ds in serialized["datasets"]}
        profile_query = ds_by_name["unified_profile"]["queryLines"][0]
        assert "WHERE 1=0" in profile_query

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

        assert "COALESCE(`drift_threshold`, 0.4)" in fields_by_name["critical_alerts"]
        assert (
            "LEAST(COALESCE(`drift_threshold`, 0.4), "
            "GREATEST(0.1, COALESCE(`drift_threshold`, 0.4) / 2.0))"
        ) in fields_by_name["warning_alerts"]
        assert "< COALESCE(`drift_threshold`, 0.4)" in fields_by_name["warning_alerts"]

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

        group_artifacts = {
            "ml_team": build_group_artifact_names("cat.sch", "ml_team"),
            "default": build_group_artifact_names("cat.sch", "default"),
        }
        results = provisioner.deploy_dashboards_by_group(
            group_artifacts, "/Workspace/Shared/DPO"
        )

        assert results == {"ml_team": "dash_ml", "default": "dash_default"}
        assert provisioner.deploy_dashboard.call_count == 2
        provisioner.deploy_dashboard.assert_has_calls(
            [
                call(
                    "cat.sch.unified_drift_metrics_ml_team",
                    "/Workspace/Shared/DPO",
                    unified_profile_view="cat.sch.unified_profile_metrics_ml_team",
                    unified_performance_view="cat.sch.unified_performance_metrics_ml_team",
                    dashboard_name="DPO Health - ml_team",
                ),
                call(
                    "cat.sch.unified_drift_metrics_default",
                    "/Workspace/Shared/DPO",
                    unified_profile_view="cat.sch.unified_profile_metrics_default",
                    unified_performance_view="cat.sch.unified_performance_metrics_default",
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

    def test_cleanup_stale_dashboards_skips_on_empty_active_groups(
        self, mock_workspace_client, sample_config
    ):
        """Empty active_group_names should skip cleanup to prevent accidental deletion."""
        stale = MagicMock(display_name="DPO Health - ml_team", dashboard_id="dash_ml")
        mock_workspace_client.lakeview.list.return_value = [stale]
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        deleted = provisioner.cleanup_stale_dashboards("/Workspace/Shared/DPO", set())

        assert deleted == []
        mock_workspace_client.lakeview.list.assert_not_called()
        mock_workspace_client.lakeview.trash.assert_not_called()

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
            in template["datasets"][0]["queryLines"][0]
        )
        assert template["pages"][0]["pageType"] == "PAGE_TYPE_CANVAS"

    def test_deploy_executive_rollup_creates_and_escapes_group_names(
        self, mock_workspace_client, sample_config
    ):
        """Rollup deployment should create dashboard and SQL-escape group names."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="rollup_123"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        dashboard_id = provisioner.deploy_executive_rollup(
            {
                "O'Reilly": build_group_artifact_names("cat.gm", "O'Reilly"),
            },
            "/Workspace/Shared/DPO",
        )

        assert dashboard_id == "rollup_123"
        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        serialized = json.loads(payload.serialized_dashboard)
        drift_query = next(
            ds["queryLines"][0] for ds in serialized["datasets"] if ds["name"] == "rollup_drift"
        )
        dataset_names = {ds["name"] for ds in serialized["datasets"]}
        assert dataset_names == {"rollup_drift"}
        assert "O''Reilly" in drift_query
        assert "O'Reilly" not in drift_query

    def test_deploy_executive_rollup_updates_existing(
        self, mock_workspace_client, sample_config
    ):
        """Rollup deployment should update existing dashboard when one is found."""
        existing = MagicMock(display_name="DPO Executive Rollup", dashboard_id="rollup_existing")
        mock_workspace_client.lakeview.list.return_value = [existing]
        mock_workspace_client.lakeview.update.return_value = MagicMock(
            dashboard_id="rollup_existing"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)

        dashboard_id = provisioner.deploy_executive_rollup(
            {
                "default": build_group_artifact_names("cat.gm", "default"),
            },
            "/Workspace/Shared/DPO",
        )

        assert dashboard_id == "rollup_existing"
        mock_workspace_client.lakeview.update.assert_called_once()
        mock_workspace_client.lakeview.create.assert_not_called()

    def test_deploy_dashboard_recovers_from_resource_already_exists(
        self, mock_workspace_client, sample_config
    ):
        """When lakeview.list misses the dashboard but create raises ResourceAlreadyExists,
        the code should resolve via workspace path and update instead."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.side_effect = ResourceAlreadyExists(
            "Node named 'DPO Global Health - test_catalog.lvdash.json' already exists"
        )
        ws_status = MagicMock()
        ws_status.resource_id = "dash_resolved"
        mock_workspace_client.workspace.get_status.return_value = ws_status
        mock_workspace_client.lakeview.update.return_value = MagicMock(
            dashboard_id="dash_resolved"
        )

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        dashboard_id = provisioner.deploy_dashboard(
            unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics",
            parent_path="/Workspace/Shared/DPO",
        )

        assert dashboard_id == "dash_resolved"
        mock_workspace_client.workspace.get_status.assert_called_once_with(
            "/Workspace/Shared/DPO/DPO Global Health - test_catalog.lvdash.json"
        )
        mock_workspace_client.lakeview.update.assert_called_once()

    def test_deploy_dashboard_reraises_when_resolve_fails(
        self, mock_workspace_client, sample_config
    ):
        """If both create and workspace path resolution fail, the original error propagates."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.side_effect = ResourceAlreadyExists(
            "already exists"
        )
        mock_workspace_client.workspace.get_status.side_effect = RuntimeError("not found")

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        with pytest.raises(ResourceAlreadyExists):
            provisioner.deploy_dashboard(
                unified_drift_view="test_catalog.global_monitoring.unified_drift_metrics",
                parent_path="/Workspace/Shared/DPO",
            )

    def test_deploy_executive_rollup_recovers_from_resource_already_exists(
        self, mock_workspace_client, sample_config
    ):
        """Rollup deployment should also recover from ResourceAlreadyExists."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.side_effect = ResourceAlreadyExists(
            "already exists"
        )
        ws_status = MagicMock()
        ws_status.resource_id = "rollup_resolved"
        mock_workspace_client.workspace.get_status.return_value = ws_status
        mock_workspace_client.lakeview.update.return_value = MagicMock(
            dashboard_id="rollup_resolved"
        )

        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        dashboard_id = provisioner.deploy_executive_rollup(
            {"default": build_group_artifact_names("cat.gm", "default")},
            "/Workspace/Shared/DPO",
        )

        assert dashboard_id == "rollup_resolved"
        mock_workspace_client.lakeview.update.assert_called_once()


class TestWidgetSpecFormat:
    """Validate all widget specs use the correct Lakeview serialized dashboard format."""

    VALID_WIDGET_TYPES = {"counter", "table", "bar", "line", "scatter", "pie"}
    VERSION_BY_TYPE = {
        "counter": 2,
        "table": 2,
        "bar": 3,
        "line": 3,
        "scatter": 3,
        "pie": 3,
    }

    @staticmethod
    def _extract_widgets(serialized: dict) -> list:
        widgets = []
        for page in serialized.get("pages", []):
            for item in page.get("layout", []):
                widget = item.get("widget", {})
                if "spec" in widget:
                    widgets.append(widget)
        return widgets

    def test_global_health_widgets_have_valid_specs(
        self, mock_workspace_client, sample_config
    ):
        """Every widget in the global health dashboard must have widgetType, version, and frame."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_spec"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_dashboard(
            unified_drift_view="cat.sch.drift",
            parent_path="/Workspace/Shared/DPO",
        )

        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        serialized = json.loads(payload.serialized_dashboard)
        widgets = self._extract_widgets(serialized)
        assert len(widgets) > 0, "Expected at least one widget"

        for widget in widgets:
            spec = widget["spec"]
            name = widget["name"]
            assert "widgetType" in spec, f"{name}: missing widgetType"
            assert "type" not in spec, f"{name}: uses deprecated 'type' key"
            assert spec["widgetType"] in self.VALID_WIDGET_TYPES, (
                f"{name}: invalid widgetType '{spec['widgetType']}'"
            )
            expected_version = self.VERSION_BY_TYPE[spec["widgetType"]]
            assert spec.get("version") == expected_version, (
                f"{name}: version should be {expected_version}, got {spec.get('version')}"
            )
            assert "frame" in spec, f"{name}: missing frame"
            assert spec["frame"].get("showTitle") is True, f"{name}: frame.showTitle not True"
            assert spec["frame"].get("title"), f"{name}: frame.title is empty"
            assert "encodings" in spec, f"{name}: missing encodings"

    def test_rollup_widgets_have_valid_specs(
        self, mock_workspace_client, sample_config
    ):
        """Every widget in the executive rollup dashboard must have widgetType, version, and frame."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="rollup_spec"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_executive_rollup(
            {"default": build_group_artifact_names("cat.gm", "default")},
            "/Workspace/Shared/DPO",
        )

        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        serialized = json.loads(payload.serialized_dashboard)
        widgets = self._extract_widgets(serialized)
        assert len(widgets) > 0

        for widget in widgets:
            spec = widget["spec"]
            name = widget["name"]
            assert "widgetType" in spec, f"{name}: missing widgetType"
            assert "type" not in spec, f"{name}: uses deprecated 'type' key"
            assert spec["widgetType"] in self.VALID_WIDGET_TYPES
            assert spec.get("version") == self.VERSION_BY_TYPE[spec["widgetType"]]
            assert "frame" in spec and spec["frame"].get("title")
            assert "encodings" in spec

    def test_custom_template_widget_has_valid_spec(
        self, mock_workspace_client, sample_config
    ):
        """Custom template widget must have widgetType, version, and frame."""
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        template = provisioner.generate_custom_dashboard_template(
            unified_view="cat.sch.drift",
            table_name="cat.sch.predictions",
            dashboard_name="Test",
        )

        widgets = self._extract_widgets(template)
        assert len(widgets) == 1
        spec = widgets[0]["spec"]
        assert spec["widgetType"] == "line"
        assert spec["version"] == 3
        assert spec["frame"]["showTitle"] is True
        assert spec["frame"]["title"]

    def test_table_widgets_have_column_encodings(
        self, mock_workspace_client, sample_config
    ):
        """Table widgets must define encodings.columns with fieldName and displayName."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_tbl"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_dashboard(
            unified_drift_view="cat.sch.drift",
            parent_path="/Workspace/Shared/DPO",
        )

        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        serialized = json.loads(payload.serialized_dashboard)
        widgets = self._extract_widgets(serialized)
        table_widgets = [w for w in widgets if w["spec"]["widgetType"] == "table"]
        assert len(table_widgets) > 0

        for widget in table_widgets:
            columns = widget["spec"]["encodings"].get("columns", [])
            assert len(columns) > 0, f"{widget['name']}: table has no columns"
            for col in columns:
                assert "fieldName" in col, f"{widget['name']}: column missing fieldName"
                assert "displayName" in col, f"{widget['name']}: column missing displayName"

    def test_no_ordinal_scale_type(
        self, mock_workspace_client, sample_config
    ):
        """Lakeview uses 'categorical' not 'ordinal' for string dimensions."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(
            dashboard_id="dash_ord"
        )
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_dashboard(
            unified_drift_view="cat.sch.drift",
            parent_path="/Workspace/Shared/DPO",
        )

        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        raw = payload.serialized_dashboard
        assert '"ordinal"' not in raw, "Found deprecated 'ordinal' scale type"


class TestDataAccuracyFixes:
    """Verify dashboard data accuracy fixes for drift counting and profile filtering."""

    @staticmethod
    def _deploy_and_parse(mock_workspace_client, sample_config):
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(dashboard_id="d")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_dashboard(
            unified_drift_view="cat.sch.drift", parent_path="/Workspace/Shared/DPO"
        )
        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        return json.loads(payload.serialized_dashboard)

    @staticmethod
    def _find_widget(serialized, widget_name):
        for page in serialized.get("pages", []):
            for item in page.get("layout", []):
                widget = item.get("widget", {})
                if widget.get("name") == widget_name:
                    return widget
        return None

    def test_drift_count_uses_threshold_not_count_star(
        self, mock_workspace_client, sample_config
    ):
        """Wall of Shame drift_count must count only rows exceeding the drift threshold."""
        serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        widget = self._find_widget(serialized, "top_drifters")
        assert widget is not None, "top_drifters widget not found"

        fields = widget["queries"][0]["query"]["fields"]
        drift_count_expr = next(
            f["expression"] for f in fields if f["name"] == "drift_count"
        )
        assert "COUNT(*)" not in drift_count_expr
        assert "COUNT(CASE WHEN" in drift_count_expr
        assert "js_distance" in drift_count_expr
        assert "drift_threshold" in drift_count_expr

    def test_drift_count_respects_configured_threshold(
        self, mock_workspace_client, sample_config
    ):
        """drift_count expression must embed the config-driven drift threshold value."""
        sample_config.alerting.drift_threshold = 0.5
        serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        widget = self._find_widget(serialized, "top_drifters")
        fields = widget["queries"][0]["query"]["fields"]
        drift_count_expr = next(
            f["expression"] for f in fields if f["name"] == "drift_count"
        )
        assert "0.5" in drift_count_expr

    def test_quality_details_table_excludes_table_rows(
        self, mock_workspace_client, sample_config
    ):
        """quality_details_table must filter out column_name = ':table' rows."""
        serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        widget = self._find_widget(serialized, "quality_details_table")
        assert widget is not None
        query = widget["queries"][0]["query"]
        filters = query.get("filters", [])
        filter_exprs = [f["expression"] for f in filters]
        assert any("':table'" in expr for expr in filter_exprs), (
            "quality_details_table missing ':table' filter"
        )

    def test_null_rate_by_column_excludes_table_rows(
        self, mock_workspace_client, sample_config
    ):
        """null_rate_by_column must filter out column_name = ':table' rows."""
        serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        widget = self._find_widget(serialized, "null_rate_by_column")
        assert widget is not None
        query = widget["queries"][0]["query"]
        filters = query.get("filters", [])
        filter_exprs = [f["expression"] for f in filters]
        assert any("':table'" in expr for expr in filter_exprs), (
            "null_rate_by_column missing ':table' filter"
        )

    def test_null_rate_trend_excludes_table_rows(
        self, mock_workspace_client, sample_config
    ):
        """null_rate_trend must filter out column_name = ':table' rows."""
        serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        widget = self._find_widget(serialized, "null_rate_trend")
        assert widget is not None
        query = widget["queries"][0]["query"]
        filters = query.get("filters", [])
        filter_exprs = [f["expression"] for f in filters]
        assert any("':table'" in expr for expr in filter_exprs), (
            "null_rate_trend missing ':table' filter"
        )

    def test_row_count_by_table_does_not_filter_table_rows(
        self, mock_workspace_client, sample_config
    ):
        """row_count_by_table should NOT filter ':table' rows (table-level aggregates are fine)."""
        serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        widget = self._find_widget(serialized, "row_count_by_table")
        assert widget is not None
        query = widget["queries"][0]["query"]
        filters = query.get("filters", [])
        filter_exprs = [f.get("expression", "") for f in filters]
        assert not any("':table'" in expr for expr in filter_exprs), (
            "row_count_by_table should not filter ':table'"
        )


class TestLakeviewTemplateFormat:
    """Verify normalized Lakeview dashboard template fields required for widget-dataset binding."""

    @staticmethod
    def _deploy_and_parse(mock_workspace_client, sample_config):
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(dashboard_id="d")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_dashboard(
            unified_drift_view="cat.sch.drift", parent_path="/Workspace/Shared/DPO"
        )
        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        return payload, json.loads(payload.serialized_dashboard)

    def test_datasets_use_query_lines_not_query(
        self, mock_workspace_client, sample_config
    ):
        """Datasets must use queryLines (list) instead of query (string) for API compatibility."""
        _, serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        for ds in serialized["datasets"]:
            assert "queryLines" in ds, f"Dataset '{ds['name']}' missing queryLines"
            assert isinstance(ds["queryLines"], list), f"Dataset '{ds['name']}' queryLines not a list"
            assert "query" not in ds, f"Dataset '{ds['name']}' still has deprecated 'query' key"

    def test_pages_have_page_type(self, mock_workspace_client, sample_config):
        """Every page must declare pageType for Lakeview rendering."""
        _, serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        for page in serialized["pages"]:
            assert page.get("pageType") == "PAGE_TYPE_CANVAS", (
                f"Page '{page['name']}' missing or wrong pageType"
            )

    def test_widget_queries_have_main_query_name(
        self, mock_workspace_client, sample_config
    ):
        """Primary widget query should be named 'main_query' for dataset binding."""
        _, serialized = self._deploy_and_parse(mock_workspace_client, sample_config)
        for page in serialized["pages"]:
            for item in page["layout"]:
                widget = item.get("widget", {})
                queries = widget.get("queries", [])
                if queries:
                    assert queries[0].get("name") == "main_query", (
                        f"Widget '{widget['name']}' first query not named 'main_query'"
                    )

    def test_rollup_datasets_use_query_lines(
        self, mock_workspace_client, sample_config
    ):
        """Executive rollup datasets must also use queryLines."""
        mock_workspace_client.lakeview.list.return_value = []
        mock_workspace_client.lakeview.create.return_value = MagicMock(dashboard_id="r")
        provisioner = DashboardProvisioner(mock_workspace_client, sample_config)
        provisioner.deploy_executive_rollup(
            {"default": build_group_artifact_names("cat.gm", "default")},
            "/Workspace/Shared/DPO",
        )
        payload = mock_workspace_client.lakeview.create.call_args.kwargs["dashboard"]
        serialized = json.loads(payload.serialized_dashboard)
        for ds in serialized["datasets"]:
            assert "queryLines" in ds, f"Rollup dataset '{ds['name']}' missing queryLines"
            assert "query" not in ds
