"""Tests for provisioning refresh and monitor-status flows."""

from unittest.mock import MagicMock

from databricks.sdk.service.dataquality import DataProfilingStatus, RefreshState

import dpo.provisioning as provisioning_module
from dpo.discovery import DiscoveredTable
from dpo.provisioning import (
    MonitorStatus,
    ProfileProvisioner,
    RefreshResult,
    get_monitor_statuses,
    print_monitor_statuses,
    wait_for_monitors,
)


def _table(full_name: str) -> DiscoveredTable:
    return DiscoveredTable(full_name=full_name, columns=[], table_type="MANAGED")


class TestRefreshFlows:
    """Tests for monitor refresh triggering and polling."""

    def test_refresh_all_aggregates_results(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """refresh_all should invoke single-table refresh for each table."""
        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        provisioner._refresh_single = MagicMock(
            side_effect=[
                RefreshResult(
                    table_name=sample_discovered_tables[0].full_name,
                    status="triggered",
                    refresh_id="r1",
                ),
                RefreshResult(
                    table_name=sample_discovered_tables[1].full_name,
                    status="failed",
                    error="boom",
                ),
            ]
        )

        results = provisioner.refresh_all(sample_discovered_tables, wait=False)

        assert len(results) == 2
        assert results[0].status == "triggered"
        assert results[1].status == "failed"
        assert provisioner._refresh_single.call_count == 2

    def test_refresh_single_triggered_without_wait(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Without wait, refresh should return triggered immediately."""
        table_info = MagicMock(table_id="table_123")
        mock_workspace_client.tables.get.return_value = table_info

        run_info = MagicMock()
        run_info.state = RefreshState.MONITOR_REFRESH_STATE_PENDING
        run_info.refresh_id = "refresh_1"
        mock_workspace_client.data_quality.create_refresh.return_value = run_info

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        result = provisioner._refresh_single(sample_discovered_table, wait=False)

        assert result.status == "triggered"
        assert result.refresh_id == "refresh_1"

    def test_refresh_single_wait_completes(
        self, monkeypatch, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """With wait=True, refresh should poll until success."""
        monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

        table_info = MagicMock(table_id="table_123")
        mock_workspace_client.tables.get.return_value = table_info

        initial = MagicMock()
        initial.state = RefreshState.MONITOR_REFRESH_STATE_PENDING
        initial.refresh_id = "refresh_1"

        finished = MagicMock()
        finished.state = RefreshState.MONITOR_REFRESH_STATE_SUCCESS
        finished.refresh_id = "refresh_1"

        mock_workspace_client.data_quality.create_refresh.return_value = initial
        mock_workspace_client.data_quality.get_refresh.return_value = finished

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        result = provisioner._refresh_single(sample_discovered_table, wait=True)

        assert result.status == "completed"
        assert result.refresh_id == "refresh_1"

    def test_refresh_single_wait_failed_state(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """With wait=True, non-success terminal state should return failed."""
        table_info = MagicMock(table_id="table_123")
        mock_workspace_client.tables.get.return_value = table_info

        run_info = MagicMock()
        run_info.state = "FAILED"
        run_info.refresh_id = "refresh_1"
        mock_workspace_client.data_quality.create_refresh.return_value = run_info

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        result = provisioner._refresh_single(sample_discovered_table, wait=True)

        assert result.status == "failed"
        assert "Refresh ended with state" in result.error

    def test_refresh_single_handles_exception(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Refresh errors should be returned as failed results, not raised."""
        mock_workspace_client.tables.get.side_effect = RuntimeError("table lookup failed")

        provisioner = ProfileProvisioner(mock_workspace_client, sample_config)
        result = provisioner._refresh_single(sample_discovered_table, wait=False)

        assert result.status == "failed"
        assert "table lookup failed" in result.error


class TestMonitorStatusFlows:
    """Tests for monitor status and wait utilities."""

    def test_get_monitor_statuses_active_no_profile_and_error(self):
        """Status fetch should classify ACTIVE, NO_PROFILE, and ERROR correctly."""
        w = MagicMock()
        tables = [
            _table("test_catalog.sch.a"),
            _table("test_catalog.sch.b"),
            _table("test_catalog.sch.c"),
        ]

        table_a = MagicMock(table_id="id_a")
        table_b = MagicMock(table_id="id_b")
        w.tables.get.side_effect = [table_a, table_b, RuntimeError("lookup failed")]

        monitor_a = MagicMock()
        monitor_a.data_profiling_config = MagicMock(
            status=DataProfilingStatus.DATA_PROFILING_STATUS_ACTIVE,
            latest_monitor_failure_message=None,
        )
        monitor_b = MagicMock()
        monitor_b.data_profiling_config = None
        w.data_quality.get_monitor.side_effect = [monitor_a, monitor_b]

        statuses = get_monitor_statuses(w, tables)

        assert statuses[0].status == DataProfilingStatus.DATA_PROFILING_STATUS_ACTIVE.value
        assert statuses[1].status == "NO_PROFILE"
        assert statuses[2].status == "ERROR"
        assert "lookup failed" in (statuses[2].error_message or "")

    def test_wait_for_monitors_transitions_pending_to_active(self, monkeypatch):
        """Wait loop should poll until monitors leave PENDING state."""
        table_name = "test_catalog.sch.a"
        tables = [_table(table_name)]
        pending = MonitorStatus(
            table_name=table_name,
            status=DataProfilingStatus.DATA_PROFILING_STATUS_PENDING.value,
        )
        active = MonitorStatus(
            table_name=table_name,
            status=DataProfilingStatus.DATA_PROFILING_STATUS_ACTIVE.value,
        )

        get_statuses = MagicMock(side_effect=[[pending], [active], [active]])
        monkeypatch.setattr(provisioning_module, "get_monitor_statuses", get_statuses)
        monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

        final = wait_for_monitors(
            MagicMock(), tables, timeout_seconds=30, poll_interval=0
        )

        assert len(final) == 1
        assert final[0].status == DataProfilingStatus.DATA_PROFILING_STATUS_ACTIVE.value
        assert get_statuses.call_count == 3

    def test_print_monitor_statuses_outputs_table(self, capsys):
        """Status printer should emit tabular output with table names and statuses."""
        statuses = [
            MonitorStatus(
                table_name="test_catalog.sch.a",
                status="ACTIVE",
                error_message=None,
            )
        ]

        print_monitor_statuses(statuses)
        output = capsys.readouterr().out

        assert "test_catalog.sch.a" in output
        assert "ACTIVE" in output
