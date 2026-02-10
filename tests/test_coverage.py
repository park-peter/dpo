"""Tests for DPO coverage governance module."""

from unittest.mock import MagicMock

import pytest

from dpo.config import (
    DiscoveryConfig,
    MonitoredTableConfig,
    OrchestratorConfig,
    ProfileConfig,
)
from dpo.coverage import (
    CoverageAnalyzer,
    CoverageReport,
    OrphanMonitor,
    StaleMonitor,
    UnmonitoredTable,
)
from dpo.discovery import DiscoveredTable


@pytest.fixture
def coverage_config() -> OrchestratorConfig:
    """Config for coverage tests."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        warehouse_id="test_warehouse",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.ml.model_a": MonitoredTableConfig(),
            "test_catalog.ml.model_b": MonitoredTableConfig(),
        },
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring",
            prediction_column="pred",
            timestamp_column="ts",
        ),
        stale_monitor_days=30,
    )


@pytest.fixture
def mock_w():
    """Mock WorkspaceClient for coverage tests."""
    w = MagicMock()

    # Schemas
    schema1 = MagicMock()
    schema1.name = "ml"
    schema2 = MagicMock()
    schema2.name = "information_schema"
    w.schemas.list.return_value = [schema1, schema2]

    # Tables in catalog
    table_a = MagicMock()
    table_a.full_name = "test_catalog.ml.model_a"
    table_b = MagicMock()
    table_b.full_name = "test_catalog.ml.model_b"
    table_c = MagicMock()
    table_c.full_name = "test_catalog.ml.unmonitored_table"
    w.tables.list.return_value = [table_a, table_b, table_c]

    # No existing monitors
    w.data_quality.list_monitor.return_value = []

    return w


class TestCoverageReport:
    """Tests for CoverageReport dataclass."""

    def test_empty_report(self):
        """Empty report serializes correctly."""
        report = CoverageReport(timestamp="2026-01-01T00:00:00Z")
        data = report.to_dict()
        assert data["summary"]["total_catalog_tables"] == 0
        assert data["summary"]["coverage_pct"] == 0.0
        assert data["unmonitored"] == []
        assert data["stale"] == []
        assert data["orphans"] == []

    def test_report_with_data(self):
        """Report with data serializes all fields."""
        report = CoverageReport(
            timestamp="2026-01-01T00:00:00Z",
            total_catalog_tables=10,
            total_monitored=7,
            coverage_pct=70.0,
            unmonitored=[
                UnmonitoredTable(full_name="cat.sch.t1", schema_name="sch"),
                UnmonitoredTable(full_name="cat.sch.t2", schema_name="sch"),
            ],
            stale=[
                StaleMonitor(table_name="cat.sch.t3", monitor_id="m1", days_since_refresh=45),
            ],
            orphans=[
                OrphanMonitor(table_name="cat.sch.t4", monitor_id="m2"),
            ],
        )
        data = report.to_dict()
        assert data["summary"]["total_catalog_tables"] == 10
        assert data["summary"]["coverage_pct"] == 70.0
        assert data["summary"]["unmonitored_count"] == 2
        assert data["summary"]["stale_count"] == 1
        assert data["summary"]["orphan_count"] == 1

    def test_report_sorted_output(self):
        """Output lists are sorted by table name."""
        report = CoverageReport(
            unmonitored=[
                UnmonitoredTable(full_name="cat.sch.zebra", schema_name="sch"),
                UnmonitoredTable(full_name="cat.sch.alpha", schema_name="sch"),
            ],
        )
        data = report.to_dict()
        assert data["unmonitored"][0]["table"] == "cat.sch.alpha"
        assert data["unmonitored"][1]["table"] == "cat.sch.zebra"


class TestCoverageAnalyzer:
    """Tests for CoverageAnalyzer."""

    def test_find_unmonitored(self, mock_w, coverage_config):
        """Unmonitored tables are detected."""
        analyzer = CoverageAnalyzer(mock_w, coverage_config)

        discovered = [
            DiscoveredTable(full_name="test_catalog.ml.model_a", columns=[]),
            DiscoveredTable(full_name="test_catalog.ml.model_b", columns=[]),
        ]

        report = analyzer.analyze(discovered)

        # There are 3 catalog tables, 0 monitored (no monitors in mock)
        assert report.total_catalog_tables == 3
        assert report.total_monitored == 0
        assert len(report.unmonitored) == 3

    def test_orphan_detection(self, mock_w, coverage_config):
        """Monitors not in config are flagged as orphans."""
        monitor = MagicMock()
        monitor.object_id = "orphan_table_id"
        monitor.monitor_id = "orphan_mon"
        mock_w.data_quality.list_monitor.return_value = [monitor]

        orphan_info = MagicMock()
        orphan_info.full_name = "test_catalog.ml.orphan_table"

        def get_table_side_effect(**kwargs):
            if kwargs.get("table_id") == "orphan_table_id":
                return orphan_info
            return MagicMock(full_name="unknown")

        mock_w.tables.get.side_effect = get_table_side_effect

        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        discovered = [
            DiscoveredTable(full_name="test_catalog.ml.model_a", columns=[]),
        ]

        report = analyzer.analyze(discovered)
        orphan_names = [o.table_name for o in report.orphans]
        assert "test_catalog.ml.orphan_table" in orphan_names

    def test_cross_catalog_monitors_excluded(self, mock_w, coverage_config):
        """Monitors from a different catalog are not counted."""
        in_catalog_monitor = MagicMock()
        in_catalog_monitor.object_id = "in_cat_id"
        in_catalog_monitor.monitor_id = "in_cat_mon"
        cross_catalog_monitor = MagicMock()
        cross_catalog_monitor.object_id = "cross_cat_id"
        cross_catalog_monitor.monitor_id = "cross_cat_mon"
        mock_w.data_quality.list_monitor.return_value = [
            in_catalog_monitor,
            cross_catalog_monitor,
        ]

        in_cat_info = MagicMock()
        in_cat_info.full_name = "test_catalog.ml.model_a"
        cross_cat_info = MagicMock()
        cross_cat_info.full_name = "other_catalog.ml.some_table"

        def get_table_side_effect(**kwargs):
            tid = kwargs.get("table_id")
            if tid == "in_cat_id":
                return in_cat_info
            if tid == "cross_cat_id":
                return cross_cat_info
            return MagicMock(full_name="unknown")

        mock_w.tables.get.side_effect = get_table_side_effect

        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        discovered = [DiscoveredTable(full_name="test_catalog.ml.model_a", columns=[])]
        report = analyzer.analyze(discovered)

        assert report.total_monitored == 1
        # Cross-catalog monitor should NOT appear as orphan
        orphan_names = [o.table_name for o in report.orphans]
        assert "other_catalog.ml.some_table" not in orphan_names

    def test_exclude_schemas_respected(self, mock_w):
        """Coverage universe respects discovery.exclude_schemas."""
        config = OrchestratorConfig(
            catalog_name="test_catalog",
            warehouse_id="wh",
            include_tagged_tables=True,
            discovery=DiscoveryConfig(exclude_schemas=["information_schema", "tmp_*"]),
            monitored_tables={},
            profile_defaults=ProfileConfig(
                profile_type="INFERENCE",
                output_schema_name="monitoring",
                prediction_column="pred",
                timestamp_column="ts",
            ),
        )

        schema_ml = MagicMock()
        schema_ml.name = "ml"
        schema_tmp = MagicMock()
        schema_tmp.name = "tmp_scratch"
        schema_info = MagicMock()
        schema_info.name = "information_schema"
        mock_w.schemas.list.return_value = [schema_ml, schema_tmp, schema_info]

        table_a = MagicMock()
        table_a.full_name = "test_catalog.ml.model_a"
        mock_w.tables.list.return_value = [table_a]

        analyzer = CoverageAnalyzer(mock_w, config)
        report = analyzer.analyze([])

        # Only "ml" schema tables should appear (tmp_scratch and information_schema excluded)
        assert report.total_catalog_tables == 1
        mock_w.tables.list.assert_called_once_with(
            catalog_name="test_catalog", schema_name="ml"
        )

    def test_include_schemas_respected(self, mock_w):
        """Coverage universe respects discovery.include_schemas."""
        config = OrchestratorConfig(
            catalog_name="test_catalog",
            warehouse_id="wh",
            include_tagged_tables=True,
            discovery=DiscoveryConfig(
                include_schemas=["ml"],
                exclude_schemas=[],
            ),
            monitored_tables={},
            profile_defaults=ProfileConfig(
                profile_type="INFERENCE",
                output_schema_name="monitoring",
                prediction_column="pred",
                timestamp_column="ts",
            ),
        )

        schema_ml = MagicMock()
        schema_ml.name = "ml"
        schema_other = MagicMock()
        schema_other.name = "analytics"
        mock_w.schemas.list.return_value = [schema_ml, schema_other]

        table_a = MagicMock()
        table_a.full_name = "test_catalog.ml.model_a"
        mock_w.tables.list.return_value = [table_a]

        analyzer = CoverageAnalyzer(mock_w, config)
        report = analyzer.analyze([])

        assert report.total_catalog_tables == 1
        mock_w.tables.list.assert_called_once_with(
            catalog_name="test_catalog", schema_name="ml"
        )

    def test_list_catalog_tables_handles_schema_list_error(self, mock_w, coverage_config):
        """Schema listing failures should return an empty catalog table set."""
        mock_w.schemas.list.side_effect = RuntimeError("schema list failed")
        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._list_catalog_tables() == {}

    def test_list_catalog_tables_handles_table_list_error(self, mock_w, coverage_config):
        """Per-schema table listing failures should be skipped."""
        schema = MagicMock()
        schema.name = "ml"
        mock_w.schemas.list.return_value = [schema]
        mock_w.tables.list.side_effect = RuntimeError("table list failed")
        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._list_catalog_tables() == {}

    def test_get_monitored_table_ids_handles_list_error(self, mock_w, coverage_config):
        """Monitor list API failures should return no monitored IDs."""
        mock_w.data_quality.list_monitor.side_effect = RuntimeError("monitor list failed")
        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._get_monitored_table_ids() == {}

    def test_get_monitored_table_ids_skips_invalid_monitor_entries(
        self, mock_w, coverage_config
    ):
        """Monitors missing object IDs or unresolvable table IDs are skipped."""
        missing_obj = MagicMock()
        missing_obj.object_id = None
        unresolved = MagicMock()
        unresolved.object_id = "bad_table_id"
        unresolved.monitor_id = "m1"
        mock_w.data_quality.list_monitor.return_value = [missing_obj, unresolved]
        mock_w.tables.get.side_effect = RuntimeError("not found")

        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._get_monitored_table_ids() == {}

    def test_resolve_table_names_ignores_lookup_errors(self, mock_w, coverage_config):
        """Name resolution should skip IDs that fail table lookup."""
        mock_w.tables.get.side_effect = RuntimeError("lookup failed")
        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._resolve_table_names({"obj1": "m1"}) == set()

    def test_find_stale_without_data_profiling_config(self, mock_w, coverage_config):
        """Monitors lacking profiling config should still be marked stale with unknown status."""
        monitor = MagicMock()
        monitor.data_profiling_config = None
        mock_w.data_quality.get_monitor.return_value = monitor
        table_info = MagicMock()
        table_info.full_name = "test_catalog.ml.model_a"
        mock_w.tables.get.return_value = table_info

        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        stale = analyzer._find_stale({"obj1": "m1"})
        assert len(stale) == 1
        assert stale[0].status == "unknown"

    def test_find_stale_handles_refresh_list_errors(self, mock_w, coverage_config):
        """Refresh API failures should not prevent stale detection."""
        status = MagicMock()
        status.value = "ACTIVE"
        cfg = MagicMock()
        cfg.status = status
        monitor = MagicMock()
        monitor.data_profiling_config = cfg
        mock_w.data_quality.get_monitor.return_value = monitor
        mock_w.data_quality.list_refresh.side_effect = RuntimeError("refresh list failed")
        table_info = MagicMock()
        table_info.full_name = "test_catalog.ml.model_a"
        mock_w.tables.get.return_value = table_info

        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        stale = analyzer._find_stale({"obj1": "m1"})
        assert len(stale) == 1
        assert stale[0].status == "ACTIVE"

    def test_find_stale_handles_monitor_fetch_error(self, mock_w, coverage_config):
        """Monitor fetch failures should be skipped safely."""
        mock_w.data_quality.get_monitor.side_effect = RuntimeError("monitor fetch failed")
        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._find_stale({"obj1": "m1"}) == []

    def test_analyze_reuses_table_name_resolution(self, mock_w, coverage_config):
        """Analyze should resolve each monitored table ID only once."""
        monitor = MagicMock()
        monitor.object_id = "obj1"
        monitor.monitor_id = "m1"
        mock_w.data_quality.list_monitor.return_value = [monitor]

        table_info = MagicMock()
        table_info.full_name = "test_catalog.ml.model_a"
        mock_w.tables.get.return_value = table_info

        monitor_details = MagicMock()
        monitor_details.data_profiling_config = None
        mock_w.data_quality.get_monitor.return_value = monitor_details

        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        discovered = [DiscoveredTable(full_name="test_catalog.ml.model_a", columns=[])]
        analyzer.analyze(discovered)

        # table_id->full_name is cached and reused for stale/orphan checks
        assert mock_w.tables.get.call_count == 1

    def test_table_name_from_id_handles_lookup_error(self, mock_w, coverage_config):
        """Table ID resolution should return None when lookup fails."""
        mock_w.tables.get.side_effect = RuntimeError("lookup failed")
        analyzer = CoverageAnalyzer(mock_w, coverage_config)
        assert analyzer._table_name_from_id("obj1") is None
