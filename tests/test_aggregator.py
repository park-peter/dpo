"""Tests for DPO aggregator module."""

from unittest.mock import MagicMock

import pytest

from dpo.aggregator import MetricsAggregator
from dpo.discovery import DiscoveredTable


class TestMetricsAggregator:
    """Tests for MetricsAggregator class."""

    def test_standard_drift_columns(self, mock_workspace_client, sample_config):
        """Test standard drift columns are defined."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        # Should include key drift metrics
        columns_str = " ".join(aggregator.STANDARD_DRIFT_COLUMNS)
        assert "js_divergence" in columns_str
        assert "ks_statistic" in columns_str
        assert "wasserstein_distance" in columns_str
        assert "chi_square_statistic" in columns_str
        assert "drift_type" in columns_str
        assert "window_start" in columns_str
        assert "window_end" in columns_str

    def test_standard_profile_columns(self, mock_workspace_client, sample_config):
        """Test standard profile columns are defined."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        columns_str = " ".join(aggregator.STANDARD_PROFILE_COLUMNS)
        assert "record_count" in columns_str
        assert "null_count" in columns_str
        assert "null_rate" in columns_str
        assert "distinct_count" in columns_str
        assert "mean" in columns_str
        assert "stddev" in columns_str


class TestUnifiedDriftView:
    """Tests for unified drift view generation."""

    def test_generate_drift_view_ddl(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test drift view DDL generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        ddl = aggregator._generate_unified_drift_view_ddl(
            sample_discovered_tables,
            "catalog.schema.unified_drift",
            use_materialized=False,
        )

        assert "CREATE OR REPLACE VIEW" in ddl
        assert "UNION ALL" in ddl
        assert "source_table_name" in ddl
        assert "owner" in ddl
        assert "department" in ddl

    def test_generate_drift_view_ddl_materialized(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test materialized view DDL generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        ddl = aggregator._generate_unified_drift_view_ddl(
            sample_discovered_tables,
            "catalog.schema.unified_drift",
            use_materialized=True,
        )

        assert "CREATE OR REPLACE MATERIALIZED VIEW" in ddl

    def test_generate_drift_view_ddl_empty(
        self, mock_workspace_client, sample_config
    ):
        """Test empty view DDL generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        ddl = aggregator._generate_unified_drift_view_ddl(
            [],  # Empty list
            "catalog.schema.unified_drift",
            use_materialized=False,
        )

        assert "WHERE 1=0" in ddl
        assert "CREATE OR REPLACE VIEW" in ddl

    def test_drift_view_includes_tags(
        self, mock_workspace_client, sample_config, sample_discovered_table
    ):
        """Test tags are injected into view."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        ddl = aggregator._generate_unified_drift_view_ddl(
            [sample_discovered_table],
            "catalog.schema.unified_drift",
            use_materialized=False,
        )

        assert "'data_team'" in ddl  # owner tag
        assert "'analytics'" in ddl  # department tag
        assert "1 as priority" in ddl  # priority tag


class TestUnifiedProfileView:
    """Tests for unified profile view generation."""

    def test_generate_profile_view_ddl(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Test profile view DDL generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        ddl = aggregator._generate_unified_profile_view_ddl(
            sample_discovered_tables,
            "catalog.schema.unified_profile",
            use_materialized=False,
        )

        assert "CREATE OR REPLACE VIEW" in ddl
        assert "UNION ALL" in ddl
        assert "source_table_name" in ddl
        assert "record_count" in ddl
        assert "null_rate" in ddl

    def test_generate_profile_view_ddl_empty(
        self, mock_workspace_client, sample_config
    ):
        """Test empty profile view DDL generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        ddl = aggregator._generate_unified_profile_view_ddl(
            [],
            "catalog.schema.unified_profile",
            use_materialized=False,
        )

        assert "WHERE 1=0" in ddl


class TestHelperQueries:
    """Tests for helper query generation."""

    def test_drift_summary_query(self, mock_workspace_client, sample_config):
        """Test drift summary (Wall of Shame) query generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        query = aggregator.get_drift_summary_query("catalog.schema.unified_drift")

        assert "source_table_name" in query
        assert "MAX(js_divergence)" in query
        assert "AVG(js_divergence)" in query
        assert "GROUP BY" in query
        assert "ORDER BY" in query
        assert "LIMIT 20" in query

    def test_feature_drift_query(self, mock_workspace_client, sample_config):
        """Test per-feature drift query generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        query = aggregator.get_feature_drift_query(
            "catalog.schema.unified_drift",
            "test_catalog.test_schema.test_table",
        )

        assert "column_name" in query
        assert "js_divergence" in query
        assert "chi_square_statistic" in query
        assert "drift_type" in query
        assert "WHERE source_table_name" in query

    def test_data_quality_summary_query(self, mock_workspace_client, sample_config):
        """Test data quality summary query generation."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        query = aggregator.get_data_quality_summary_query(
            "catalog.schema.unified_profile"
        )

        assert "source_table_name" in query
        assert "AVG(null_rate)" in query
        assert "MAX(null_rate)" in query
        assert "SUM(record_count)" in query
        assert "GROUP BY" in query


class TestMaterializedViewSupport:
    """Tests for materialized view support detection."""

    @pytest.mark.parametrize(
        "serverless_enabled,expected",
        [
            (True, True),
            (False, False),
        ],
    )
    def test_supports_materialized_views_flag(
        self, mock_workspace_client, sample_config, serverless_enabled, expected
    ):
        """Test detection based on warehouse serverless flag."""
        warehouse = MagicMock()
        warehouse.enable_serverless_compute = serverless_enabled
        mock_workspace_client.warehouses.list.return_value = [warehouse]
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        assert aggregator._supports_materialized_views() is expected

    def test_supports_materialized_views_error(
        self, mock_workspace_client, sample_config
    ):
        """Test detection handles errors gracefully."""
        mock_workspace_client.warehouses.list.side_effect = Exception("API Error")

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        assert aggregator._supports_materialized_views() is False


class TestSchemaCreation:
    """Tests for output schema creation."""

    def test_ensure_output_schema_exists(
        self, mock_workspace_client, sample_config
    ):
        """Test schema check when it exists."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        aggregator._ensure_output_schema("catalog.schema.view")

        mock_workspace_client.schemas.get.assert_called_once()

    def test_ensure_output_schema_creates(
        self, mock_workspace_client, sample_config
    ):
        """Test schema creation when it doesn't exist."""
        mock_workspace_client.schemas.get.side_effect = Exception("Not found")

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)
        aggregator._ensure_output_schema("catalog.new_schema.view")

        mock_workspace_client.schemas.create.assert_called_once()


class TestUnifiedViewsByGroup:
    """Tests for per-group unified view creation."""

    def test_create_unified_views_by_group(
        self, mock_workspace_client, sample_config, sample_grouped_tables
    ):
        """Test views are created per group."""
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        results = aggregator.create_unified_views_by_group(
            sample_grouped_tables,
            "test_catalog.global_monitoring",
            "monitor_group",
        )

        # Should have 3 groups: ml_team, data_eng, default
        assert len(results) == 3
        assert "ml_team" in results
        assert "data_eng" in results
        assert "default" in results

        # Check view names
        ml_drift, ml_profile = results["ml_team"]
        assert "unified_drift_metrics_ml_team" in ml_drift
        assert "unified_profile_metrics_ml_team" in ml_profile

    def test_create_unified_views_sanitizes_names(
        self, mock_workspace_client, sample_config
    ):
        """Test group names are sanitized for SQL."""
        tables = [
            DiscoveredTable(
                full_name="cat.sch.tbl1",
                tags={"monitor_group": "Marketing & Sales"},
            ),
        ]

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        results = aggregator.create_unified_views_by_group(
            tables,
            "test_catalog.global_monitoring",
            "monitor_group",
        )

        # Group name should be sanitized
        assert "Marketing & Sales" in results
        drift_view, _ = results["Marketing & Sales"]
        assert "marketing_sales" in drift_view


class TestCleanupStaleViews:
    """Tests for stale view cleanup."""

    def test_cleanup_stale_views(self, mock_workspace_client, sample_config):
        """Test stale views are dropped."""
        # Setup mock to return existing views
        result = MagicMock()
        result.result.data_array = [
            ["unified_drift_metrics_ml_team"],
            ["unified_drift_metrics_old_group"],  # Stale
            ["unified_profile_metrics_ml_team"],
            ["unified_profile_metrics_old_group"],  # Stale
            ["other_view"],  # Not a DPO view
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = result

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        dropped = aggregator.cleanup_stale_views(
            "test_catalog.global_monitoring",
            active_groups={"ml_team"},  # Only ml_team is active
        )

        # Should drop old_group views (2 views)
        assert len(dropped) == 2
        assert any("old_group" in v for v in dropped)

    def test_cleanup_stale_views_no_stale(
        self, mock_workspace_client, sample_config
    ):
        """Test no views dropped when all are active."""
        result = MagicMock()
        result.result.data_array = [
            ["unified_drift_metrics_ml_team"],
            ["unified_profile_metrics_ml_team"],
        ]
        mock_workspace_client.statement_execution.execute_statement.return_value = result

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        dropped = aggregator.cleanup_stale_views(
            "test_catalog.global_monitoring",
            active_groups={"ml_team"},
        )

        assert len(dropped) == 0

    def test_cleanup_stale_views_empty_schema(
        self, mock_workspace_client, sample_config
    ):
        """Test handles empty schema gracefully."""
        result = MagicMock()
        result.result = None
        mock_workspace_client.statement_execution.execute_statement.return_value = result

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        dropped = aggregator.cleanup_stale_views(
            "test_catalog.global_monitoring",
            active_groups={"ml_team"},
        )

        assert dropped == []


class TestViewExecution:
    """Tests for runtime execution wrappers around generated DDL."""

    def test_create_unified_drift_view_raises_on_failed_statement(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Failed SQL execution status should raise RuntimeError."""
        failed = MagicMock()
        failed.status.state.value = "FAILED"
        failed.status.error = "permission denied"
        mock_workspace_client.statement_execution.execute_statement.return_value = failed

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        with pytest.raises(RuntimeError, match="Failed to create view"):
            aggregator.create_unified_drift_view(
                sample_discovered_tables,
                "test_catalog.global_monitoring.unified_drift_metrics",
                use_materialized=False,
            )

    def test_create_unified_profile_view_raises_on_statement_exception(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """Unexpected SQL execution exceptions should be propagated."""
        mock_workspace_client.statement_execution.execute_statement.side_effect = RuntimeError(
            "warehouse offline"
        )
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        with pytest.raises(RuntimeError, match="warehouse offline"):
            aggregator.create_unified_profile_view(
                sample_discovered_tables,
                "test_catalog.global_monitoring.unified_profile_metrics",
                use_materialized=False,
            )

    def test_create_unified_profile_view_raises_on_failed_statement(
        self, mock_workspace_client, sample_config, sample_discovered_tables
    ):
        """FAILED SQL execution status should raise RuntimeError for profile view."""
        failed = MagicMock()
        failed.status.state.value = "FAILED"
        failed.status.error = "permission denied"
        mock_workspace_client.statement_execution.execute_statement.return_value = failed
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        with pytest.raises(RuntimeError, match="Failed to create view"):
            aggregator.create_unified_profile_view(
                sample_discovered_tables,
                "test_catalog.global_monitoring.unified_profile_metrics",
                use_materialized=False,
            )

    def test_ensure_output_schema_ignores_create_errors(
        self, mock_workspace_client, sample_config
    ):
        """Schema creation failures should be logged and not raised."""
        mock_workspace_client.schemas.get.side_effect = RuntimeError("not found")
        mock_workspace_client.schemas.create.side_effect = RuntimeError("cannot create")
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        aggregator._ensure_output_schema("test_catalog.global_monitoring.unified_drift")

        mock_workspace_client.schemas.create.assert_called_once()

    def test_cleanup_stale_views_continues_when_drop_fails(
        self, mock_workspace_client, sample_config
    ):
        """Drop errors for one stale view should not abort cleanup."""
        show_result = MagicMock()
        show_result.result.data_array = [["unified_drift_metrics_old_group"]]

        mock_workspace_client.statement_execution.execute_statement.side_effect = [
            show_result,
            RuntimeError("drop failed"),
        ]

        aggregator = MetricsAggregator(mock_workspace_client, sample_config)
        dropped = aggregator.cleanup_stale_views(
            "test_catalog.global_monitoring",
            active_groups={"ml_team"},
        )

        assert dropped == []

    def test_cleanup_stale_views_handles_show_views_error(
        self, mock_workspace_client, sample_config
    ):
        """SHOW VIEWS errors should return empty dropped list."""
        mock_workspace_client.statement_execution.execute_statement.side_effect = RuntimeError(
            "cannot list views"
        )
        aggregator = MetricsAggregator(mock_workspace_client, sample_config)

        dropped = aggregator.cleanup_stale_views(
            "test_catalog.global_monitoring",
            active_groups={"ml_team"},
        )

        assert dropped == []
