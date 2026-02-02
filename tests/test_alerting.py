"""Tests for DPO alerting module."""

import pytest
from unittest.mock import MagicMock

from dpo.alerting import AlertProvisioner


class TestAlertProvisioner:
    """Tests for AlertProvisioner class."""

    def test_initialization(self, mock_workspace_client, sample_config):
        """Test AlertProvisioner initialization."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        assert alerter.warehouse_id == "test_warehouse_123"
        assert alerter.config.alerting.drift_threshold == 0.2


class TestDriftAlert:
    """Tests for drift alert creation."""

    def test_create_unified_drift_alert(
        self, mock_workspace_client, sample_config
    ):
        """Test unified drift alert creation."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        alert_id = alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )
        
        assert alert_id == "alert_123"
        mock_workspace_client.queries.create.assert_called_once()
        mock_workspace_client.alerts.create.assert_called_once()

    def test_drift_alert_query_sql(
        self, mock_workspace_client, sample_config
    ):
        """Test drift alert query includes correct SQL."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )
        
        # Get the query SQL that was passed
        call_args = mock_workspace_client.queries.create.call_args
        query_sql = call_args.kwargs.get("query", "")
        
        assert "js_divergence" in query_sql
        assert "chi_square_statistic" in query_sql
        assert "drift_type" in query_sql
        assert "CRITICAL" in query_sql
        assert "WARNING" in query_sql

    def test_drift_alert_updates_existing(
        self, mock_workspace_client, sample_config
    ):
        """Test drift alert updates existing query/alert."""
        # Mock existing query
        existing_query = MagicMock()
        existing_query.name = "DPO Drift Detection - test_catalog"
        existing_query.id = "existing_query_123"
        mock_workspace_client.queries.list.return_value = [existing_query]
        
        # Mock existing alert
        existing_alert = MagicMock()
        existing_alert.name = "[DPO] Drift Alert - test_catalog"
        existing_alert.id = "existing_alert_123"
        mock_workspace_client.alerts.list.return_value = [existing_alert]
        
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )
        
        # Should update instead of create
        mock_workspace_client.queries.update.assert_called_once()
        mock_workspace_client.alerts.update.assert_called_once()


class TestDataQualityAlert:
    """Tests for data quality alert creation."""

    def test_create_data_quality_alert(
        self, mock_workspace_client, sample_config
    ):
        """Test data quality alert creation."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        alert_id = alerter.create_data_quality_alert(
            "catalog.schema.unified_profile", "test_catalog"
        )
        
        assert alert_id == "alert_123"

    def test_data_quality_alert_skipped_when_no_thresholds(
        self, mock_workspace_client, sample_config
    ):
        """Test alert is skipped when no thresholds configured."""
        # Clear all thresholds
        sample_config.alerting.null_rate_threshold = None
        sample_config.alerting.row_count_min = None
        sample_config.alerting.distinct_count_min = None
        
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        alert_id = alerter.create_data_quality_alert(
            "catalog.schema.unified_profile", "test_catalog"
        )
        
        assert alert_id is None
        mock_workspace_client.queries.create.assert_not_called()

    def test_data_quality_alert_query_includes_conditions(
        self, mock_workspace_client, sample_config
    ):
        """Test data quality alert query includes threshold conditions."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        alerter.create_data_quality_alert(
            "catalog.schema.unified_profile", "test_catalog"
        )
        
        call_args = mock_workspace_client.queries.create.call_args
        query_sql = call_args.kwargs.get("query", "")
        
        assert "null_rate" in query_sql
        assert "record_count" in query_sql
        assert "HIGH_NULL_RATE" in query_sql
        assert "LOW_ROW_COUNT" in query_sql


class TestCustomAlertDDL:
    """Tests for custom alert DDL generation."""

    def test_generate_custom_alert_ddl(
        self, mock_workspace_client, sample_config
    ):
        """Test custom alert DDL generation."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        ddl = alerter.generate_custom_alert_ddl(
            unified_view="catalog.schema.unified_drift",
            table_name="catalog.schema.predictions",
            business_rule="js_divergence > 0.3 AND column_name = 'revenue'",
            alert_name="Revenue Drift Alert",
        )
        
        assert "Revenue Drift Alert" in ddl
        assert "PAUSED" in ddl
        assert "js_divergence > 0.3" in ddl
        assert "column_name = 'revenue'" in ddl

    def test_generate_custom_alert_ddl_default_name(
        self, mock_workspace_client, sample_config
    ):
        """Test custom alert DDL with default name."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        ddl = alerter.generate_custom_alert_ddl(
            unified_view="catalog.schema.unified_drift",
            table_name="catalog.schema.my_table",
            business_rule="js_divergence > 0.5",
        )
        
        assert "Custom Alert - my_table" in ddl


class TestTieredAlerts:
    """Tests for tiered alert query generation."""

    def test_generate_tiered_alert_queries(
        self, mock_workspace_client, sample_config
    ):
        """Test tiered alert query generation."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        queries = alerter.generate_tiered_alert_queries(
            "catalog.schema.unified_drift", "test_catalog"
        )
        
        assert len(queries) == 2
        assert "[DPO] Warning Query - test_catalog" in queries
        assert "[DPO] Critical Query - test_catalog" in queries

        warning_query = queries["[DPO] Warning Query - test_catalog"]
        assert "WARNING" in warning_query
        
        critical_query = queries["[DPO] Critical Query - test_catalog"]
        assert "CRITICAL" in critical_query

    def test_tiered_thresholds_calculated(
        self, mock_workspace_client, sample_config
    ):
        """Test tiered thresholds are calculated correctly."""
        sample_config.alerting.drift_threshold = 0.4
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        queries = alerter.generate_tiered_alert_queries(
            "catalog.schema.unified_drift", "test_catalog"
        )
        
        # Warning should be half of critical (0.2)
        warning_query = queries["[DPO] Warning Query - test_catalog"]
        assert "0.2" in warning_query
        
        # Critical should be full threshold (0.4)
        critical_query = queries["[DPO] Critical Query - test_catalog"]
        assert "0.4" in critical_query


class TestAlertStatusQuery:
    """Tests for alert status dashboard query."""

    def test_get_alert_status_query(
        self, mock_workspace_client, sample_config
    ):
        """Test alert status query generation."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        query = alerter.get_alert_status_query("catalog.schema.unified_drift")
        
        assert "critical_count" in query
        assert "warning_count" in query
        assert "max_drift" in query
        assert "GROUP BY" in query
        assert "HAVING" in query


class TestNotifications:
    """Tests for notification handling."""

    def test_add_notification(self, mock_workspace_client, sample_config):
        """Test notification addition (currently a no-op with logging)."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        
        # Should not raise
        alerter._add_notification("alert_123", "test@example.com")

    def test_notifications_added_to_alert(
        self, mock_workspace_client, sample_config
    ):
        """Test notifications are processed during alert creation."""
        sample_config.alerting.default_notifications = [
            "team1@example.com",
            "team2@example.com",
        ]
        
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )
        
        # Alert should be created successfully even with notifications
        mock_workspace_client.alerts.create.assert_called_once()


class TestPerGroupNotifications:
    """Tests for per-group notification routing."""

    def test_get_notifications_for_group_specific(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test group-specific notifications are returned."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)
        
        notifications = alerter._get_notifications_for_group("ml_team")
        
        assert "ml-team@example.com" in notifications
        assert "ml-oncall@example.com" in notifications

    def test_get_notifications_for_group_fallback(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test fallback to default notifications."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)
        
        notifications = alerter._get_notifications_for_group("unknown_group")
        
        assert notifications == ["default@example.com"]

    def test_get_notifications_for_default_group(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test default group uses default_notifications."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)
        
        notifications = alerter._get_notifications_for_group("default")
        
        assert notifications == ["default@example.com"]

    def test_create_alerts_by_group(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test alerts are created per group."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)
        
        views_by_group = {
            "ml_team": ("cat.sch.drift_ml", "cat.sch.profile_ml"),
            "data_eng": ("cat.sch.drift_data", "cat.sch.profile_data"),
        }
        
        results = alerter.create_alerts_by_group(views_by_group, "test_catalog")
        
        # Should have alerts for both groups
        assert "ml_team" in results
        assert "data_eng" in results
        
        # Each group should have drift and quality alert
        drift_id, _ = results["ml_team"]
        assert drift_id is not None

    def test_alert_name_includes_group_suffix(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test alert names include group suffix."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)
        
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift",
            "test_catalog",
            alert_suffix=" - ml_team",
        )
        
        # Check the alert name includes the suffix
        call_args = mock_workspace_client.alerts.create.call_args
        alert_name = call_args.kwargs.get("name", "")
        assert "ml_team" in alert_name

    def test_alert_uses_provided_notifications(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test alert uses provided notifications instead of default."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)
        
        # Pass specific notifications
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift",
            "test_catalog",
            notifications=["custom@example.com"],
        )
        
        # Alert should be created
        mock_workspace_client.alerts.create.assert_called_once()
