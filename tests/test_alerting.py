"""Tests for DPO alerting module."""

from unittest.mock import MagicMock

import pytest

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
        mock_workspace_client.alerts_v2.create_alert.assert_called_once()

    def test_drift_alert_query_sql(
        self, mock_workspace_client, sample_config
    ):
        """Test drift alert embeds correct SQL query."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")
        query_sql = alert_v2.query_text

        assert "js_divergence" in query_sql
        assert "chi_square_statistic" in query_sql
        assert "drift_type" in query_sql
        assert "CRITICAL" in query_sql
        assert "WARNING" in query_sql

    def test_drift_alert_updates_existing(
        self, mock_workspace_client, sample_config
    ):
        """Test drift alert updates existing alert."""
        existing_alert = MagicMock()
        existing_alert.display_name = "[DPO] Drift Alert - test_catalog"
        existing_alert.id = "existing_alert_123"
        mock_workspace_client.alerts_v2.list_alerts.return_value = [existing_alert]

        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )

        mock_workspace_client.alerts_v2.update_alert.assert_called_once()
        mock_workspace_client.alerts_v2.create_alert.assert_not_called()

    def test_drift_alert_uses_configured_schedule_and_timezone(
        self, mock_workspace_client, sample_config
    ):
        """Alert schedule should come from config, not hardcoded defaults."""
        sample_config.alerting.alert_cron_schedule = "0 0 6 * * ?"
        sample_config.alerting.alert_timezone = "America/Chicago"
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")

        assert alert_v2.schedule.quartz_cron_schedule == "0 0 6 * * ?"
        assert alert_v2.schedule.timezone_id == "America/Chicago"


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
        sample_config.alerting.null_rate_threshold = None
        sample_config.alerting.row_count_min = None
        sample_config.alerting.distinct_count_min = None

        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        alert_id = alerter.create_data_quality_alert(
            "catalog.schema.unified_profile", "test_catalog"
        )

        assert alert_id is None
        mock_workspace_client.alerts_v2.create_alert.assert_not_called()

    def test_data_quality_alert_query_includes_conditions(
        self, mock_workspace_client, sample_config
    ):
        """Test data quality alert query includes threshold conditions."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        alerter.create_data_quality_alert(
            "catalog.schema.unified_profile", "test_catalog"
        )

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")
        query_sql = alert_v2.query_text

        assert "null_rate" in query_sql
        assert "record_count" in query_sql
        assert "HIGH_NULL_RATE" in query_sql
        assert "LOW_ROW_COUNT" in query_sql

    def test_data_quality_alert_includes_distinct_count_condition(
        self, mock_workspace_client, sample_config
    ):
        """Distinct-count threshold should appear in query when configured."""
        sample_config.alerting.null_rate_threshold = None
        sample_config.alerting.row_count_min = None
        sample_config.alerting.distinct_count_min = 5

        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter.create_data_quality_alert(
            "catalog.schema.unified_profile", "test_catalog"
        )

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")
        query_sql = alert_v2.query_text
        assert "distinct_count < 5" in query_sql
        assert "LOW_CARDINALITY" in query_sql


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

        warning_query = queries["[DPO] Warning Query - test_catalog"]
        assert "0.2" in warning_query

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

    def test_get_alert_status_query_uses_dynamic_warning_threshold(
        self, mock_workspace_client, sample_config
    ):
        """Warning threshold should track configured drift threshold."""
        sample_config.alerting.drift_threshold = 0.4
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        query = alerter.get_alert_status_query("catalog.schema.unified_drift")

        assert "js_divergence >= 0.2" in query
        assert "js_divergence < 0.4" in query


class TestSubscriptionResolution:
    """Tests for notification subscription resolution."""

    def test_email_resolved_to_user_subscription(
        self, mock_workspace_client, sample_config
    ):
        """Test email addresses become user_email subscriptions."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        subs = alerter._resolve_subscriptions(["user@example.com"])

        assert len(subs) == 1
        assert subs[0].user_email == "user@example.com"
        assert subs[0].destination_id is None

    def test_destination_name_resolved_via_api(
        self, mock_workspace_client, sample_config
    ):
        """Test non-email strings are looked up as notification destinations."""
        dest = MagicMock()
        dest.display_name = "ops-slack-channel"
        dest.id = "dest_456"
        mock_workspace_client.notification_destinations.list.return_value = [dest]

        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        subs = alerter._resolve_subscriptions(["ops-slack-channel"])

        assert len(subs) == 1
        assert subs[0].destination_id == "dest_456"
        assert subs[0].user_email is None

    def test_uuid_passed_as_destination_id_directly(
        self, mock_workspace_client, sample_config
    ):
        """Test UUID strings bypass the lookup and are used as destination_id."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        uuid_str = "12345678-1234-1234-1234-123456789abc"
        subs = alerter._resolve_subscriptions([uuid_str])

        assert len(subs) == 1
        assert subs[0].destination_id == uuid_str

    def test_unknown_destination_skipped_with_warning(
        self, mock_workspace_client, sample_config
    ):
        """Test unresolvable destinations are skipped gracefully."""
        mock_workspace_client.notification_destinations.list.return_value = []

        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        subs = alerter._resolve_subscriptions(["nonexistent-channel"])

        assert len(subs) == 0

    def test_mixed_emails_and_destinations(
        self, mock_workspace_client, sample_config
    ):
        """Test a mix of emails and destination names are resolved correctly."""
        dest = MagicMock()
        dest.display_name = "pagerduty-oncall"
        dest.id = "dest_789"
        mock_workspace_client.notification_destinations.list.return_value = [dest]

        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        subs = alerter._resolve_subscriptions([
            "team@example.com",
            "pagerduty-oncall",
        ])

        assert len(subs) == 2
        assert subs[0].user_email == "team@example.com"
        assert subs[1].destination_id == "dest_789"

    def test_destination_lookup_retry_after_transient_failure(
        self, mock_workspace_client, sample_config
    ):
        """Test destination lookup does not crash and retries after transient failures."""
        dest = MagicMock()
        dest.display_name = "ops-slack-channel"
        dest.id = "dest_456"
        mock_workspace_client.notification_destinations.list.side_effect = [
            RuntimeError("temporary error"),
            [dest],
        ]

        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        first = alerter._resolve_subscriptions(["ops-slack-channel"])
        second = alerter._resolve_subscriptions(["ops-slack-channel"])

        assert first == []
        assert len(second) == 1
        assert second[0].destination_id == "dest_456"
        assert mock_workspace_client.notification_destinations.list.call_count == 2


class TestNotificationIntegration:
    """Tests for end-to-end notification attachment to alerts."""

    def test_notifications_attached_to_drift_alert(
        self, mock_workspace_client, sample_config
    ):
        """Test notifications are embedded in the alert evaluation."""
        sample_config.alerting.default_notifications = [
            "team@example.com",
            "oncall@example.com",
        ]

        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")
        notification = alert_v2.evaluation.notification

        assert notification is not None
        assert len(notification.subscriptions) == 2
        assert notification.subscriptions[0].user_email == "team@example.com"
        assert notification.subscriptions[1].user_email == "oncall@example.com"

    def test_empty_notifications_still_creates_alert(
        self, mock_workspace_client, sample_config
    ):
        """Test alerts are created even with no notifications configured."""
        sample_config.alerting.default_notifications = []

        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alert_id = alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift", "test_catalog"
        )

        assert alert_id == "alert_123"
        mock_workspace_client.alerts_v2.create_alert.assert_called_once()


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

    @pytest.mark.parametrize("group_name", ["unknown_group", "default"])
    def test_get_notifications_fallback_to_default(
        self, mock_workspace_client, sample_config_with_groups, group_name
    ):
        """Test unconfigured groups use default notifications."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)

        notifications = alerter._get_notifications_for_group(group_name)

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

        assert "ml_team" in results
        assert "data_eng" in results

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

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")
        assert "ml_team" in alert_v2.display_name

    def test_alert_uses_provided_notifications(
        self, mock_workspace_client, sample_config_with_groups
    ):
        """Test alert uses provided notifications instead of default."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config_with_groups)

        alerter.create_unified_drift_alert(
            "catalog.schema.unified_drift",
            "test_catalog",
            notifications=["custom@example.com"],
        )

        call_args = mock_workspace_client.alerts_v2.create_alert.call_args
        alert_v2 = call_args.kwargs.get("alert")
        subs = alert_v2.evaluation.notification.subscriptions

        assert len(subs) == 1
        assert subs[0].user_email == "custom@example.com"


class TestAlertErrorHandling:
    """Tests for alert creation error-handling paths."""

    def test_create_unified_drift_alert_re_raises(self, mock_workspace_client, sample_config):
        """Drift alert creation should log and re-raise underlying failures."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter._create_or_update_alert = MagicMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            alerter.create_unified_drift_alert(
                "catalog.schema.unified_drift", "test_catalog"
            )

    def test_create_data_quality_alert_re_raises(self, mock_workspace_client, sample_config):
        """Data quality alert creation should log and re-raise underlying failures."""
        alerter = AlertProvisioner(mock_workspace_client, sample_config)
        alerter._create_or_update_alert = MagicMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            alerter.create_data_quality_alert(
                "catalog.schema.unified_profile", "test_catalog"
            )

    def test_find_existing_alert_handles_list_errors(
        self, mock_workspace_client, sample_config
    ):
        """Alert lookup should return None if list API errors."""
        mock_workspace_client.alerts_v2.list_alerts.side_effect = RuntimeError("api down")
        alerter = AlertProvisioner(mock_workspace_client, sample_config)

        found = alerter._find_existing_alert("anything")

        assert found is None
