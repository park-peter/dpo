"""Shared test fixtures for DPO tests."""

from unittest.mock import MagicMock

import pytest

from dpo.config import (
    AlertConfig,
    CustomMetricConfig,
    DiscoveryConfig,
    MonitoredTableConfig,
    ObjectiveFunctionConfig,
    OrchestratorConfig,
    ProfileConfig,
)
from dpo.discovery import DiscoveredTable


def create_mock_column(name: str, type_text: str) -> MagicMock:
    """Helper to create a mock column."""
    col = MagicMock()
    col.name = name
    col.type_text = type_text
    return col


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient."""
    client = MagicMock()

    # Mock current_user
    client.current_user.me.return_value = MagicMock(user_name="test_user@example.com")

    # Mock statement_execution
    result = MagicMock()
    result.result.data_array = [[10]]
    result.status.state.value = "SUCCEEDED"
    client.statement_execution.execute_statement.return_value = result

    # Mock data_quality
    client.data_quality.list_monitor.return_value = []
    client.data_quality.get_monitor.return_value = None
    client.data_quality.create_monitor.return_value = MagicMock(monitor_id="mon_123")

    # Mock tables
    table_info = MagicMock()
    table_info.table_id = "table_123"
    table_info.full_name = "test_catalog.test_schema.test_table"
    table_info.columns = [
        create_mock_column("prediction", "DOUBLE"),
        create_mock_column("label", "STRING"),
        create_mock_column("timestamp", "TIMESTAMP"),
        create_mock_column("region", "STRING"),
    ]
    client.tables.get.return_value = table_info

    # Mock schemas
    schema_info = MagicMock()
    schema_info.schema_id = "schema_123"
    client.schemas.get.return_value = schema_info

    # Mock warehouses
    warehouse = MagicMock()
    warehouse.enable_serverless_compute = True
    client.warehouses.list.return_value = [warehouse]

    # Mock alerts_v2
    client.alerts_v2.list_alerts.return_value = []
    client.alerts_v2.create_alert.return_value = MagicMock(id="alert_123")
    client.alerts_v2.update_alert.return_value = MagicMock(id="alert_123")

    # Mock notification destinations
    client.notification_destinations.list.return_value = []

    return client


@pytest.fixture
def sample_config() -> OrchestratorConfig:
    """Create a sample configuration for testing."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        mode="full",
        monitor_group_tag="monitor_group",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.test_schema.test_table": MonitoredTableConfig(
                label_column="label",
                prediction_column="prediction",
                timestamp_column="timestamp",
            ),
        },
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            granularity="1 day",
            output_schema_name="monitoring_results",
            slicing_exprs=["region"],
            max_slicing_cardinality=50,
            problem_type="PROBLEM_TYPE_CLASSIFICATION",
            prediction_column="prediction",
            label_column="label",
            timestamp_column="timestamp",
            model_id_column="model_version",
        ),
        alerting=AlertConfig(
            enable_aggregated_alerts=True,
            drift_threshold=0.2,
            null_rate_threshold=0.1,
            row_count_min=1000,
            default_notifications=["test@example.com"],
        ),
        dry_run=False,
        cleanup_orphans=False,
        deploy_aggregated_dashboard=True,
        dashboard_parent_path="/Workspace/Shared/DPO",
    )


@pytest.fixture
def sample_bulk_config() -> OrchestratorConfig:
    """Create a sample bulk mode configuration for testing."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        mode="bulk_provision_only",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.ml.predictions": MonitoredTableConfig(),
        },
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring_results",
            prediction_column="prediction",
            timestamp_column="timestamp",
        ),
        alerting=AlertConfig(enable_aggregated_alerts=False),
        deploy_aggregated_dashboard=False,
    )


@pytest.fixture
def sample_config_with_discovery() -> OrchestratorConfig:
    """Create a sample configuration with tag discovery enabled."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        mode="full",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=True,
        discovery=DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            exclude_schemas=["information_schema", "tmp_*"],
        ),
        monitored_tables={},
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring_results",
            prediction_column="prediction",
            timestamp_column="timestamp",
        ),
        alerting=AlertConfig(enable_aggregated_alerts=True),
    )


@pytest.fixture
def sample_config_with_groups() -> OrchestratorConfig:
    """Create a sample configuration with per-group alert routing."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        mode="full",
        monitor_group_tag="monitor_group",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.ml.model_a": MonitoredTableConfig(),
            "test_catalog.ml.model_b": MonitoredTableConfig(),
        },
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring_results",
            prediction_column="prediction",
            timestamp_column="timestamp",
        ),
        alerting=AlertConfig(
            enable_aggregated_alerts=True,
            drift_threshold=0.2,
            default_notifications=["default@example.com"],
            group_notifications={
                "ml_team": ["ml-team@example.com", "ml-oncall@example.com"],
                "data_eng": ["data-eng@example.com"],
            },
        ),
        deploy_aggregated_dashboard=True,
    )


@pytest.fixture
def sample_config_with_include_schemas() -> OrchestratorConfig:
    """Create a sample configuration with include_schemas filtering."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        mode="full",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=True,
        discovery=DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            include_schemas=["ml_models", "data_warehouse_*"],
            exclude_schemas=["information_schema"],
        ),
        monitored_tables={},
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring_results",
            prediction_column="prediction",
            timestamp_column="timestamp",
        ),
    )


@pytest.fixture
def sample_timeseries_config() -> OrchestratorConfig:
    """Create a sample TIMESERIES configuration for testing."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.events.page_views": MonitoredTableConfig(
                timestamp_column="event_time",
            ),
        },
        profile_defaults=ProfileConfig(
            profile_type="TIMESERIES",
            granularity="1 day",
            output_schema_name="monitoring_results",
            timeseries_timestamp_column="event_time",
        ),
        alerting=AlertConfig(enable_aggregated_alerts=True, drift_threshold=0.2),
    )


@pytest.fixture
def sample_snapshot_config() -> OrchestratorConfig:
    """Create a sample SNAPSHOT configuration for testing."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.dimensions.products": MonitoredTableConfig(),
        },
        profile_defaults=ProfileConfig(
            profile_type="SNAPSHOT",
            granularity="1 day",
            output_schema_name="monitoring_results",
        ),
        alerting=AlertConfig(enable_aggregated_alerts=True, drift_threshold=0.2),
    )


@pytest.fixture
def sample_discovered_table() -> DiscoveredTable:
    """Create a sample discovered table for testing."""
    columns = [
        create_mock_column("prediction", "DOUBLE"),
        create_mock_column("label", "STRING"),
        create_mock_column("timestamp", "TIMESTAMP"),
        create_mock_column("region", "STRING"),
        create_mock_column("model_version", "STRING"),
    ]

    return DiscoveredTable(
        full_name="test_catalog.test_schema.test_table",
        tags={
            "monitor_enabled": "true",
            "owner": "data_team",
            "department": "analytics",
            "monitor_priority": "1",
        },
        columns=columns,
        has_primary_key=True,
        table_type="MANAGED",
        priority=1,
    )


@pytest.fixture
def sample_discovered_tables(sample_discovered_table) -> list:
    """Create a list of sample discovered tables."""
    columns = [create_mock_column("value", "DOUBLE")]
    table2 = DiscoveredTable(
        full_name="test_catalog.test_schema.second_table",
        tags={"monitor_enabled": "true", "owner": "ml_team"},
        columns=columns,
        has_primary_key=True,
        table_type="MANAGED",
        priority=2,
    )
    return [sample_discovered_table, table2]


@pytest.fixture
def sample_grouped_tables() -> list:
    """Create tables with different monitor_group tags."""
    columns = [create_mock_column("value", "DOUBLE")]

    return [
        DiscoveredTable(
            full_name="test_catalog.ml.model_a",
            tags={"monitor_enabled": "true", "monitor_group": "ml_team"},
            columns=columns,
        ),
        DiscoveredTable(
            full_name="test_catalog.ml.model_b",
            tags={"monitor_enabled": "true", "monitor_group": "ml_team"},
            columns=columns,
        ),
        DiscoveredTable(
            full_name="test_catalog.warehouse.sales",
            tags={"monitor_enabled": "true", "monitor_group": "data_eng"},
            columns=columns,
        ),
        DiscoveredTable(
            full_name="test_catalog.shared.reference",
            tags={"monitor_enabled": "true"},  # No monitor_group -> default
            columns=columns,
        ),
    ]


@pytest.fixture
def config_with_custom_metrics() -> OrchestratorConfig:
    """Create a configuration with custom metrics."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.ml.predictions": MonitoredTableConfig(),
        },
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            granularity="1 day",
            output_schema_name="monitoring_results",
            prediction_column="prediction",
            timestamp_column="timestamp",
            custom_metrics=[
                CustomMetricConfig(
                    name="revenue_sum",
                    metric_type="aggregate",
                    input_columns=["revenue"],
                    definition="SUM(revenue)",
                    output_type="double",
                ),
                CustomMetricConfig(
                    name="null_ratio",
                    metric_type="derived",
                    input_columns=["null_count", "count"],
                    definition="{{null_count}} / {{count}}",
                    output_type="double",
                ),
            ],
        ),
        alerting=AlertConfig(enable_aggregated_alerts=True, drift_threshold=0.2),
    )


@pytest.fixture
def config_with_per_table_baseline() -> OrchestratorConfig:
    """Create a configuration with per-table baseline tables."""
    return OrchestratorConfig(
        catalog_name="test_catalog",
        warehouse_id="test_warehouse_123",
        include_tagged_tables=False,
        monitored_tables={
            "test_catalog.ml.churn_model": MonitoredTableConfig(
                baseline_table_name="test_catalog.ml.churn_baseline",
                label_column="churned",
                prediction_column="churn_prob",
            ),
            "test_catalog.ml.fraud_model": MonitoredTableConfig(
                baseline_table_name="test_catalog.ml.fraud_baseline",
                label_column="is_fraud",
                prediction_column="fraud_score",
            ),
        },
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            granularity="1 day",
            output_schema_name="monitoring_results",
            prediction_column="prediction",
            timestamp_column="timestamp",
        ),
        alerting=AlertConfig(enable_aggregated_alerts=True, drift_threshold=0.2),
    )
