"""Tests for execution planning and artifact naming."""

import pytest

from dpo.config import MonitoredTableConfig
from dpo.discovery import DiscoveredTable


def _table(full_name: str, group: str | None = None) -> DiscoveredTable:
    tags = {"monitor_enabled": "true"}
    if group is not None:
        tags["monitor_group"] = group
    return DiscoveredTable(
        full_name=full_name,
        tags=tags,
        columns=[],
        table_type="MANAGED",
    )


class TestExecutionPlan:
    """Tests for the canonical execution plan."""

    def test_rejects_duplicate_leaf_table_names(self, sample_config):
        """Two tables with the same final segment should fail plan construction."""
        sample_config.monitored_tables = {
            "test_catalog.sales.events": MonitoredTableConfig(),
            "test_catalog.marketing.events": MonitoredTableConfig(),
        }

        from dpo.planning import PlanningError, build_execution_plan

        tables = [
            _table("test_catalog.sales.events"),
            _table("test_catalog.marketing.events"),
        ]

        with pytest.raises(
            PlanningError,
            match="Duplicate table leaf names are not allowed",
        ):
            build_execution_plan(sample_config, tables)

    def test_rejects_group_slug_collisions(self, sample_config):
        """Different raw group names that sanitize identically should fail fast."""
        from dpo.planning import PlanningError, build_execution_plan

        tables = [
            _table("test_catalog.ml.model_a", group="ML Team"),
            _table("test_catalog.ml.model_b", group="ML-Team"),
        ]

        with pytest.raises(
            PlanningError,
            match="Duplicate sanitized monitor group names are not allowed",
        ):
            build_execution_plan(sample_config, tables)

    def test_builds_group_scoped_artifacts(self, sample_config):
        """Each group should receive drift, profile, and performance artifact names."""
        from dpo.planning import build_execution_plan

        tables = [
            _table("test_catalog.ml.model_a", group="ML Team"),
            _table("test_catalog.shared.reference"),
        ]

        plan = build_execution_plan(sample_config, tables)

        assert set(plan.groups) == {"ML Team", "default"}
        assert plan.groups["ML Team"].artifacts.drift_view.endswith("_ml_team")
        assert plan.groups["ML Team"].artifacts.profile_view.endswith("_ml_team")
        assert plan.groups["ML Team"].artifacts.performance_view.endswith("_ml_team")
        assert plan.groups["default"].artifacts.performance_view.endswith("_default")

    def test_uses_full_table_name_for_assets_slug(self, sample_config):
        """Workspace assets should derive from the full table name, not just the leaf name."""
        from dpo.planning import build_execution_plan

        table = _table("test_catalog.sales.events")

        plan = build_execution_plan(sample_config, [table])

        assert (
            plan.tables_by_name["test_catalog.sales.events"].assets_suffix
            == "test_catalog_sales_events"
        )
