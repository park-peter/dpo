"""Tests for DPO validators module."""

from unittest.mock import MagicMock

import pytest

from dpo.config import (
    CustomMetricConfig,
    MonitoredTableConfig,
    ObjectiveFunctionConfig,
    OrchestratorConfig,
    ProfileConfig,
)
from dpo.validators import run_preflight_checks, validate_uc_functions


class TestValidateUcFunctions:
    """Tests for validate_uc_functions()."""

    def test_missing_function_returns_issue(self):
        """Missing UC function should return an issue."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
            objective_functions={
                "obj1": ObjectiveFunctionConfig(
                    uc_function_name="cat.sch.missing_func",
                    metric=CustomMetricConfig(
                        name="m", metric_type="aggregate",
                        input_columns=["a"], definition="SUM(a)",
                    ),
                ),
            },
        )

        w = MagicMock()
        w.functions.get.side_effect = Exception("Not found")

        issues = validate_uc_functions(config, w)
        assert len(issues) == 1
        assert issues[0]["objective"] == "obj1"

    def test_existing_function_passes(self):
        """Existing UC function should pass."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
            objective_functions={
                "obj1": ObjectiveFunctionConfig(
                    uc_function_name="cat.sch.func",
                    metric=CustomMetricConfig(
                        name="m", metric_type="aggregate",
                        input_columns=["a"], definition="SUM(a)",
                    ),
                ),
            },
        )

        w = MagicMock()
        w.functions.get.return_value = MagicMock()

        issues = validate_uc_functions(config, w)
        assert len(issues) == 0

    def test_empty_objectives_no_api_calls(self):
        """Empty objectives should make zero API calls."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
        )

        w = MagicMock()
        issues = validate_uc_functions(config, w)
        assert len(issues) == 0
        w.functions.get.assert_not_called()

    def test_deduplicates_uc_functions(self):
        """Same UC function referenced by multiple objectives should only be checked once."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
            objective_functions={
                "obj1": ObjectiveFunctionConfig(
                    uc_function_name="cat.sch.shared_func",
                    metric=CustomMetricConfig(
                        name="m1", metric_type="aggregate",
                        input_columns=["a"], definition="SUM(a)",
                    ),
                ),
                "obj2": ObjectiveFunctionConfig(
                    uc_function_name="cat.sch.shared_func",
                    metric=CustomMetricConfig(
                        name="m2", metric_type="aggregate",
                        input_columns=["b"], definition="AVG(b)",
                    ),
                ),
            },
        )

        w = MagicMock()
        w.functions.get.return_value = MagicMock()

        validate_uc_functions(config, w)
        assert w.functions.get.call_count == 1


class TestRunPreflightChecks:
    """Tests for run_preflight_checks()."""

    def test_empty_objectives_is_noop(self):
        """run_preflight_checks with empty objectives should be a no-op."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
        )

        w = MagicMock()
        run_preflight_checks(config, w)
        w.functions.get.assert_not_called()

    def test_raises_on_missing_uc_function(self):
        """run_preflight_checks should raise ValueError on missing UC function."""
        config = OrchestratorConfig(
            catalog_name="test",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"test.sch.tbl": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="SNAPSHOT",
                output_schema_name="monitoring",
            ),
            objective_functions={
                "obj1": ObjectiveFunctionConfig(
                    uc_function_name="cat.sch.missing",
                    metric=CustomMetricConfig(
                        name="m", metric_type="aggregate",
                        input_columns=["a"], definition="SUM(a)",
                    ),
                ),
            },
        )

        w = MagicMock()
        w.functions.get.side_effect = Exception("Not found")

        with pytest.raises(ValueError, match="UC function validation failed"):
            run_preflight_checks(config, w)
