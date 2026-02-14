"""Tests for DPO CLI module."""

import json
from unittest.mock import MagicMock

import databricks.sdk as dbsdk
import pytest
from click.testing import CliRunner

import dpo as dpo_pkg
from dpo.cli import cli, validate_policy, validate_workspace
from dpo.config import (
    CustomMetricConfig,
    MonitoredTableConfig,
    ObjectiveFunctionConfig,
    OrchestratorConfig,
    PolicyConfig,
    ProfileConfig,
)
from dpo.coverage import CoverageReport, OrphanMonitor, StaleMonitor, UnmonitoredTable


def _build_test_config(policy: PolicyConfig | None = None) -> OrchestratorConfig:
    """Construct a minimal valid config for CLI command tests."""
    kwargs = {}
    if policy is not None:
        kwargs["policy"] = policy
    return OrchestratorConfig(
        catalog_name="prod",
        warehouse_id="test_warehouse",
        include_tagged_tables=False,
        monitored_tables={"prod.ml.predictions": MonitoredTableConfig()},
        profile_defaults=ProfileConfig(
            profile_type="INFERENCE",
            output_schema_name="monitoring",
            prediction_column="pred",
            timestamp_column="ts",
        ),
        **kwargs,
    )


@pytest.fixture
def runner():
    """Create a Click test runner."""
    return CliRunner()


@pytest.fixture
def valid_config_file(tmp_path):
    """Create a minimal valid config file."""
    content = """
catalog_name: "prod"
warehouse_id: "test_warehouse"
include_tagged_tables: false
monitored_tables:
  prod.ml.predictions:
    label_column: "label"
profile_defaults:
  profile_type: "INFERENCE"
  output_schema_name: "monitoring"
  prediction_column: "pred"
  timestamp_column: "ts"
alerting:
  drift_threshold: 0.2
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(content)
    return str(config_file)


@pytest.fixture
def invalid_config_file(tmp_path):
    """Create an invalid config file."""
    content = """
catalog_name: "prod"
warehouse_id: "test"
include_tagged_tables: false
monitored_tables: {}
profile_defaults:
  profile_type: "INFERENCE"
  output_schema_name: "monitoring"
  prediction_column: "pred"
  timestamp_column: "ts"
"""
    config_file = tmp_path / "bad_config.yaml"
    config_file.write_text(content)
    return str(config_file)


class TestValidateCommand:
    """Tests for `dpo validate`."""

    def test_validate_valid_config(self, runner, valid_config_file):
        """Valid config passes validation."""
        result = runner.invoke(cli, ["validate", valid_config_file])
        assert result.exit_code == 0
        assert "PASS" in result.output

    def test_validate_invalid_config(self, runner, invalid_config_file):
        """Invalid config fails with exit code 1."""
        result = runner.invoke(cli, ["validate", invalid_config_file])
        assert result.exit_code == 1

    def test_validate_missing_file(self, runner):
        """Missing file fails with exit code 1."""
        result = runner.invoke(cli, ["validate", "/nonexistent/path.yaml"])
        assert result.exit_code != 0

    def test_validate_json_output(self, runner, valid_config_file):
        """JSON output contains required keys."""
        result = runner.invoke(cli, ["validate", valid_config_file, "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["command"] == "validate"
        assert data["status"] == "pass"
        assert "checks" in data
        assert "errors" in data

    def test_validate_json_output_on_failure(self, runner, invalid_config_file):
        """JSON output on failure contains error details."""
        result = runner.invoke(cli, ["validate", invalid_config_file, "--format", "json"])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["status"] == "fail"
        assert len(data["errors"]) > 0

    def test_validate_with_workspace_checks(self, runner, valid_config_file, monkeypatch):
        """Workspace issues should fail validation when --check-workspace is enabled."""
        config = _build_test_config()
        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(
            "dpo.cli.validate_workspace",
            lambda _cfg: [{"check": "warehouse", "message": "missing"}],
        )

        result = runner.invoke(
            cli, ["validate", valid_config_file, "--check-workspace", "--format", "json"]
        )
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["status"] == "fail"
        assert any(c["check"] == "workspace" for c in data["checks"])

    def test_validate_with_workspace_checks_reports_uc_function_issues(
        self, runner, valid_config_file, monkeypatch
    ):
        """UC function issues should fail validate when workspace checks are enabled."""
        config = _build_test_config()
        config.objective_functions = {
            "obj1": ObjectiveFunctionConfig(
                uc_function_name="cat.sch.missing_func",
                metric=CustomMetricConfig(
                    name="metric_a",
                    metric_type="aggregate",
                    input_columns=["a"],
                    definition="SUM(a)",
                ),
            )
        }
        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr("dpo.cli.validate_workspace", lambda _cfg: [])
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: MagicMock())
        monkeypatch.setattr(
            "dpo.validators.validate_uc_functions",
            lambda _cfg, _w: [{"check": "uc_function", "message": "not found"}],
        )

        result = runner.invoke(
            cli, ["validate", valid_config_file, "--check-workspace", "--format", "json"]
        )
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert any(
            c["check"] == "uc_functions" and c["status"] == "fail"
            for c in data["checks"]
        )

    def test_validate_with_workspace_checks_passes_uc_function_validation(
        self, runner, valid_config_file, monkeypatch
    ):
        """UC function check should pass when validator returns no issues."""
        config = _build_test_config()
        config.objective_functions = {
            "obj1": ObjectiveFunctionConfig(
                uc_function_name="cat.sch.func",
                metric=CustomMetricConfig(
                    name="metric_a",
                    metric_type="aggregate",
                    input_columns=["a"],
                    definition="SUM(a)",
                ),
            )
        }
        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr("dpo.cli.validate_workspace", lambda _cfg: [])
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: MagicMock())
        monkeypatch.setattr(
            "dpo.validators.validate_uc_functions",
            lambda _cfg, _w: [],
        )

        result = runner.invoke(
            cli, ["validate", valid_config_file, "--check-workspace", "--format", "json"]
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert any(
            c["check"] == "uc_functions" and c["status"] == "pass"
            for c in data["checks"]
        )

    def test_validate_handles_schema_file_error(self, runner, valid_config_file, monkeypatch):
        """Schema load errors from loader should map to validation failure."""
        monkeypatch.setattr(
            "dpo.cli.load_config",
            lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing")),
        )
        result = runner.invoke(cli, ["validate", valid_config_file, "--format", "json"])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["status"] == "fail"
        assert data["errors"][0]["check"] == "schema"

    def test_validate_policy_failure_in_command(self, runner, valid_config_file, monkeypatch):
        """Policy violations should fail command-level validation."""
        config = _build_test_config(
            PolicyConfig(naming_patterns=[r"^prod\.other\..*"])
        )
        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)

        result = runner.invoke(cli, ["validate", valid_config_file, "--format", "json"])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert any(c["check"] == "policy" and c["status"] == "fail" for c in data["checks"])


class TestRunCommand:
    """Tests for `dpo run`."""

    def test_run_without_confirm_exits_2(self, runner, valid_config_file):
        """Run without --confirm fails with exit code 2 (safety)."""
        result = runner.invoke(cli, ["run", valid_config_file])
        assert result.exit_code == 2
        assert "--confirm" in result.output

    def test_run_json_success(self, runner, valid_config_file, monkeypatch):
        """Run with --confirm returns JSON report and exit code 0 on success."""
        config = _build_test_config()
        report = MagicMock()
        report.tables_discovered = 1
        report.monitors_created = 1
        report.monitors_updated = 0
        report.monitors_skipped = 0
        report.monitors_failed = 0
        report.orphans_cleaned = 0
        report.unified_drift_views = {}
        report.drift_alert_ids = {}
        report.dashboard_ids = {}
        report.to_dict.return_value = {"monitors_failed": 0}

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", lambda _cfg: report)

        result = runner.invoke(
            cli, ["run", valid_config_file, "--confirm", "--format", "json"]
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["command"] == "run"
        assert data["status"] == "success"
        assert data["confirmed"] is True
        assert config.dry_run is False

    def test_run_returns_runtime_exit_code_when_monitors_fail(
        self, runner, valid_config_file, monkeypatch
    ):
        """Run should exit with runtime code when monitor failures are reported."""
        config = _build_test_config()
        report = MagicMock()
        report.tables_discovered = 1
        report.monitors_created = 0
        report.monitors_updated = 0
        report.monitors_skipped = 0
        report.monitors_failed = 1
        report.orphans_cleaned = 0
        report.unified_drift_views = {}
        report.drift_alert_ids = {}
        report.dashboard_ids = {}

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", lambda _cfg: report)

        result = runner.invoke(cli, ["run", valid_config_file, "--confirm"])
        assert result.exit_code == 3
        assert "Monitors failed:   1" in result.output

    def test_run_handles_runtime_error(self, runner, valid_config_file, monkeypatch):
        """Unhandled runtime errors should return runtime exit code."""
        config = _build_test_config()

        def _raise_runtime(_cfg):
            raise RuntimeError("boom")

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", _raise_runtime)

        result = runner.invoke(cli, ["run", valid_config_file, "--confirm"])
        assert result.exit_code == 3
        assert "Runtime error: boom" in result.output

    def test_run_handles_permission_error(self, runner, valid_config_file, monkeypatch):
        """Permission errors should return runtime exit code with clear message."""
        config = _build_test_config()

        def _raise_permission(_cfg):
            raise PermissionError("denied")

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", _raise_permission)

        result = runner.invoke(cli, ["run", valid_config_file, "--confirm"])
        assert result.exit_code == 3
        assert "Permission error: denied" in result.output

    def test_run_handles_config_error(self, runner, valid_config_file, monkeypatch):
        """Config load errors should map to validation exit code."""
        monkeypatch.setattr(
            "dpo.cli.load_config",
            lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing")),
        )

        result = runner.invoke(cli, ["run", valid_config_file, "--confirm"])
        assert result.exit_code == 1
        assert "Config error: missing" in result.output

    def test_run_table_output_lists_artifacts(
        self, runner, valid_config_file, monkeypatch
    ):
        """Table output should include views, alerts, and dashboards when present."""
        config = _build_test_config()
        report = MagicMock()
        report.tables_discovered = 1
        report.monitors_created = 1
        report.monitors_updated = 0
        report.monitors_skipped = 0
        report.monitors_failed = 0
        report.orphans_cleaned = 0
        report.unified_drift_views = {"default": "prod.global.unified_drift_metrics_default"}
        report.drift_alert_ids = {"default": "alert-1"}
        report.dashboard_ids = {"default": "dash-1"}

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", lambda _cfg: report)

        result = runner.invoke(cli, ["run", valid_config_file, "--confirm"])
        assert result.exit_code == 0
        assert "Unified views:" in result.output
        assert "Alerts:" in result.output
        assert "Dashboards:" in result.output


class TestDryRunCommand:
    """Tests for `dpo dry-run`."""

    def test_dry_run_invalid_config(self, runner, invalid_config_file):
        """Dry-run with invalid config fails with exit code 1."""
        result = runner.invoke(cli, ["dry-run", invalid_config_file])
        assert result.exit_code == 1

    def test_dry_run_json_success(self, runner, valid_config_file, monkeypatch):
        """Dry-run should force dry_run mode and output summary JSON."""
        config = _build_test_config()
        impact = MagicMock()
        impact.to_dict.return_value = {"planned_actions": [{"table": "prod.ml.predictions"}]}
        report = MagicMock()
        report.tables_discovered = 1
        report.monitors_created = 1
        report.monitors_skipped = 0
        report.impact_report = impact

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", lambda _cfg: report)

        result = runner.invoke(
            cli, ["dry-run", valid_config_file, "--format", "json"]
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["command"] == "dry-run"
        assert data["status"] == "success"
        assert data["summary"]["tables_discovered"] == 1
        assert config.dry_run is True

    @pytest.mark.parametrize(
        "exc_type, expected",
        [
            (PermissionError, "Permission error"),
            (RuntimeError, "Runtime error"),
        ],
    )
    def test_dry_run_runtime_failures(
        self, runner, valid_config_file, monkeypatch, exc_type, expected
    ):
        """Dry-run maps runtime exceptions to runtime exit code."""
        config = _build_test_config()

        def _raise(_cfg):
            raise exc_type("failure")

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dpo_pkg, "run_orchestration", _raise)

        result = runner.invoke(cli, ["dry-run", valid_config_file])
        assert result.exit_code == 3
        assert expected in result.output


class TestPolicyValidation:
    """Tests for policy validation logic."""

    def _make_config(self, policy: PolicyConfig, tables=None) -> OrchestratorConfig:
        """Helper to make a config with policy."""
        return OrchestratorConfig(
            catalog_name="prod",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables=tables or {"prod.ml.predictions": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="INFERENCE",
                output_schema_name="monitoring",
                prediction_column="pred",
                timestamp_column="ts",
            ),
            policy=policy,
        )

    def test_no_policy_returns_empty(self):
        """No policy = no violations."""
        config = OrchestratorConfig(
            catalog_name="prod",
            warehouse_id="test",
            include_tagged_tables=False,
            monitored_tables={"prod.ml.predictions": MonitoredTableConfig()},
            profile_defaults=ProfileConfig(
                profile_type="INFERENCE",
                output_schema_name="monitoring",
                prediction_column="pred",
                timestamp_column="ts",
            ),
        )
        assert validate_policy(config) == []

    def test_naming_pattern_violation(self):
        """Tables not matching naming pattern produce violations."""
        config = self._make_config(
            PolicyConfig(naming_patterns=[r"^prod\.ml\..*"]),
            tables={"prod.data.some_table": MonitoredTableConfig()},
        )
        violations = validate_policy(config)
        assert len(violations) == 1
        assert violations[0]["check"] == "naming_pattern"

    def test_forbidden_pattern_violation(self):
        """Tables matching forbidden patterns produce violations."""
        config = self._make_config(
            PolicyConfig(forbidden_patterns=[r".*_tmp$"]),
            tables={"prod.ml.data_tmp": MonitoredTableConfig()},
        )
        violations = validate_policy(config)
        assert len(violations) == 1
        assert violations[0]["check"] == "forbidden_pattern"

    def test_require_baseline_violation(self):
        """Missing baseline when required produces violation."""
        config = self._make_config(PolicyConfig(require_baseline=True))
        violations = validate_policy(config)
        assert any(v["check"] == "require_baseline" for v in violations)

    def test_require_baseline_satisfied(self):
        """Baseline present when required passes."""
        config = self._make_config(
            PolicyConfig(require_baseline=True),
            tables={
                "prod.ml.predictions": MonitoredTableConfig(
                    baseline_table_name="prod.ml.baseline"
                )
            },
        )
        violations = validate_policy(config)
        assert not any(v["check"] == "require_baseline" for v in violations)

    def test_max_tables_violation(self):
        """Exceeding max tables produces violation."""
        tables = {f"prod.ml.table_{i}": MonitoredTableConfig() for i in range(5)}
        config = self._make_config(PolicyConfig(max_tables_per_config=3), tables=tables)
        violations = validate_policy(config)
        assert any(v["check"] == "max_tables_per_config" for v in violations)

    def test_require_slicing_violation(self):
        """Missing slicing should violate require_slicing policy."""
        config = self._make_config(PolicyConfig(require_slicing=True))
        violations = validate_policy(config)
        assert any(v["check"] == "require_slicing" for v in violations)

    def test_require_slicing_satisfied_by_default(self):
        """Default slicing_exprs should satisfy require_slicing policy."""
        config = self._make_config(PolicyConfig(require_slicing=True))
        config.profile_defaults.slicing_exprs = ["region"]
        violations = validate_policy(config)
        assert not any(v["check"] == "require_slicing" for v in violations)

    def test_workspace_required_tags_violation(self, monkeypatch):
        """Missing required tags should fail workspace validation."""
        config = self._make_config(
            PolicyConfig(required_tags=["owner", "department"]),
            tables={"prod.ml.predictions": MonitoredTableConfig()},
        )

        w = MagicMock()
        wh = MagicMock()
        wh.id = "test"
        w.warehouses.list.return_value = [wh]
        w.tables.get.return_value = MagicMock()

        owner_tag = MagicMock()
        owner_tag.tag_key = "owner"
        owner_tag.tag_value = "ml-team"
        w.entity_tag_assignments.list.return_value = [owner_tag]

        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)

        issues = validate_workspace(config)
        assert any(
            i["check"] == "required_tags" and "department" in i["message"]
            for i in issues
        )

    def test_workspace_required_tags_satisfied_by_config_override(self, monkeypatch):
        """Required tags can be satisfied by per-table config overrides."""
        config = self._make_config(
            PolicyConfig(required_tags=["owner"]),
            tables={
                "prod.ml.predictions": MonitoredTableConfig(owner="owner@example.com")
            },
        )

        w = MagicMock()
        wh = MagicMock()
        wh.id = "test"
        w.warehouses.list.return_value = [wh]
        w.tables.get.return_value = MagicMock()
        w.entity_tag_assignments.list.return_value = []

        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)

        issues = validate_workspace(config)
        assert not any(i["check"] == "required_tags" for i in issues)

    def test_workspace_missing_warehouse(self, monkeypatch):
        """Unknown warehouse ID should be reported."""
        config = self._make_config(PolicyConfig())
        config.warehouse_id = "missing"

        w = MagicMock()
        wh = MagicMock()
        wh.id = "different"
        w.warehouses.list.return_value = [wh]
        w.tables.get.return_value = MagicMock()
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)

        issues = validate_workspace(config)
        assert any(i["check"] == "warehouse" for i in issues)

    def test_workspace_tag_api_failure(self, monkeypatch):
        """Tag API errors should produce required_tags issue entries."""
        config = self._make_config(
            PolicyConfig(required_tags=["owner"]),
            tables={"prod.ml.predictions": MonitoredTableConfig()},
        )

        w = MagicMock()
        wh = MagicMock()
        wh.id = "test"
        w.warehouses.list.return_value = [wh]
        w.tables.get.return_value = MagicMock()
        w.entity_tag_assignments.list.side_effect = RuntimeError("tag api down")
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)

        issues = validate_workspace(config)
        assert any(
            i["check"] == "required_tags" and "tag API error" in i["message"]
            for i in issues
        )

    def test_workspace_connectivity_failure(self, monkeypatch):
        """Client construction failures should map to connectivity issue."""
        config = self._make_config(PolicyConfig())

        def _raise():
            raise RuntimeError("cannot connect")

        monkeypatch.setattr(dbsdk, "WorkspaceClient", _raise)

        issues = validate_workspace(config)
        assert any(i["check"] == "connectivity" for i in issues)


class TestCoverageCommand:
    """Tests for `dpo coverage`."""

    def test_coverage_json_success(self, runner, valid_config_file, monkeypatch):
        """Coverage command should emit JSON report payload."""
        config = _build_test_config()
        report = CoverageReport(
            total_catalog_tables=3,
            total_monitored=2,
            coverage_pct=66.7,
            unmonitored=[
                UnmonitoredTable(full_name="prod.ml.unmonitored", schema_name="ml")
            ],
            stale=[
                StaleMonitor(
                    table_name="prod.ml.predictions",
                    monitor_id="m1",
                    days_since_refresh=35,
                    status="ACTIVE",
                )
            ],
            orphans=[
                OrphanMonitor(table_name="prod.ml.orphan", monitor_id="m2")
            ],
        )
        analyzer = MagicMock()
        analyzer.analyze.return_value = report
        w = MagicMock()

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)
        monkeypatch.setattr(dpo_pkg, "_build_table_list", lambda _w, _cfg: [])
        monkeypatch.setattr(dpo_pkg, "CoverageAnalyzer", lambda _w, _cfg: analyzer)

        result = runner.invoke(
            cli, ["coverage", valid_config_file, "--format", "json"]
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["command"] == "coverage"
        assert data["status"] == "success"
        assert data["summary"]["total_catalog_tables"] == 3
        assert data["summary"]["unmonitored_count"] == 1

    def test_coverage_table_output(self, runner, valid_config_file, monkeypatch):
        """Table output should include summary section and sample rows."""
        config = _build_test_config()
        report = CoverageReport(
            total_catalog_tables=1,
            total_monitored=0,
            coverage_pct=0.0,
            unmonitored=[
                UnmonitoredTable(full_name="prod.ml.predictions", schema_name="ml")
            ],
        )
        analyzer = MagicMock()
        analyzer.analyze.return_value = report
        w = MagicMock()

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)
        monkeypatch.setattr(dpo_pkg, "_build_table_list", lambda _w, _cfg: [])
        monkeypatch.setattr(dpo_pkg, "CoverageAnalyzer", lambda _w, _cfg: analyzer)

        result = runner.invoke(cli, ["coverage", valid_config_file])
        assert result.exit_code == 0
        assert "DPO COVERAGE GOVERNANCE REPORT" in result.output
        assert "UNMONITORED TABLES" in result.output

    def test_coverage_table_output_includes_stale_and_orphans(
        self, runner, valid_config_file, monkeypatch
    ):
        """Table output should render stale and orphan sections when present."""
        config = _build_test_config()
        report = CoverageReport(
            total_catalog_tables=2,
            total_monitored=1,
            coverage_pct=50.0,
            stale=[
                StaleMonitor(
                    table_name="prod.ml.predictions",
                    monitor_id="m1",
                    days_since_refresh=40,
                    status="ACTIVE",
                )
            ],
            orphans=[
                OrphanMonitor(table_name="prod.ml.orphan", monitor_id="m2")
            ],
        )
        analyzer = MagicMock()
        analyzer.analyze.return_value = report
        w = MagicMock()

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)
        monkeypatch.setattr(dpo_pkg, "_build_table_list", lambda _w, _cfg: [])
        monkeypatch.setattr(dpo_pkg, "CoverageAnalyzer", lambda _w, _cfg: analyzer)

        result = runner.invoke(cli, ["coverage", valid_config_file])
        assert result.exit_code == 0
        assert "STALE MONITORS:" in result.output
        assert "ORPHAN MONITORS (not in config):" in result.output

    def test_coverage_runtime_error(self, runner, valid_config_file, monkeypatch):
        """Coverage runtime exceptions should return runtime exit code."""
        config = _build_test_config()
        w = MagicMock()
        analyzer = MagicMock()
        analyzer.analyze.side_effect = RuntimeError("broken")

        monkeypatch.setattr("dpo.cli.load_config", lambda _path: config)
        monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda: w)
        monkeypatch.setattr(dpo_pkg, "_build_table_list", lambda _w, _cfg: [])
        monkeypatch.setattr(dpo_pkg, "CoverageAnalyzer", lambda _w, _cfg: analyzer)

        result = runner.invoke(cli, ["coverage", valid_config_file])
        assert result.exit_code == 3
        assert "Runtime error: broken" in result.output

    def test_coverage_config_error(self, runner, valid_config_file, monkeypatch):
        """Coverage command should fail fast on config loading errors."""
        monkeypatch.setattr(
            "dpo.cli.load_config",
            lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing")),
        )
        result = runner.invoke(cli, ["coverage", valid_config_file])
        assert result.exit_code == 1
        assert "Config error: missing" in result.output
