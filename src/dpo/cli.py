"""
DPO CLI - Operator-friendly command interface for Data Profiling Orchestrator.

Commands:
    dpo validate <config_path>   Offline schema/policy validation
    dpo dry-run <config_path>    Safe preview without mutations
    dpo run <config_path>        Apply monitor orchestration (requires --confirm)
    dpo coverage <config_path>   Coverage governance report

Exit codes:
    0: Success
    1: Validation/policy failure
    2: CLI usage/safety error (including missing --confirm)
    3: Runtime/connectivity/auth failure

CLI UX contract is provisional for v0.2. Command semantics and safety
behaviour may be tuned based on operator feedback before freeze.
"""

import json
import logging
import re
import sys

import click
from pydantic import ValidationError
from tabulate import tabulate

from dpo.config import OrchestratorConfig, load_config

logger = logging.getLogger("dpo")

# Exit codes
EXIT_SUCCESS = 0
EXIT_VALIDATION = 1
EXIT_USAGE = 2
EXIT_RUNTIME = 3


def _setup_logging(verbose: bool) -> None:
    """Configure logging based on verbosity flag."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )


# ---------------------------------------------------------------------------
# Policy validation logic
# ---------------------------------------------------------------------------

def validate_policy(config: OrchestratorConfig) -> list[dict]:
    """Run policy checks on the config. Returns list of violation dicts."""
    violations: list[dict] = []
    policy = config.policy
    if policy is None:
        return violations

    # Naming patterns
    for pattern in policy.naming_patterns:
        regex = re.compile(pattern)
        for table_name in config.monitored_tables:
            if not regex.match(table_name):
                violations.append({
                    "check": "naming_pattern",
                    "table": table_name,
                    "message": f"Table name does not match required pattern '{pattern}'",
                })

    # Forbidden patterns
    for pattern in policy.forbidden_patterns:
        regex = re.compile(pattern)
        for table_name in config.monitored_tables:
            if regex.match(table_name):
                violations.append({
                    "check": "forbidden_pattern",
                    "table": table_name,
                    "message": f"Table name matches forbidden pattern '{pattern}'",
                })

    # Required baseline (inference only)
    if policy.require_baseline and config.profile_defaults.profile_type == "INFERENCE":
        for table_name, table_cfg in config.monitored_tables.items():
            if not table_cfg.baseline_table_name:
                violations.append({
                    "check": "require_baseline",
                    "table": table_name,
                    "message": "Baseline table required by policy but not configured",
                })

    # Required slicing
    if policy.require_slicing:
        for table_name, table_cfg in config.monitored_tables.items():
            default_slicing = config.profile_defaults.slicing_exprs
            table_slicing = table_cfg.slicing_exprs
            if not table_slicing and not default_slicing:
                violations.append({
                    "check": "require_slicing",
                    "table": table_name,
                    "message": "At least one slicing_expr required by policy",
                })

    # Max tables
    if policy.max_tables_per_config is not None:
        count = len(config.monitored_tables)
        if count > policy.max_tables_per_config:
            violations.append({
                "check": "max_tables_per_config",
                "table": "",
                "message": f"Config has {count} tables, max allowed is {policy.max_tables_per_config}",
            })

    return violations


def validate_workspace(config: OrchestratorConfig) -> list[dict]:
    """Run online workspace validation checks. Returns list of issue dicts."""
    issues: list[dict] = []
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        # Check warehouse exists and is accessible
        try:
            warehouses = {wh.id: wh for wh in w.warehouses.list()}
            if config.warehouse_id not in warehouses:
                issues.append({"check": "warehouse", "message": f"Warehouse '{config.warehouse_id}' not found"})
        except Exception as e:
            issues.append({"check": "warehouse", "message": f"Cannot list warehouses: {e}"})

        policy_required_tags = (
            list(config.policy.required_tags)
            if config.policy and config.policy.required_tags
            else []
        )

        # Check tables exist and (optionally) required tags
        for table_name, table_cfg in config.monitored_tables.items():
            table_accessible = False
            try:
                w.tables.get(full_name=table_name)
                table_accessible = True
            except Exception:
                issues.append({"check": "table_exists", "table": table_name, "message": f"Table '{table_name}' not accessible"})

            if not table_accessible or not policy_required_tags:
                continue

            # Tag requirements can be satisfied either by UC tags
            # or by explicit per-table config overrides when names match.
            uc_tags = {}
            try:
                tag_assignments = w.entity_tag_assignments.list(
                    entity_type="TABLE", entity_name=table_name
                )
                uc_tags = {tag.tag_key: tag.tag_value for tag in tag_assignments}
            except Exception as e:
                issues.append({
                    "check": "required_tags",
                    "table": table_name,
                    "message": f"Could not validate required tags (tag API error): {e}",
                })
                continue

            for required_tag in policy_required_tags:
                override_value = getattr(table_cfg, required_tag, None)
                has_override = (
                    override_value is not None and str(override_value).strip() != ""
                )
                has_uc_tag = required_tag in uc_tags and str(uc_tags[required_tag]).strip() != ""
                if not has_override and not has_uc_tag:
                    issues.append({
                        "check": "required_tags",
                        "table": table_name,
                        "message": (
                            f"Required tag '{required_tag}' missing "
                            "(not found in UC tags or per-table config override)"
                        ),
                    })

    except Exception as e:
        issues.append({"check": "connectivity", "message": f"Cannot connect to workspace: {e}"})

    return issues


# ---------------------------------------------------------------------------
# Click CLI
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(package_name="databricks-dpo")
@click.option("--verbose", is_flag=True, help="Enable diagnostic logging")
@click.pass_context
def cli(ctx, verbose):
    """DPO - Data Profiling Orchestrator CLI."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    _setup_logging(verbose)


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--check-workspace", is_flag=True, help="Run online workspace checks (tables, warehouse)")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table", help="Output format")
@click.pass_context
def validate(ctx, config_path, check_workspace, output_format):
    """Validate config schema, policy rules, and optionally workspace connectivity."""
    errors: list[dict] = []
    checks: list[dict] = []

    # 1. Schema validation
    try:
        config = load_config(config_path)
        checks.append({"check": "schema", "status": "pass"})
    except FileNotFoundError as e:
        errors.append({"check": "schema", "message": str(e)})
        _output_validate(config_path, checks, errors, output_format, "fail")
        sys.exit(EXIT_VALIDATION)
    except ValidationError as e:
        for err in e.errors():
            errors.append({"check": "schema", "field": ".".join(str(x) for x in err["loc"]), "message": err["msg"]})
        _output_validate(config_path, checks, errors, output_format, "fail")
        sys.exit(EXIT_VALIDATION)

    # 2. Policy validation
    policy_violations = validate_policy(config)
    if policy_violations:
        checks.append({"check": "policy", "status": "fail", "violations": len(policy_violations)})
        errors.extend(policy_violations)
    else:
        checks.append({"check": "policy", "status": "pass"})

    # 3. Online workspace checks
    if check_workspace:
        workspace_issues = validate_workspace(config)
        if workspace_issues:
            checks.append({"check": "workspace", "status": "fail", "issues": len(workspace_issues)})
            errors.extend(workspace_issues)
        else:
            checks.append({"check": "workspace", "status": "pass"})

        # UC function validation for objective functions
        if config.objective_functions:
            from dpo.validators import validate_uc_functions
            from databricks.sdk import WorkspaceClient
            try:
                w = WorkspaceClient()
                uc_issues = validate_uc_functions(config, w)
                if uc_issues:
                    checks.append({"check": "uc_functions", "status": "fail", "issues": len(uc_issues)})
                    errors.extend(uc_issues)
                else:
                    checks.append({"check": "uc_functions", "status": "pass"})
            except Exception as e:
                errors.append({"check": "uc_functions", "message": f"UC function validation failed: {e}"})

    status = "fail" if errors else "pass"
    _output_validate(config_path, checks, errors, output_format, status)
    sys.exit(EXIT_VALIDATION if errors else EXIT_SUCCESS)


def _output_validate(config_path, checks, errors, output_format, status):
    """Output validate results in chosen format."""
    if output_format == "json":
        click.echo(json.dumps({
            "command": "validate",
            "status": status,
            "config_path": config_path,
            "checks": checks,
            "errors": errors,
        }, indent=2, default=str))
    else:
        click.echo(f"\nDPO Validate: {config_path}")
        click.echo("=" * 60)
        for c in checks:
            icon = "PASS" if c.get("status") == "pass" else "FAIL"
            click.echo(f"  [{icon}] {c['check']}")
        if errors:
            click.echo(f"\nErrors ({len(errors)}):")
            for e in errors:
                click.echo(f"  - [{e.get('check', 'unknown')}] {e.get('message', '')}")
        else:
            click.echo("\nAll checks passed.")


@cli.command("dry-run")
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table", help="Output format")
@click.pass_context
def dry_run(ctx, config_path, output_format):
    """Preview what DPO would do without making any changes."""
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValidationError) as e:
        click.echo(f"Config error: {e}", err=True)
        sys.exit(EXIT_VALIDATION)

    # Force dry_run mode
    config.dry_run = True

    try:
        from dpo import run_orchestration
        report = run_orchestration(config)
    except PermissionError as e:
        click.echo(f"Permission error: {e}", err=True)
        sys.exit(EXIT_RUNTIME)
    except Exception as e:
        click.echo(f"Runtime error: {e}", err=True)
        sys.exit(EXIT_RUNTIME)

    if output_format == "json":
        result = {
            "command": "dry-run",
            "status": "success",
            "config_path": config_path,
            "summary": {
                "tables_discovered": report.tables_discovered,
                "monitors_created": report.monitors_created,
                "monitors_skipped": report.monitors_skipped,
            },
            "actions": report.impact_report.to_dict() if report.impact_report else {},
        }
        click.echo(json.dumps(result, indent=2, default=str))
    # Table output is handled by ImpactReport.print_summary() during execution
    sys.exit(EXIT_SUCCESS)


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--confirm", is_flag=True, help="Required flag to execute changes")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table", help="Output format")
@click.pass_context
def run(ctx, config_path, confirm, output_format):
    """Execute monitor orchestration. Requires --confirm to apply changes."""
    if not confirm:
        click.echo(
            "Safety: --confirm flag required to execute changes.\n"
            "This command will create/update/delete monitors in your workspace.\n"
            "Run 'dpo dry-run' first to preview, then:\n\n"
            f"  dpo run {config_path} --confirm\n",
            err=True,
        )
        sys.exit(EXIT_USAGE)

    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValidationError) as e:
        click.echo(f"Config error: {e}", err=True)
        sys.exit(EXIT_VALIDATION)

    # Force live execution
    config.dry_run = False

    try:
        from dpo import run_orchestration
        report = run_orchestration(config)
    except PermissionError as e:
        click.echo(f"Permission error: {e}", err=True)
        sys.exit(EXIT_RUNTIME)
    except Exception as e:
        click.echo(f"Runtime error: {e}", err=True)
        sys.exit(EXIT_RUNTIME)

    if output_format == "json":
        result = {
            "command": "run",
            "status": "success",
            "config_path": config_path,
            "confirmed": True,
            "report": report.to_dict(),
        }
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        click.echo("\n" + "=" * 60)
        click.echo("DPO RUN - EXECUTION REPORT")
        click.echo("=" * 60)
        click.echo(f"Tables discovered: {report.tables_discovered}")
        click.echo(f"Monitors created:  {report.monitors_created}")
        click.echo(f"Monitors updated:  {report.monitors_updated}")
        click.echo(f"Monitors skipped:  {report.monitors_skipped}")
        click.echo(f"Monitors failed:   {report.monitors_failed}")
        click.echo(f"Orphans cleaned:   {report.orphans_cleaned}")
        if report.unified_drift_views:
            click.echo(f"\nUnified views: {list(report.unified_drift_views.values())}")
        if report.drift_alert_ids:
            click.echo(f"Alerts: {list(report.drift_alert_ids.values())}")
        if report.dashboard_ids:
            click.echo(f"Dashboards: {list(report.dashboard_ids.values())}")
        click.echo("=" * 60)

    exit_code = EXIT_SUCCESS if report.monitors_failed == 0 else EXIT_RUNTIME
    sys.exit(exit_code)


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), default="table", help="Output format")
@click.pass_context
def coverage(ctx, config_path, output_format):
    """Analyse monitoring coverage gaps, stale monitors, and orphans."""
    try:
        config = load_config(config_path)
    except (FileNotFoundError, ValidationError) as e:
        click.echo(f"Config error: {e}", err=True)
        sys.exit(EXIT_VALIDATION)

    try:
        from databricks.sdk import WorkspaceClient

        from dpo import CoverageAnalyzer, _build_table_list

        w = WorkspaceClient()
        tables = _build_table_list(w, config)
        analyzer = CoverageAnalyzer(w, config)
        report = analyzer.analyze(tables)
    except Exception as e:
        click.echo(f"Runtime error: {e}", err=True)
        sys.exit(EXIT_RUNTIME)

    if output_format == "json":
        result = {
            "command": "coverage",
            "status": "success",
            "config_path": config_path,
            **report.to_dict(),
        }
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        click.echo("\n" + "=" * 60)
        click.echo("DPO COVERAGE GOVERNANCE REPORT")
        click.echo("=" * 60)
        click.echo(f"Catalog tables:    {report.total_catalog_tables}")
        click.echo(f"Monitored:         {report.total_monitored}")
        click.echo(f"Coverage:          {report.coverage_pct:.1f}%")
        click.echo(f"Unmonitored:       {len(report.unmonitored)}")
        click.echo(f"Stale monitors:    {len(report.stale)}")
        click.echo(f"Orphan monitors:   {len(report.orphans)}")

        if report.unmonitored:
            click.echo("\nUNMONITORED TABLES (first 20):")
            click.echo(tabulate(
                [[u.full_name, u.schema_name, u.owner or ""] for u in report.unmonitored[:20]],
                headers=["Table", "Schema", "Owner"],
                tablefmt="simple",
            ))

        if report.stale:
            click.echo("\nSTALE MONITORS:")
            click.echo(tabulate(
                [[s.table_name, s.days_since_refresh or "never", s.status] for s in report.stale[:20]],
                headers=["Table", "Days Since Refresh", "Status"],
                tablefmt="simple",
            ))

        if report.orphans:
            click.echo("\nORPHAN MONITORS (not in config):")
            click.echo(tabulate(
                [[o.table_name, o.reason] for o in report.orphans[:20]],
                headers=["Table", "Reason"],
                tablefmt="simple",
            ))

        click.echo("=" * 60)

    sys.exit(EXIT_SUCCESS)


def main():
    """Entry point for the DPO CLI."""
    cli()
