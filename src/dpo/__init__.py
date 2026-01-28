"""
Data Profiling Orchestrator (DPO)

Automate Databricks Data Profiling at scale across Unity Catalog.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dpo.config import load_config, OrchestratorConfig
from dpo.discovery import TableDiscovery, DiscoveredTable
from dpo.provisioning import (
    ProfileProvisioner,
    ProvisioningResult,
    RefreshResult,
    MonitorStatus,
    get_monitor_statuses,
    wait_for_monitors,
    print_monitor_statuses,
)
from dpo.aggregator import MetricsAggregator
from dpo.alerting import AlertProvisioner
from dpo.dashboard import DashboardProvisioner
from dpo.utils import verify_output_schema_permissions, hash_config, sanitize_sql_identifier

__version__ = "0.1.0"

__all__ = [
    "load_config",
    "OrchestratorConfig",
    "TableDiscovery",
    "DiscoveredTable",
    "ProfileProvisioner",
    "ProvisioningResult",
    "RefreshResult",
    "MonitorStatus",
    "get_monitor_statuses",
    "wait_for_monitors",
    "print_monitor_statuses",
    "MetricsAggregator",
    "AlertProvisioner",
    "DashboardProvisioner",
    "verify_output_schema_permissions",
    "hash_config",
    "sanitize_sql_identifier",
    "run_orchestration",
    "run_bulk_provisioning",
    "OrchestrationReport",
]


@dataclass
class OrchestrationReport:
    """Summary report from orchestration run.

    In bulk_provision_only mode, all Dict fields are empty.
    In full mode with groups, each Dict is keyed by group name.
    """

    tables_discovered: int
    monitors_created: int
    monitors_updated: int
    monitors_skipped: int
    monitors_failed: int
    orphans_cleaned: int
    # Group-aware fields (empty dict for bulk mode)
    # Key = group name (e.g., "ml_team", "data_eng", "default")
    unified_drift_views: Dict[str, str] = field(default_factory=dict)
    unified_profile_views: Dict[str, str] = field(default_factory=dict)
    drift_alert_ids: Dict[str, str] = field(default_factory=dict)
    quality_alert_ids: Dict[str, str] = field(default_factory=dict)
    dashboard_ids: Dict[str, str] = field(default_factory=dict)
    monitor_statuses: List[MonitorStatus] = field(default_factory=list)


def run_bulk_provisioning(config: OrchestratorConfig) -> OrchestrationReport:
    """Simplified mode: Discover, provision, and cleanup only.

    Skips aggregation, alerting, and dashboard deployment.
    Use this for fastest onboarding when unified observability is not needed.

    Args:
        config: Validated orchestrator configuration.

    Returns:
        OrchestrationReport with execution summary (Dict fields empty).
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    catalog = config.discovery.catalog_name

    # 1. Pre-flight permission check (CREATE TABLE only, not VIEW)
    verify_output_schema_permissions(
        w=w,
        catalog=catalog,
        schema=config.profile_defaults.output_schema_name,
        warehouse_id=config.warehouse_id,
        mode="bulk_provision_only",
    )

    # 2. Discovery
    discovery = TableDiscovery(w, config.discovery)
    tables = discovery.discover()

    # 3. Provisioning
    provisioner = ProfileProvisioner(w, config)
    if config.dry_run:
        results = provisioner.dry_run_all(tables)
    else:
        results = provisioner.provision_all(tables)

    # 4. Wait for monitors to become ACTIVE
    monitor_statuses = []
    if config.wait_for_monitors and not config.dry_run:
        monitor_statuses = wait_for_monitors(
            w,
            tables,
            timeout_seconds=config.wait_timeout_seconds,
            poll_interval=config.wait_poll_interval,
        )

    # 5. Orphan cleanup
    orphans = []
    if config.cleanup_orphans and not config.dry_run:
        orphans = provisioner.cleanup_orphans(tables)

    # 6. Return report
    return OrchestrationReport(
        tables_discovered=len(tables),
        monitors_created=sum(1 for r in results if r.action == "created"),
        monitors_updated=sum(1 for r in results if r.action == "updated"),
        monitors_skipped=sum(1 for r in results if "skipped" in r.action),
        monitors_failed=sum(1 for r in results if r.action == "failed"),
        orphans_cleaned=len(orphans),
        monitor_statuses=monitor_statuses,
    )


def run_orchestration(config: OrchestratorConfig) -> OrchestrationReport:
    """Main entry point for DPO orchestration.

    Executes the full pipeline based on config.mode:
    - bulk_provision_only: Discovery + Provisioning + Cleanup only
    - full: Complete pipeline with per-group aggregation/alerting/dashboards

    Args:
        config: Validated orchestrator configuration.

    Returns:
        OrchestrationReport with execution summary.
    """
    # Dispatch to bulk mode if specified
    if config.mode == "bulk_provision_only":
        return run_bulk_provisioning(config)

    # Full mode with group support
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    catalog = config.discovery.catalog_name
    output_schema = f"{catalog}.global_monitoring"

    # 1. Pre-flight permission check (includes VIEW check for full mode)
    verify_output_schema_permissions(
        w=w,
        catalog=catalog,
        schema=config.profile_defaults.output_schema_name,
        warehouse_id=config.warehouse_id,
        mode="full",
    )

    # 2. Discovery
    discovery = TableDiscovery(w, config.discovery)
    tables = discovery.discover()

    # 3. Provisioning
    provisioner = ProfileProvisioner(w, config)

    if config.dry_run:
        results = provisioner.dry_run_all(tables)
    else:
        results = provisioner.provision_all(tables)

    # 4. Wait for monitors to become ACTIVE
    monitor_statuses = []
    if config.wait_for_monitors and not config.dry_run:
        monitor_statuses = wait_for_monitors(
            w,
            tables,
            timeout_seconds=config.wait_timeout_seconds,
            poll_interval=config.wait_poll_interval,
        )

    # 5. Orphan cleanup
    orphans = []
    if config.cleanup_orphans and not config.dry_run:
        orphans = provisioner.cleanup_orphans(tables)

    # 6. Aggregation - per group
    aggregator = MetricsAggregator(w, config)
    views_by_group = {}

    if not config.dry_run:
        views_by_group = aggregator.create_unified_views_by_group(
            tables,
            output_schema,
            config.monitor_group_tag,
        )

        # 6b. Cleanup stale views from renamed/deleted groups
        active_sanitized_groups = {
            sanitize_sql_identifier(g) for g in views_by_group.keys()
        }
        aggregator.cleanup_stale_views(output_schema, active_sanitized_groups)

    # 7. Alerting - per group
    alerts_by_group = {}
    if config.alerting.enable_aggregated_alerts and not config.dry_run:
        alerter = AlertProvisioner(w, config)
        alerts_by_group = alerter.create_alerts_by_group(views_by_group, catalog)

    # 8. Aggregated Dashboard - per group
    dashboards_by_group = {}
    if config.deploy_aggregated_dashboard and not config.dry_run:
        dashboard_provisioner = DashboardProvisioner(w, config)
        dashboards_by_group = dashboard_provisioner.deploy_dashboards_by_group(
            views_by_group, config.dashboard_parent_path
        )

        # 8b. Cleanup stale dashboards from renamed/deleted groups
        active_group_names = set(views_by_group.keys())
        dashboard_provisioner.cleanup_stale_dashboards(
            config.dashboard_parent_path, active_group_names
        )

    return OrchestrationReport(
        tables_discovered=len(tables),
        monitors_created=sum(1 for r in results if r.action == "created"),
        monitors_updated=sum(1 for r in results if r.action == "updated"),
        monitors_skipped=sum(1 for r in results if "skipped" in r.action),
        monitors_failed=sum(1 for r in results if r.action == "failed"),
        orphans_cleaned=len(orphans),
        unified_drift_views={g: v[0] for g, v in views_by_group.items()},
        unified_profile_views={g: v[1] for g, v in views_by_group.items()},
        drift_alert_ids={g: a[0] for g, a in alerts_by_group.items() if a[0]},
        quality_alert_ids={g: a[1] for g, a in alerts_by_group.items() if a[1]},
        dashboard_ids=dashboards_by_group,
        monitor_statuses=monitor_statuses,
    )
