"""
Data Profiling Orchestrator (DPO)

Automate Databricks Data Profiling at scale across Unity Catalog.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dpo.aggregator import MetricsAggregator
from dpo.alerting import AlertProvisioner
from dpo.config import OrchestratorConfig, load_config
from dpo.coverage import CoverageAnalyzer, CoverageReport
from dpo.dashboard import DashboardProvisioner
from dpo.discovery import DiscoveredTable, TableDiscovery
from dpo.provisioning import (
    ImpactReport,
    MonitorStatus,
    ProfileProvisioner,
    ProvisioningResult,
    RefreshResult,
    get_monitor_statuses,
    print_monitor_statuses,
    wait_for_monitors,
)
from dpo.utils import (
    hash_config,
    sanitize_sql_identifier,
    verify_output_schema_permissions,
    verify_view_permissions,
)

__version__ = "0.3.0rc1"

logger = logging.getLogger(__name__)

__all__ = [
    "load_config",
    "OrchestratorConfig",
    "TableDiscovery",
    "DiscoveredTable",
    "ProfileProvisioner",
    "ProvisioningResult",
    "ImpactReport",
    "RefreshResult",
    "MonitorStatus",
    "get_monitor_statuses",
    "wait_for_monitors",
    "print_monitor_statuses",
    "MetricsAggregator",
    "AlertProvisioner",
    "DashboardProvisioner",
    "CoverageAnalyzer",
    "CoverageReport",
    "verify_output_schema_permissions",
    "verify_view_permissions",
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
    unified_drift_views: Dict[str, str] = field(default_factory=dict)
    unified_profile_views: Dict[str, str] = field(default_factory=dict)
    drift_alert_ids: Dict[str, str] = field(default_factory=dict)
    quality_alert_ids: Dict[str, str] = field(default_factory=dict)
    dashboard_ids: Dict[str, str] = field(default_factory=dict)
    rollup_dashboard_id: Optional[str] = None
    monitor_statuses: List[MonitorStatus] = field(default_factory=list)
    # Dry-run structured report
    impact_report: Optional[ImpactReport] = None
    # Coverage report
    coverage_report: Optional[CoverageReport] = None

    def to_dict(self) -> dict:
        """Serialize report for JSON output."""
        data = {
            "tables_discovered": self.tables_discovered,
            "monitors_created": self.monitors_created,
            "monitors_updated": self.monitors_updated,
            "monitors_skipped": self.monitors_skipped,
            "monitors_failed": self.monitors_failed,
            "orphans_cleaned": self.orphans_cleaned,
            "unified_drift_views": self.unified_drift_views,
            "unified_profile_views": self.unified_profile_views,
            "drift_alert_ids": self.drift_alert_ids,
            "quality_alert_ids": self.quality_alert_ids,
            "dashboard_ids": self.dashboard_ids,
            "rollup_dashboard_id": self.rollup_dashboard_id,
        }
        if self.impact_report:
            data["impact"] = self.impact_report.to_dict()
        if self.coverage_report:
            data["coverage"] = self.coverage_report.to_dict()
        return data


def _create_table_from_config(w, table_name: str) -> DiscoveredTable:
    """Create a DiscoveredTable from a monitored_tables entry."""
    table_info = w.tables.get(full_name=table_name)

    tags = {}
    try:
        tag_assignments = w.entity_tag_assignments.list(
            entity_type="TABLE", entity_name=table_name
        )
        tags = {tag.tag_key: tag.tag_value for tag in tag_assignments}
    except Exception as e:
        logger.debug(f"Could not fetch tags for {table_name}: {e}")

    return DiscoveredTable(
        full_name=table_name,
        tags=tags,
        has_primary_key=False,
        columns=table_info.columns or [],
        table_type=table_info.table_type.value if table_info.table_type else "UNKNOWN",
        priority=int(tags.get("monitor_priority", "99")),
    )


def _build_table_list(w, config: OrchestratorConfig) -> List[DiscoveredTable]:
    """Build the list of tables to process with YAML precedence.

    Resolves enrichment metadata for each table after building.
    """
    tables = []
    yaml_table_names = set(config.monitored_tables.keys())

    # 1. Tag-discovered tables (excluding YAML overrides)
    if config.include_tagged_tables and config.discovery:
        discovery = TableDiscovery(w, config.discovery, config.catalog_name)
        for table in discovery.discover():
            if table.full_name not in yaml_table_names:
                tables.append(table)
            else:
                logger.debug(
                    f"Skipping tag-discovered {table.full_name} - YAML config takes precedence"
                )

    # 2. All tables from monitored_tables (authoritative)
    for table_name in config.monitored_tables.keys():
        tables.append(_create_table_from_config(w, table_name))

    # 3. Resolve enrichment metadata for all tables
    for table in tables:
        table_config = config.monitored_tables.get(table.full_name)
        table.resolve_enrichment(table_config)

    logger.info(f"Built table list: {len(tables)} tables to process")
    return tables


def _successful_tables(
    tables: List[DiscoveredTable], results: List[ProvisioningResult]
) -> List[DiscoveredTable]:
    """Return tables with monitors that exist and are usable downstream."""
    success_actions = {"created", "updated", "no_change"}
    table_map = {t.full_name: t for t in tables}
    return [
        table_map[r.table_name]
        for r in results
        if r.action in success_actions and r.table_name in table_map
    ]


def run_bulk_provisioning(config: OrchestratorConfig) -> OrchestrationReport:
    """Simplified mode: Provision monitors only."""
    from databricks.sdk import WorkspaceClient

    from dpo.validators import run_preflight_checks

    w = WorkspaceClient()
    catalog = config.catalog_name

    run_preflight_checks(config, w)

    verify_output_schema_permissions(
        w=w,
        catalog=catalog,
        schema=config.profile_defaults.output_schema_name,
        warehouse_id=config.warehouse_id,
        mode="bulk_provision_only",
    )

    tables = _build_table_list(w, config)
    provisioner = ProfileProvisioner(w, config)
    impact_report = None
    if config.dry_run:
        results, impact_report = provisioner.dry_run_all(tables)
    else:
        results = provisioner.provision_all(tables)

    healthy_tables = _successful_tables(tables, results)

    monitor_statuses = []
    if config.wait_for_monitors and not config.dry_run and healthy_tables:
        monitor_statuses = wait_for_monitors(
            w,
            healthy_tables,
            timeout_seconds=config.wait_timeout_seconds,
            poll_interval=config.wait_poll_interval,
        )
    elif config.wait_for_monitors and not config.dry_run:
        logger.warning("No successfully provisioned monitors; skipping status wait")

    orphans = []
    if config.cleanup_orphans and not config.dry_run:
        orphans = provisioner.cleanup_orphans(tables)

    coverage_report = None
    try:
        coverage_report = CoverageAnalyzer(w, config).analyze(tables)
    except Exception as e:
        logger.warning("Coverage analysis failed (continuing): %s", e)

    return OrchestrationReport(
        tables_discovered=len(tables),
        monitors_created=sum(1 for r in results if r.action == "created"),
        monitors_updated=sum(1 for r in results if r.action == "updated"),
        monitors_skipped=sum(1 for r in results if "skipped" in r.action),
        monitors_failed=sum(1 for r in results if r.action == "failed"),
        orphans_cleaned=len(orphans),
        monitor_statuses=monitor_statuses,
        impact_report=impact_report,
        coverage_report=coverage_report,
    )


def run_orchestration(config: OrchestratorConfig) -> OrchestrationReport:
    """Main entry point for DPO orchestration.

    Executes the full pipeline based on config.mode:
    - bulk_provision_only: Discovery + Provisioning + Cleanup only
    - full: Complete pipeline with per-group aggregation/alerting/dashboards
    """
    if config.mode == "bulk_provision_only":
        return run_bulk_provisioning(config)

    from databricks.sdk import WorkspaceClient

    from dpo.validators import run_preflight_checks

    w = WorkspaceClient()
    catalog = config.catalog_name
    output_schema = f"{catalog}.global_monitoring"

    # 1. Pre-flight checks (UC functions, etc.)
    run_preflight_checks(config, w)

    # 2. Pre-flight permission checks
    verify_output_schema_permissions(
        w=w,
        catalog=catalog,
        schema=config.profile_defaults.output_schema_name,
        warehouse_id=config.warehouse_id,
        mode="bulk_provision_only",
    )
    verify_view_permissions(
        w=w,
        catalog=catalog,
        schema="global_monitoring",
        warehouse_id=config.warehouse_id,
    )

    # 2. Build table list (handles discovery + monitored_tables + enrichment)
    tables = _build_table_list(w, config)
    # 3. Provisioning
    provisioner = ProfileProvisioner(w, config)
    impact_report = None

    if config.dry_run:
        results, impact_report = provisioner.dry_run_all(tables)
    else:
        results = provisioner.provision_all(tables)

    healthy_tables = _successful_tables(tables, results)

    # 4. Wait for monitors
    monitor_statuses = []
    if config.wait_for_monitors and not config.dry_run and healthy_tables:
        monitor_statuses = wait_for_monitors(
            w,
            healthy_tables,
            timeout_seconds=config.wait_timeout_seconds,
            poll_interval=config.wait_poll_interval,
        )
    elif config.wait_for_monitors and not config.dry_run:
        logger.warning("No healthy tables available; skipping aggregation/alerting/dashboard steps")

    # 5. Orphan cleanup
    orphans = []
    if config.cleanup_orphans and not config.dry_run:
        orphans = provisioner.cleanup_orphans(tables)

    coverage_report = None
    try:
        coverage_report = CoverageAnalyzer(w, config).analyze(tables)
    except Exception as e:
        logger.warning("Coverage analysis failed (continuing): %s", e)

    # 6. Aggregation
    aggregator = MetricsAggregator(w, config)
    views_by_group = {}

    if not config.dry_run and healthy_tables:
        views_by_group = aggregator.create_unified_views_by_group(
            healthy_tables,
            output_schema,
            config.monitor_group_tag,
        )

        active_sanitized_groups = {
            sanitize_sql_identifier(g) for g in views_by_group.keys()
        }
        aggregator.cleanup_stale_views(output_schema, active_sanitized_groups)

    # 7. Alerting
    alerts_by_group = {}
    if config.alerting.enable_aggregated_alerts and not config.dry_run and views_by_group:
        alerter = AlertProvisioner(w, config)
        alerts_by_group = alerter.create_alerts_by_group(views_by_group, catalog)

    # 8. Performance views
    perf_view: Optional[str] = None
    if not config.dry_run and healthy_tables:
        try:
            perf_view = f"{output_schema}.unified_performance_metrics"
            aggregator.create_unified_performance_view(healthy_tables, perf_view)
        except Exception as e:
            logger.warning("Performance view creation failed (continuing): %s", e)
            perf_view = None

    # 9. Dashboards
    dashboards_by_group = {}
    rollup_dashboard_id = None
    if config.deploy_aggregated_dashboard and not config.dry_run and views_by_group:
        dashboard_provisioner = DashboardProvisioner(w, config)
        dashboards_by_group = dashboard_provisioner.deploy_dashboards_by_group(
            views_by_group,
            config.dashboard_parent_path,
            unified_performance_view=perf_view,
            coverage_report=coverage_report,
        )
        active_group_names = set(views_by_group.keys())
        dashboard_provisioner.cleanup_stale_dashboards(
            config.dashboard_parent_path, active_group_names
        )

        # Executive rollup
        if config.deploy_executive_rollup and len(views_by_group) > 1:
            try:
                rollup_dashboard_id = dashboard_provisioner.deploy_executive_rollup(
                    views_by_group, config.dashboard_parent_path, coverage_report=coverage_report
                )
            except Exception as e:
                logger.warning("Executive rollup dashboard failed (continuing): %s", e)

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
        rollup_dashboard_id=rollup_dashboard_id,
        monitor_statuses=monitor_statuses,
        impact_report=impact_report,
        coverage_report=coverage_report,
    )
