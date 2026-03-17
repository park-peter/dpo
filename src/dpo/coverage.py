"""
Coverage Governance Module

Analyzes monitoring coverage gaps: unmonitored tables, stale monitors,
and orphaned monitors. Produces structured reports for CLI and dashboard
consumption.

Scope: catalog-scoped and discovery-policy-scoped. Only tables in the
configured catalog (respecting include/exclude schema filters) are
considered, and only monitors targeting that catalog are evaluated.
"""

import fnmatch
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from databricks.sdk import WorkspaceClient

from dpo.config import OrchestratorConfig
from dpo.discovery import DiscoveredTable

logger = logging.getLogger(__name__)


@dataclass
class UnmonitoredTable:
    """A table in the catalog that has no active monitor."""

    full_name: str
    schema_name: str
    owner: Optional[str] = None
    department: Optional[str] = None
    reason: str = "no_monitor"


@dataclass
class StaleMonitor:
    """A monitor that has not refreshed within the configured threshold."""

    table_name: str
    monitor_id: str
    refresh_state: Literal["stale", "never_refreshed", "refresh_history_unavailable"] = "stale"
    last_refresh: Optional[datetime] = None
    days_since_refresh: Optional[int] = None
    status: str = "unknown"


@dataclass
class OrphanMonitor:
    """A monitor whose source table is no longer in the DPO config."""

    table_name: str
    monitor_id: str
    reason: str = "not_in_config"


@dataclass
class CoverageReport:
    """Structured coverage governance report."""

    timestamp: str = ""
    total_catalog_tables: int = 0
    total_monitored: int = 0
    coverage_pct: float = 0.0
    unmonitored: List[UnmonitoredTable] = field(default_factory=list)
    stale: List[StaleMonitor] = field(default_factory=list)
    orphans: List[OrphanMonitor] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for JSON output."""
        return {
            "timestamp": self.timestamp,
            "summary": {
                "total_catalog_tables": self.total_catalog_tables,
                "total_monitored": self.total_monitored,
                "coverage_pct": round(self.coverage_pct, 2),
                "unmonitored_count": len(self.unmonitored),
                "stale_count": len(self.stale),
                "orphan_count": len(self.orphans),
            },
            "unmonitored": sorted(
                [{"table": u.full_name, "schema": u.schema_name, "owner": u.owner, "reason": u.reason} for u in self.unmonitored],
                key=lambda x: x["table"],
            ),
            "stale": sorted(
                [
                    {
                        "table": s.table_name,
                        "monitor_id": s.monitor_id,
                        "refresh_state": s.refresh_state,
                        "last_refresh": s.last_refresh.isoformat() if s.last_refresh else None,
                        "days_since_refresh": s.days_since_refresh,
                        "status": s.status,
                    }
                    for s in self.stale
                ],
                key=lambda x: x["table"],
            ),
            "orphans": sorted(
                [{"table": o.table_name, "monitor_id": o.monitor_id, "reason": o.reason} for o in self.orphans],
                key=lambda x: x["table"],
            ),
        }


class CoverageAnalyzer:
    """Computes unmonitored, stale, and orphan monitor sets.

    Catalog-scoped and discovery-policy-scoped: only tables within the
    configured catalog (respecting exclude/include schema filters) are
    enumerated, and only monitors targeting tables in that catalog are
    considered.
    """

    def __init__(self, workspace_client: WorkspaceClient, config: OrchestratorConfig):
        self.w = workspace_client
        self.config = config
        self.catalog = config.catalog_name
        self.stale_days = config.stale_monitor_days
        self._exclude_schemas = config.discovery.exclude_schemas if config.discovery else ["information_schema"]
        self._include_schemas = (config.discovery.include_schemas if config.discovery else None) or None

    def analyze(self, discovered_tables: List[DiscoveredTable]) -> CoverageReport:
        """Run full coverage analysis."""
        report = CoverageReport(timestamp=datetime.now(timezone.utc).isoformat())

        catalog_tables = self._list_catalog_tables()
        report.total_catalog_tables = len(catalog_tables)

        monitored_table_ids, monitored_table_names_by_id = self._get_monitored_tables(catalog_tables)
        monitored_table_names = set(monitored_table_names_by_id.values())
        report.total_monitored = len(monitored_table_names)

        if catalog_tables:
            report.coverage_pct = (len(monitored_table_names) / len(catalog_tables)) * 100

        report.unmonitored = self._find_unmonitored(catalog_tables, monitored_table_names)
        report.stale = self._find_stale(monitored_table_ids, monitored_table_names_by_id)

        config_table_names = self._get_config_table_names(discovered_tables)
        report.orphans = self._find_orphans(
            monitored_table_ids,
            config_table_names,
            monitored_table_names_by_id,
        )

        logger.info(
            "Coverage analysis: %d tables in catalog, %d monitored (%.1f%%), "
            "%d unmonitored, %d stale, %d orphans",
            report.total_catalog_tables,
            report.total_monitored,
            report.coverage_pct,
            len(report.unmonitored),
            len(report.stale),
            len(report.orphans),
        )

        return report

    def _list_catalog_tables(self) -> Dict[str, Dict[str, str]]:
        """List tables in the catalog, respecting discovery schema filters.

        Returns:
            Dict mapping full_name -> {schema, table_id (when available)}.
        """
        tables: Dict[str, Dict[str, str]] = {}

        try:
            for schema in self.w.schemas.list(catalog_name=self.catalog):
                if self._include_schemas:
                    if not any(fnmatch.fnmatch(schema.name, p) for p in self._include_schemas):
                        continue
                if any(fnmatch.fnmatch(schema.name, p) for p in self._exclude_schemas):
                    continue
                try:
                    for table_summary in self.w.tables.list(
                        catalog_name=self.catalog, schema_name=schema.name
                    ):
                        entry: Dict[str, str] = {"schema": schema.name}
                        table_id = getattr(table_summary, "table_id", None)
                        if table_id:
                            entry["table_id"] = table_id
                        tables[table_summary.full_name] = entry
                except Exception as e:
                    logger.debug("Failed to list tables in schema %s: %s", schema.name, e)
        except Exception as e:
            logger.warning("Failed to list schemas in catalog %s: %s", self.catalog, e)

        return tables

    def _get_monitored_tables(
        self,
        catalog_tables: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Get monitors and resolved table names scoped to the configured catalog.

        Args:
            catalog_tables: Pre-built catalog inventory from _list_catalog_tables().
                Reused by the fallback path to avoid a second catalog enumeration.

        Returns:
            Tuple:
              - Dict mapping object_id -> monitor_id for in-catalog monitors only.
              - Dict mapping object_id -> resolved full table name.
        """
        monitors: Dict[str, str] = {}
        table_names_by_id: Dict[str, str] = {}
        catalog_prefix = f"{self.catalog}."

        try:
            for monitor in self.w.data_quality.list_monitor():
                obj_id = getattr(monitor, "object_id", None)
                if not obj_id:
                    continue
                try:
                    info = self.w.tables.get(table_id=obj_id)
                    if info and info.full_name and info.full_name.startswith(catalog_prefix):
                        monitors[obj_id] = getattr(monitor, "monitor_id", obj_id)
                        table_names_by_id[obj_id] = info.full_name
                except Exception:
                    logger.debug("Could not resolve table for monitor %s; skipping", obj_id)
            logger.debug("Monitor discovery via list_monitor: %d monitors found", len(monitors))
            return monitors, table_names_by_id
        except Exception:
            logger.info(
                "list_monitor unavailable; falling back to cached catalog inventory"
            )

        inventory = catalog_tables if catalog_tables is not None else self._list_catalog_tables()
        for table_name, meta in inventory.items():
            table_id = meta.get("table_id")
            try:
                if not table_id:
                    info = self.w.tables.get(full_name=table_name)
                    if not info:
                        continue
                    table_id = info.table_id
                monitor = self.w.data_quality.get_monitor(
                    object_type="table", object_id=table_id
                )
                monitors[table_id] = getattr(monitor, "monitor_id", table_id)
                table_names_by_id[table_id] = table_name
            except Exception:
                pass

        return monitors, table_names_by_id

    def _resolve_table_names(
        self,
        table_ids: Dict[str, str],
        table_names_by_id: Optional[Dict[str, str]] = None,
    ) -> Set[str]:
        """Resolve table IDs to full names (already catalog-filtered)."""
        if table_names_by_id is not None:
            return set(table_names_by_id.values())

        names: Set[str] = set()
        for obj_id in table_ids:
            try:
                info = self.w.tables.get(table_id=obj_id)
                if info and info.full_name:
                    names.add(info.full_name)
            except Exception:
                pass
        return names

    def _find_unmonitored(
        self,
        catalog_tables: Dict[str, Dict[str, str]],
        monitored_names: Set[str],
    ) -> List[UnmonitoredTable]:
        """Find tables with no active monitor."""
        unmonitored = []
        for full_name, meta in catalog_tables.items():
            if full_name not in monitored_names:
                unmonitored.append(
                    UnmonitoredTable(
                        full_name=full_name,
                        schema_name=meta.get("schema", ""),
                    )
                )
        return unmonitored

    def _find_stale(
        self,
        monitored_table_ids: Dict[str, str],
        table_names_by_id: Optional[Dict[str, str]] = None,
    ) -> List[StaleMonitor]:
        """Find monitors that haven't refreshed within stale_days."""
        stale = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.stale_days)

        for obj_id, monitor_id in monitored_table_ids.items():
            try:
                monitor = self.w.data_quality.get_monitor(object_type="table", object_id=obj_id)
                table_name = (
                    table_names_by_id.get(obj_id) if table_names_by_id else None
                ) or self._table_name_from_id(obj_id)

                last_refresh = None
                status = "unknown"
                refresh_lookup_failed = False

                if monitor.data_profiling_config:
                    cfg = monitor.data_profiling_config
                    status = cfg.status.value if cfg.status else "unknown"

                    try:
                        refreshes = list(self.w.data_quality.list_refresh(object_type="table", object_id=obj_id))
                        if refreshes:
                            latest = max(refreshes, key=lambda r: getattr(r, "created_at", 0) or 0)
                            created = getattr(latest, "created_at", None)
                            if created:
                                last_refresh = datetime.fromtimestamp(created / 1000, tz=timezone.utc) if isinstance(created, (int, float)) else created
                    except Exception:
                        refresh_lookup_failed = True

                if (
                    last_refresh is None
                    and not refresh_lookup_failed
                    and status in (
                    "DATA_PROFILING_STATUS_ACTIVE",
                    "ACTIVE",
                    )
                ):
                    continue

                if refresh_lookup_failed:
                    stale.append(
                        StaleMonitor(
                            table_name=table_name or obj_id,
                            monitor_id=monitor_id,
                            refresh_state="refresh_history_unavailable",
                            status=status,
                        )
                    )
                elif last_refresh is None:
                    stale.append(
                        StaleMonitor(
                            table_name=table_name or obj_id,
                            monitor_id=monitor_id,
                            refresh_state="never_refreshed",
                            status=status,
                        )
                    )
                elif last_refresh < cutoff:
                    days = (datetime.now(timezone.utc) - last_refresh).days
                    stale.append(
                        StaleMonitor(
                            table_name=table_name or obj_id,
                            monitor_id=monitor_id,
                            refresh_state="stale",
                            last_refresh=last_refresh,
                            days_since_refresh=days,
                            status=status,
                        )
                    )

            except Exception as e:
                logger.debug("Failed to check staleness for monitor %s: %s", obj_id, e)

        return stale

    def _find_orphans(
        self,
        monitored_table_ids: Dict[str, str],
        config_names: Set[str],
        table_names_by_id: Optional[Dict[str, str]] = None,
    ) -> List[OrphanMonitor]:
        """Find monitors whose tables are not in the DPO config."""
        orphans = []
        name_to_id = {}

        for obj_id, monitor_id in monitored_table_ids.items():
            name = (
                table_names_by_id.get(obj_id) if table_names_by_id else None
            ) or self._table_name_from_id(obj_id)
            if name:
                name_to_id[name] = monitor_id

        for name, monitor_id in name_to_id.items():
            if name not in config_names:
                orphans.append(OrphanMonitor(table_name=name, monitor_id=monitor_id))

        return orphans

    def _get_config_table_names(self, discovered_tables: List[DiscoveredTable]) -> Set[str]:
        """Get the set of table names from config + discovery."""
        names = set(self.config.monitored_tables.keys())
        names.update(t.full_name for t in discovered_tables)
        return names

    def _table_name_from_id(self, obj_id: str) -> Optional[str]:
        """Resolve a table ID to its full name."""
        try:
            info = self.w.tables.get(table_id=obj_id)
            return info.full_name if info else None
        except Exception:
            return None
