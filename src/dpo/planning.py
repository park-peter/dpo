"""Canonical execution planning for DPO runs."""

from __future__ import annotations

from dataclasses import dataclass, field

from dpo.config import OrchestratorConfig
from dpo.discovery import DiscoveredTable
from dpo.naming import (
    DEFAULT_GROUP_NAME,
    GroupArtifactNames,
    build_group_artifact_names,
    full_name_slug,
    group_slug,
    table_leaf_name,
)


class PlanningError(ValueError):
    """Raised when execution planning detects invalid artifact identity."""


@dataclass(frozen=True)
class PlannedTable:
    """Resolved planning metadata for a monitored table."""

    table: DiscoveredTable
    leaf_name: str
    group_name: str
    group_slug: str
    assets_suffix: str


@dataclass(frozen=True)
class PlannedGroup:
    """Resolved group-scoped execution data."""

    name: str
    slug: str
    tables: tuple[PlannedTable, ...]
    artifacts: GroupArtifactNames


@dataclass(frozen=True)
class ExecutionPlan:
    """Fully resolved execution plan for a DPO run."""

    tables: tuple[PlannedTable, ...]
    groups: dict[str, PlannedGroup]
    output_schema: str
    tables_by_name: dict[str, PlannedTable] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tables_by_name",
            {planned.table.full_name: planned for planned in self.tables},
        )

    def groups_for_tables(self, table_names: set[str]) -> dict[str, PlannedGroup]:
        """Return only the groups that contain the requested table names."""
        selected: dict[str, PlannedGroup] = {}
        for name, group in self.groups.items():
            filtered_tables = tuple(
                planned
                for planned in group.tables
                if planned.table.full_name in table_names
            )
            if filtered_tables:
                selected[name] = PlannedGroup(
                    name=group.name,
                    slug=group.slug,
                    tables=filtered_tables,
                    artifacts=group.artifacts,
                )
        return selected


def build_execution_plan(
    config: OrchestratorConfig,
    tables: list[DiscoveredTable],
) -> ExecutionPlan:
    """Resolve the canonical table/group/artifact plan for a run."""
    _validate_leaf_name_collisions(tables)

    output_schema = f"{config.catalog_name}.global_monitoring"
    grouped_tables: dict[str, list[PlannedTable]] = {}
    planned_tables: list[PlannedTable] = []

    for table in tables:
        group_name = table.tags.get(config.monitor_group_tag, DEFAULT_GROUP_NAME)
        planned = PlannedTable(
            table=table,
            leaf_name=table_leaf_name(table.full_name),
            group_name=group_name,
            group_slug=group_slug(group_name),
            assets_suffix=full_name_slug(table.full_name),
        )
        grouped_tables.setdefault(group_name, []).append(planned)
        planned_tables.append(planned)

    _validate_group_slug_collisions(grouped_tables)

    groups = {
        name: PlannedGroup(
            name=name,
            slug=group_slug(name),
            tables=tuple(group_tables),
            artifacts=build_group_artifact_names(output_schema, name),
        )
        for name, group_tables in grouped_tables.items()
    }

    return ExecutionPlan(
        tables=tuple(planned_tables),
        groups=groups,
        output_schema=output_schema,
    )


def _validate_leaf_name_collisions(tables: list[DiscoveredTable]) -> None:
    """Reject duplicate leaf names that would collide in Databricks artifacts."""
    collisions: dict[str, list[str]] = {}
    for table in tables:
        collisions.setdefault(table_leaf_name(table.full_name), []).append(table.full_name)

    duplicates = {leaf: names for leaf, names in collisions.items() if len(names) > 1}
    if duplicates:
        details = ", ".join(
            f"{leaf}: {sorted(names)}"
            for leaf, names in sorted(duplicates.items())
        )
        raise PlanningError(
            "Duplicate table leaf names are not allowed because Databricks monitor "
            f"artifacts collide: {details}"
        )


def _validate_group_slug_collisions(grouped_tables: dict[str, list[PlannedTable]]) -> None:
    """Reject monitor-group names that sanitize to the same artifact slug."""
    grouped_by_slug: dict[str, list[str]] = {}
    for group_name in grouped_tables:
        grouped_by_slug.setdefault(group_slug(group_name), []).append(group_name)

    duplicates = {slug: names for slug, names in grouped_by_slug.items() if len(names) > 1}
    if duplicates:
        details = ", ".join(
            f"{slug}: {sorted(names)}"
            for slug, names in sorted(duplicates.items())
        )
        raise PlanningError(
            "Duplicate sanitized monitor group names are not allowed because "
            f"group-scoped artifacts would collide: {details}"
        )
