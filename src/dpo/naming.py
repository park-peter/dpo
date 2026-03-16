"""Canonical naming helpers for DPO artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dpo.utils import sanitize_sql_identifier

DEFAULT_GROUP_NAME = "default"
DEFAULT_PRIORITY = 99


def normalize_monitor_priority(value: object) -> int:
    """Normalize a monitor priority tag into an integer."""
    try:
        return int(value) if value is not None else DEFAULT_PRIORITY
    except (TypeError, ValueError):
        return DEFAULT_PRIORITY


def table_leaf_name(full_name: str) -> str:
    """Return the table name segment from a fully-qualified name."""
    return full_name.split(".")[-1]


def full_name_slug(full_name: str) -> str:
    """Build a stable slug from the full table name."""
    return sanitize_sql_identifier(full_name.replace(".", "_"))


def group_slug(group_name: str) -> str:
    """Build the SQL-safe group identifier."""
    return sanitize_sql_identifier(group_name or DEFAULT_GROUP_NAME)


@dataclass(frozen=True)
class GroupArtifactNames:
    """Resolved names for all group-scoped artifacts."""

    drift_view: str
    profile_view: str
    performance_view: Optional[str]
    dashboard_name: str


def build_group_artifact_names(output_schema: str, group_name: str) -> GroupArtifactNames:
    """Build the artifact names for a monitor group."""
    slug = group_slug(group_name)
    return GroupArtifactNames(
        drift_view=f"{output_schema}.unified_drift_metrics_{slug}",
        profile_view=f"{output_schema}.unified_profile_metrics_{slug}",
        performance_view=f"{output_schema}.unified_performance_metrics_{slug}",
        dashboard_name=f"DPO Health - {group_name}",
    )
