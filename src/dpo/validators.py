"""Shared workspace validation helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from databricks.sdk.errors import NotFound

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from dpo.config import OrchestratorConfig

logger = logging.getLogger(__name__)


def validate_uc_functions(
    config: "OrchestratorConfig",
    w: "WorkspaceClient",
) -> list[dict]:
    """Validate all UC functions referenced by objective functions exist.

    Args:
        config: The orchestrator configuration.
        w: Authenticated WorkspaceClient.

    Returns:
        List of issue dicts (same format as validate_workspace).
    """
    issues = []
    seen_functions: set[str] = set()
    for obj_key, obj_func in config.objective_functions.items():
        uc_name = obj_func.uc_function_name
        if not uc_name or uc_name in seen_functions:
            continue
        seen_functions.add(uc_name)
        try:
            w.functions.get(name=uc_name)
        except NotFound:
            issues.append({
                "check": "uc_function",
                "objective": obj_key,
                "function": uc_name,
                "message": (
                    f"UC function '{uc_name}' referenced by "
                    f"objective '{obj_key}' not found."
                ),
            })
    return issues


def run_preflight_checks(
    config: "OrchestratorConfig",
    w: "WorkspaceClient",
) -> None:
    """Run all pre-provisioning validation checks.

    Args:
        config: The orchestrator configuration.
        w: Authenticated WorkspaceClient.

    Raises:
        ValueError: If any preflight check fails.
    """
    if config.objective_functions:
        uc_issues = validate_uc_functions(config, w)
        if uc_issues:
            raise ValueError(f"UC function validation failed: {uc_issues}")
