"""
Shared utilities for DPO.

Includes:
- Config hashing for smart diffing
- Permission verification
- API retry wrappers
- SQL identifier sanitization
"""

import hashlib
import json
import logging
import re
import time
from typing import Any, Callable, Dict, Literal

from databricks.sdk import WorkspaceClient
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

MAX_SQL_IDENTIFIER_LENGTH = 64


def sanitize_sql_identifier(
    name: str, max_length: int = MAX_SQL_IDENTIFIER_LENGTH
) -> str:
    """Convert a user-defined string to a safe SQL identifier.

    Handles cases like:
    - "Marketing & Sales (North America)" -> "marketing_sales_north_america"
    - "ML-Team" -> "ml_team"
    - "" -> "unnamed"

    Args:
        name: User-provided string (e.g., monitor_group tag value).
        max_length: Maximum length for the identifier.

    Returns:
        Safe, lowercase, alphanumeric-and-underscore string.
    """
    if not name or not name.strip():
        return "unnamed"

    # Replace all non-alphanumeric characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9]", "_", name)

    # Collapse multiple underscores into one
    sanitized = re.sub(r"_+", "_", sanitized)

    # Remove leading/trailing underscores
    sanitized = sanitized.strip("_")

    # Force lowercase
    sanitized = sanitized.lower()

    # Truncate to max length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length].rstrip("_")

    # Ensure result is not empty after processing
    return sanitized if sanitized else "unnamed"


def hash_config(config_dict: Dict[str, Any]) -> str:
    """
    Generate deterministic hash of config for change detection.

    Only hashes fields we control (ignores system fields like monitor_id, created_at).
    """
    controlled_fields = {
        "profile_type",
        "output_schema_name",
        "granularity",
        "problem_type",
        "prediction_column",
        "label_column",
        "timestamp_column",
        "model_id_column",
        "slicing_exprs",
        "baseline_table_name",
        "custom_metrics",
    }

    filtered = {k: v for k, v in config_dict.items() if k in controlled_fields}

    # Sort for deterministic output
    json_str = json.dumps(filtered, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()[:16]


def calculate_config_diff(existing_config: Dict[str, Any], desired_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare only user-controllable fields, ignoring system fields.

    Returns dict of differences: {field: (existing_value, desired_value)}
    """
    ignore_fields = {
        "monitor_id",
        "created_at",
        "updated_at",
        "status",
        "dashboard_id",
        "object_id",
        "object_type",
    }

    diff = {}

    all_keys = set(existing_config.keys()) | set(desired_config.keys())

    for key in all_keys:
        if key in ignore_fields:
            continue

        existing_val = existing_config.get(key)
        desired_val = desired_config.get(key)

        if existing_val != desired_val:
            diff[key] = (existing_val, desired_val)

    return diff


def verify_output_schema_permissions(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    warehouse_id: str,
    mode: Literal["full", "bulk_provision_only"] = "full",
) -> None:
    """Pre-flight check: Verify we can write to the output schema.

    Args:
        w: WorkspaceClient instance.
        catalog: Target catalog name.
        schema: Target schema name.
        warehouse_id: SQL Warehouse ID for statement execution.
        mode: When 'bulk_provision_only', only checks CREATE TABLE.
              When 'full', also checks CREATE VIEW permissions.

    Raises:
        PermissionError: If required permissions are missing.
    """
    test_table = f"{catalog}.{schema}._dpo_permission_check_{int(time.time())}"

    logger.info(f"Verifying write permissions on {catalog}.{schema} (mode={mode})")

    try:
        # Ensure schema exists first
        try:
            w.schemas.get(full_name=f"{catalog}.{schema}")
        except Exception:
            # Try to create the schema
            logger.info(f"Creating schema {catalog}.{schema}")
            w.schemas.create(name=schema, catalog_name=catalog)

        # Always check CREATE TABLE (needed for monitor outputs)
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"CREATE TABLE {test_table} (test_col INT) USING DELTA",
            wait_timeout="30s",
        )

        if result.status and result.status.state.value == "FAILED":
            raise PermissionError(f"Failed to create test table: {result.status.error}")

        # Clean up table immediately
        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DROP TABLE IF EXISTS {test_table}",
            wait_timeout="30s",
        )

        # Only check CREATE VIEW for full mode
        if mode == "full":
            test_view = f"{catalog}.{schema}._dpo_view_check_{int(time.time())}"
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"CREATE VIEW {test_view} AS SELECT 1 as test_col",
                wait_timeout="30s",
            )

            if result.status and result.status.state.value == "FAILED":
                raise PermissionError(
                    f"Failed to create test view: {result.status.error}. "
                    "Full mode requires CREATE VIEW permissions for aggregation."
                )

            # Clean up view
            w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DROP VIEW IF EXISTS {test_view}",
                wait_timeout="30s",
            )

        logger.info(
            f"Pre-flight check passed: Write permissions verified on {catalog}.{schema}"
        )

    except PermissionError:
        raise
    except Exception as e:
        error_msg = str(e).lower()
        if any(
            term in error_msg
            for term in ["permission", "denied", "unauthorized", "access"]
        ):
            raise PermissionError(
                f"FATAL: Cannot write to output schema '{catalog}.{schema}'. "
                f"Grant CREATE TABLE permission to the service principal before running DPO. "
                f"Original error: {e}"
            )
        # Re-raise other errors
        raise


def verify_view_permissions(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> None:
    """Pre-flight check: verify CREATE VIEW permissions in a schema."""
    test_view = f"{catalog}.{schema}._dpo_view_check_{int(time.time())}"

    logger.info("Verifying CREATE VIEW permissions on %s.%s", catalog, schema)

    try:
        try:
            w.schemas.get(full_name=f"{catalog}.{schema}")
        except Exception:
            logger.info("Creating schema %s.%s", catalog, schema)
            w.schemas.create(name=schema, catalog_name=catalog)

        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"CREATE VIEW {test_view} AS SELECT 1 as test_col",
            wait_timeout="30s",
        )

        if result.status and result.status.state.value == "FAILED":
            raise PermissionError(f"Failed to create test view: {result.status.error}")

        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DROP VIEW IF EXISTS {test_view}",
            wait_timeout="30s",
        )

        logger.info(
            "Pre-flight check passed: CREATE VIEW permissions verified on %s.%s",
            catalog,
            schema,
        )
    except PermissionError:
        raise
    except Exception as e:
        error_msg = str(e).lower()
        if any(
            term in error_msg
            for term in ["permission", "denied", "unauthorized", "access"]
        ):
            raise PermissionError(
                f"FATAL: Cannot create views in '{catalog}.{schema}'. "
                f"Grant CREATE VIEW permission before running full mode. "
                f"Original error: {e}"
            )
        raise


def create_retry_wrapper(
    max_attempts: int = 5,
    min_wait: int = 2,
    max_wait: int = 60,
) -> Callable:
    """
    Create a retry decorator with exponential backoff.

    Handles HTTP 429 (Too Many Requests) and transient errors.
    """

    def decorator(func: Callable) -> Callable:
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


def api_call_with_retry(func: Callable, *args, **kwargs) -> Any:
    """
    Execute an API call with retry logic for rate limiting.

    Handles HTTP 429 errors with exponential backoff.
    """

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _call():
        return func(*args, **kwargs)

    return _call()


def safe_get_table_id(w: WorkspaceClient, full_name: str) -> str | None:
    """Safely get table_id from full name."""
    try:
        table_info = w.tables.get(full_name=full_name)
        return table_info.table_id
    except Exception:
        return None


def safe_get_schema_id(w: WorkspaceClient, catalog: str, schema: str) -> str | None:
    """Safely get schema_id from catalog.schema."""
    try:
        schema_info = w.schemas.get(full_name=f"{catalog}.{schema}")
        return schema_info.schema_id
    except Exception:
        return None


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"
