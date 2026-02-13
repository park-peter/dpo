"""Idempotent Provisioning Controller.

Manages the create/update lifecycle of Data Profiling monitors.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo
from databricks.sdk.service.dataquality import (
    AggregationGranularity,
    DataProfilingConfig,
    DataProfilingCustomMetric,
    DataProfilingCustomMetricType,
    InferenceLogConfig,
    InferenceProblemType,
    Monitor,
    Refresh,
    RefreshState,
    SnapshotConfig,
    TimeSeriesConfig,
)
from tabulate import tabulate
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dpo.config import OrchestratorConfig
from dpo.discovery import DiscoveredTable
from dpo.utils import calculate_config_diff, hash_config

logger = logging.getLogger(__name__)


@dataclass
class ProvisioningResult:
    """Result of a single table provisioning operation."""

    table_name: str
    action: Literal[
        "created",
        "updated",
        "no_change",
        "skipped_quota",
        "skipped_no_pk",
        "skipped_cardinality",
        "skipped_column_missing",
        "failed",
        "dry_run",
    ]
    success: bool
    error_message: Optional[str] = None
    monitor_id: Optional[str] = None
    config_hash: Optional[str] = None


class ImpactReport:
    """Generates human-readable and structured dry run impact reports."""

    def __init__(self):
        self.planned_actions: List[dict] = []
        self.validation_warnings: List[dict] = []

    def add(self, table: str, action: str, reason: str = ""):
        """Add a planned action to the report."""
        self.planned_actions.append({
            "table": table,
            "action": action,
            "reason": reason,
        })

    def add_warning(self, table: str, warning: str):
        """Add a validation warning (column/slicing issues found during dry-run)."""
        self.validation_warnings.append({"table": table, "warning": warning})

    def to_dict(self) -> dict:
        """Return structured dict for programmatic consumption."""
        actions = [p["action"] for p in self.planned_actions]
        return {
            "summary": {
                "create": actions.count("create"),
                "update": actions.count("update"),
                "no_change": actions.count("no_change"),
                "skip_quota": actions.count("skip_quota"),
                "skip_column_missing": actions.count("skip_column_missing"),
                "skip_cardinality": actions.count("skip_cardinality"),
                "total": len(self.planned_actions),
            },
            "actions": sorted(self.planned_actions, key=lambda p: p["table"]),
            "warnings": sorted(self.validation_warnings, key=lambda w: w["table"]),
        }

    def print_summary(self):
        """Print human-readable impact report."""
        actions = [p["action"] for p in self.planned_actions]
        summary = {
            "New Monitors": actions.count("create"),
            "Updates": actions.count("update"),
            "Skipped (Quota)": actions.count("skip_quota"),
            "Skipped (Column Missing)": actions.count("skip_column_missing"),
            "Skipped (High Cardinality)": actions.count("skip_cardinality"),
            "No Change": actions.count("no_change"),
        }

        print("\n" + "=" * 60)
        print("DPO DRY RUN - IMPACT REPORT")
        print("=" * 60)

        print("\nPLANNED ACTIONS:")
        print(tabulate(
            [[k, v] for k, v in summary.items() if v > 0],
            headers=["Action", "Count"],
            tablefmt="simple",
        ))

        print("\nDETAILS (first 20):")
        print(tabulate(
            [
                [p["table"].split(".")[-1], p["action"], p["reason"]]
                for p in self.planned_actions[:20]
            ],
            headers=["Table", "Action", "Reason"],
            tablefmt="simple",
        ))

        if len(self.planned_actions) > 20:
            print(f"\n... and {len(self.planned_actions) - 20} more tables")

        if self.validation_warnings:
            print("\nVALIDATION WARNINGS:")
            print(tabulate(
                [[w["table"].split(".")[-1], w["warning"]] for w in self.validation_warnings[:10]],
                headers=["Table", "Warning"],
                tablefmt="simple",
            ))

        print("\n" + "=" * 60)
        print("To execute: dpo run <config_path> --confirm")
        print("=" * 60 + "\n")


class ProfileProvisioner:
    """Provisions Data Profiling monitors for discovered tables.

    Supports all three profile types: SNAPSHOT, TIMESERIES, and INFERENCE.

    Features:
    - Parallel provisioning with ThreadPoolExecutor
    - Graceful quota handling (skip, don't crash)
    - Smart config diffing with hashing
    - Cardinality explosion prevention
    - Rate limiting with exponential backoff
    - Custom metrics provisioning
    - Per-table setting resolution (monitored_tables > profile_defaults)
    - Column existence validation
    """

    MAX_MONITORS_PER_METASTORE = 100
    MAX_WORKERS = 10

    GRANULARITY_MAP = {
        "5 minutes": AggregationGranularity.AGGREGATION_GRANULARITY_5_MINUTES,
        "30 minutes": AggregationGranularity.AGGREGATION_GRANULARITY_30_MINUTES,
        "1 hour": AggregationGranularity.AGGREGATION_GRANULARITY_1_HOUR,
        "1 day": AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY,
        "1 week": AggregationGranularity.AGGREGATION_GRANULARITY_1_WEEK,
        "2 weeks": AggregationGranularity.AGGREGATION_GRANULARITY_2_WEEKS,
        "3 weeks": AggregationGranularity.AGGREGATION_GRANULARITY_3_WEEKS,
        "4 weeks": AggregationGranularity.AGGREGATION_GRANULARITY_4_WEEKS,
        "1 month": AggregationGranularity.AGGREGATION_GRANULARITY_1_MONTH,
        "1 year": AggregationGranularity.AGGREGATION_GRANULARITY_1_YEAR,
    }

    def __init__(
        self, workspace_client: WorkspaceClient, config: OrchestratorConfig
    ):
        self.w = workspace_client
        self.config = config
        self.catalog = config.catalog_name
        self.output_schema = config.profile_defaults.output_schema_name
        self.warehouse_id = config.warehouse_id

        try:
            self.username = self.w.current_user.me().user_name
        except Exception:
            self.username = "dpo_service"

    def _resolve_setting(self, table: DiscoveredTable, setting: str) -> Any:
        """Resolve a setting with priority: monitored_tables > profile_defaults.

        Args:
            table: The discovered table.
            setting: The setting name to resolve.

        Returns:
            The resolved setting value.
        """
        table_config = self.config.monitored_tables.get(table.full_name)
        if table_config:
            value = getattr(table_config, setting, None)
            if value is not None:
                return value
        return getattr(self.config.profile_defaults, setting, None)

    def _validate_columns_exist(self, table: DiscoveredTable) -> None:
        """Validate that configured columns exist in the table schema.

        Args:
            table: The discovered table with column info.

        Raises:
            ValueError: If any configured column is not found in the table.
        """
        table_columns = {col.name.lower() for col in table.columns}
        columns_to_check: List[tuple[str, str]] = []

        profile_type = self.config.profile_defaults.profile_type

        if profile_type == "INFERENCE":
            prediction_col = self._resolve_setting(table, "prediction_column")
            if prediction_col:
                columns_to_check.append(("prediction_column", prediction_col))

            timestamp_col = self._resolve_setting(table, "timestamp_column")
            if timestamp_col:
                columns_to_check.append(("timestamp_column", timestamp_col))

            label_col = self._resolve_setting(table, "label_column")
            if label_col:
                columns_to_check.append(("label_column", label_col))

            model_id_col = self._resolve_setting(table, "model_id_column")
            if model_id_col:
                columns_to_check.append(("model_id_column", model_id_col))

        elif profile_type == "TIMESERIES":
            ts_timestamp = self._resolve_setting(table, "timestamp_column")
            if not ts_timestamp:
                ts_timestamp = self.config.profile_defaults.timeseries_timestamp_column
            if ts_timestamp:
                columns_to_check.append(("timeseries_timestamp_column", ts_timestamp))

        for setting_name, column_name in columns_to_check:
            if column_name.lower() not in table_columns:
                available = sorted(table_columns)
                raise ValueError(
                    f"Column '{column_name}' (from {setting_name}) not found in table "
                    f"'{table.full_name}'. Available columns: {available}"
                )

    def provision_all(
        self, tables: List[DiscoveredTable]
    ) -> List[ProvisioningResult]:
        """Provision monitors for all tables with graceful quota handling.

        Args:
            tables: List of discovered tables to provision monitors for.

        Returns:
            List of provisioning results for each table.
        """
        current_count = self._get_monitor_count()
        remaining_quota = max(0, self.MAX_MONITORS_PER_METASTORE - current_count)

        logger.info(
            "Current monitor count: %d/%d",
            current_count,
            self.MAX_MONITORS_PER_METASTORE,
        )
        logger.info(
            "Tables to process: %d, Remaining quota: %d",
            len(tables),
            remaining_quota,
        )

        tables_to_provision = (
            tables[:remaining_quota] if remaining_quota < len(tables) else tables
        )
        tables_skipped_quota = (
            tables[remaining_quota:] if remaining_quota < len(tables) else []
        )

        results = []

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            future_to_table = {
                executor.submit(self._provision_single, table): table
                for table in tables_to_provision
            }

            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error("Provisioning failed for %s: %s", table.full_name, e)
                    results.append(ProvisioningResult(
                        table_name=table.full_name,
                        action="failed",
                        success=False,
                        error_message=str(e),
                    ))

        for table in tables_skipped_quota:
            logger.warning("Skipped (quota): %s", table.full_name)
            results.append(ProvisioningResult(
                table_name=table.full_name,
                action="skipped_quota",
                success=False,
                error_message=(
                    f"Monitor quota reached ({self.MAX_MONITORS_PER_METASTORE}). "
                    "Request increase from Databricks support."
                ),
            ))

        return results

    def dry_run_all(
        self, tables: List[DiscoveredTable]
    ) -> tuple[List[ProvisioningResult], ImpactReport]:
        """Execute a dry run with full column/slicing validation.

        Mirrors real provisioning validations so the preview matches
        actual apply outcomes.

        Args:
            tables: List of discovered tables to evaluate.

        Returns:
            Tuple of (results list, ImpactReport with structured data).
        """
        report = ImpactReport()
        results = []

        current_count = self._get_monitor_count()
        remaining_quota = max(0, self.MAX_MONITORS_PER_METASTORE - current_count)

        for i, table in enumerate(tables):
            if i >= remaining_quota:
                report.add(table.full_name, "skip_quota", "Monitor quota reached")
                results.append(ProvisioningResult(
                    table_name=table.full_name,
                    action="skipped_quota",
                    success=False,
                ))
                continue

            # Mirror real provisioning: validate columns exist
            try:
                self._validate_columns_exist(table)
            except ValueError as e:
                report.add(table.full_name, "skip_column_missing", str(e))
                report.add_warning(table.full_name, str(e))
                results.append(ProvisioningResult(
                    table_name=table.full_name,
                    action="skipped_column_missing",
                    success=False,
                    error_message=str(e),
                ))
                continue

            # Mirror real provisioning: validate slicing columns
            candidate_slicing = self._resolve_setting(table, "slicing_exprs") or []
            table_column_names = {col.name.lower() for col in table.columns}
            for col_name in candidate_slicing:
                if col_name.lower() not in table_column_names:
                    report.add_warning(table.full_name, f"Slicing column '{col_name}' not found in table schema")

            existing = self._get_existing_monitor(table)

            if existing:
                desired_config = self._build_config_dict(table)
                desired_hash = hash_config(desired_config)
                if self._has_config_drift(existing, table):
                    report.add(table.full_name, "update", "config drift detected")
                else:
                    report.add(table.full_name, "no_change", "monitor config already in sync")
                results.append(ProvisioningResult(
                    table_name=table.full_name,
                    action="dry_run",
                    success=True,
                    config_hash=desired_hash,
                ))
            else:
                report.add(table.full_name, "create", "")
                results.append(ProvisioningResult(
                    table_name=table.full_name,
                    action="dry_run",
                    success=True,
                ))

        report.print_summary()
        return results, report

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _provision_single(self, table: DiscoveredTable) -> ProvisioningResult:
        """Create or update a single monitor with retry logic."""
        try:
            # Pre-flight: validate columns exist
            self._validate_columns_exist(table)

            table_info = self._get_table_info(table.full_name)
            if not table_info:
                return ProvisioningResult(
                    table_name=table.full_name,
                    action="failed",
                    success=False,
                    error_message="Table not found",
                )

            schema_info = self._get_schema_info()
            if not schema_info:
                return ProvisioningResult(
                    table_name=table.full_name,
                    action="failed",
                    success=False,
                    error_message="Output schema not found",
                )

            safe_slicing = self._validate_slicing_columns(table)
            existing = self._get_existing_monitor(table)
            config = self._build_data_profiling_config(
                table, schema_info.schema_id, safe_slicing
            )
            config_hash = hash_config(self._build_config_dict(table))

            monitor_obj = Monitor(
                object_type="table",
                object_id=table_info.table_id,
                data_profiling_config=config,
            )

            if existing:
                if not self._has_config_drift(existing, table):
                    logger.info(
                        "No config changes for %s; skipping update", table.full_name
                    )
                    return ProvisioningResult(
                        table_name=table.full_name,
                        action="no_change",
                        success=True,
                        config_hash=config_hash,
                    )

                self.w.data_quality.update_monitor(
                    object_type="table",
                    object_id=table_info.table_id,
                    monitor=monitor_obj,
                    update_mask="data_profiling_config",
                )
                logger.info("Updated monitor for %s", table.full_name)

                return ProvisioningResult(
                    table_name=table.full_name,
                    action="updated",
                    success=True,
                    config_hash=config_hash,
                )
            else:
                created = self.w.data_quality.create_monitor(monitor=monitor_obj)
                logger.info("Created monitor for %s", table.full_name)

                return ProvisioningResult(
                    table_name=table.full_name,
                    action="created",
                    success=True,
                    monitor_id=getattr(created, "monitor_id", None),
                    config_hash=config_hash,
                )

        except ValueError as e:
            # Column validation errors
            logger.warning("Column validation failed for %s: %s", table.full_name, e)
            return ProvisioningResult(
                table_name=table.full_name,
                action="skipped_column_missing",
                success=False,
                error_message=str(e),
            )
        except Exception as e:
            error_msg = str(e).lower()
            if "limit" in error_msg or "quota" in error_msg:
                return ProvisioningResult(
                    table_name=table.full_name,
                    action="skipped_quota",
                    success=False,
                    error_message="Monitor quota reached",
                )
            raise

    def _build_data_profiling_config(
        self,
        table: DiscoveredTable,
        schema_id: str,
        slicing_exprs: List[str],
    ) -> DataProfilingConfig:
        """Build DataProfilingConfig based on profile_type with per-table resolution.

        Args:
            table: The discovered table to configure.
            schema_id: Output schema ID for metric tables.
            slicing_exprs: Validated slicing expressions.

        Returns:
            Configured DataProfilingConfig for the appropriate profile type.
        """
        profile_type = self.config.profile_defaults.profile_type

        granularities_cfg = self._resolve_setting(table, "granularities")
        if granularities_cfg:
            resolved_granularities = [
                self.GRANULARITY_MAP.get(g, AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY)
                for g in granularities_cfg
            ]
        else:
            granularity_str = self._resolve_setting(table, "granularity")
            resolved_granularities = [
                self.GRANULARITY_MAP.get(
                    granularity_str,
                    AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY,
                )
            ]

        assets_dir = (
            f"/Workspace/Users/{self.username}/dpo_monitoring/{table.table_name}"
        )
        skip_builtin_dashboard = not self.config.profile_defaults.create_builtin_dashboard

        baseline_table = self._resolve_setting(table, "baseline_table_name")
        custom_metrics = self._build_custom_metrics(table)

        if profile_type == "SNAPSHOT":
            return DataProfilingConfig(
                output_schema_id=schema_id,
                assets_dir=assets_dir,
                skip_builtin_dashboard=skip_builtin_dashboard,
                snapshot=SnapshotConfig(),
                slicing_exprs=slicing_exprs if slicing_exprs else None,
                baseline_table_name=baseline_table,
                custom_metrics=custom_metrics or None,
            )

        elif profile_type == "TIMESERIES":
            ts_timestamp = self._resolve_setting(table, "timestamp_column")
            if not ts_timestamp:
                ts_timestamp = self.config.profile_defaults.timeseries_timestamp_column
            return DataProfilingConfig(
                output_schema_id=schema_id,
                assets_dir=assets_dir,
                skip_builtin_dashboard=skip_builtin_dashboard,
                time_series=TimeSeriesConfig(
                    timestamp_column=ts_timestamp,
                    granularities=resolved_granularities,
                ),
                slicing_exprs=slicing_exprs if slicing_exprs else None,
                baseline_table_name=baseline_table,
                custom_metrics=custom_metrics or None,
            )

        else:  # INFERENCE
            problem_type = self._resolve_problem_type(table)
            prediction_col = self._resolve_setting(table, "prediction_column")
            timestamp_col = self._resolve_setting(table, "timestamp_column")
            label_col = self._resolve_setting(table, "label_column")
            model_id_col = self._resolve_setting(table, "model_id_column")

            return DataProfilingConfig(
                output_schema_id=schema_id,
                assets_dir=assets_dir,
                skip_builtin_dashboard=skip_builtin_dashboard,
                inference_log=InferenceLogConfig(
                    problem_type=problem_type,
                    prediction_column=prediction_col,
                    timestamp_column=timestamp_col,
                    label_column=label_col,
                    model_id_column=model_id_col,
                    granularities=resolved_granularities,
                ),
                slicing_exprs=slicing_exprs if slicing_exprs else None,
                baseline_table_name=baseline_table,
                custom_metrics=custom_metrics or None,
            )

    METRIC_TYPE_MAP = {
        "aggregate": DataProfilingCustomMetricType.DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE,
        "derived": DataProfilingCustomMetricType.DATA_PROFILING_CUSTOM_METRIC_TYPE_DERIVED,
        "drift": DataProfilingCustomMetricType.DATA_PROFILING_CUSTOM_METRIC_TYPE_DRIFT,
    }

    def _build_custom_metrics(self, table: DiscoveredTable) -> List[DataProfilingCustomMetric]:
        """Build SDK custom metric objects using three-tier merge.

        Resolution order: per-table custom_metrics override objective function
        metrics override profile_defaults. Cross-layer name collisions are
        intentional overrides.

        Args:
            table: The discovered table to resolve metrics for.

        Returns:
            List of DataProfilingCustomMetric to embed in DataProfilingConfig.
        """
        # Layer 1: profile_defaults.custom_metrics
        merged = {m.name: m for m in (self.config.profile_defaults.custom_metrics or [])}

        # Layer 2: resolved objective_functions
        table_config = self.config.monitored_tables.get(table.full_name)
        if table_config and table_config.objective_function_ids:
            for obj_id in table_config.objective_function_ids:
                obj_func = self.config.objective_functions[obj_id]
                merged[obj_func.metric.name] = obj_func.metric

        # Layer 3: per-table custom_metrics (highest priority)
        if table_config and table_config.custom_metrics:
            for m in table_config.custom_metrics:
                merged[m.name] = m

        return self._convert_to_sdk_metrics(list(merged.values()))

    def _convert_to_sdk_metrics(self, metrics: List) -> List[DataProfilingCustomMetric]:
        """Convert CustomMetricConfig list to SDK metric objects.

        Args:
            metrics: List of CustomMetricConfig instances.

        Returns:
            List of DataProfilingCustomMetric SDK objects.
        """
        sdk_metrics = []
        for metric in metrics:
            metric_type = self.METRIC_TYPE_MAP.get(metric.metric_type)
            if metric_type is None:
                logger.warning("Unknown metric type '%s', skipping", metric.metric_type)
                continue
            sdk_metrics.append(
                DataProfilingCustomMetric(
                    name=metric.name,
                    type=metric_type,
                    input_columns=metric.input_columns,
                    definition=metric.definition,
                    output_data_type=metric.output_type,
                )
            )
        return sdk_metrics

    def _build_config_dict(self, table: DiscoveredTable) -> dict:
        """Build config dict for hashing."""
        profile_type = self.config.profile_defaults.profile_type
        if profile_type == "SNAPSHOT":
            granularities = None
        else:
            multi = self._resolve_setting(table, "granularities")
            if multi:
                granularities = sorted(multi)
            else:
                single = self._resolve_setting(table, "granularity")
                granularities = [single] if single else None

        base_config = {
            "profile_type": profile_type,
            "output_schema_name": self.output_schema,
            "granularities": granularities,
            "slicing_exprs": self._resolve_setting(table, "slicing_exprs"),
            "baseline_table_name": self._resolve_setting(
                table, "baseline_table_name"
            ),
            "custom_metrics": self._serialize_resolved_custom_metrics(table),
        }

        if profile_type == "INFERENCE":
            base_config.update({
                "problem_type": self._resolve_problem_type(table).value,
                "prediction_column": self._resolve_setting(table, "prediction_column"),
                "label_column": self._resolve_setting(table, "label_column"),
                "timestamp_column": self._resolve_setting(table, "timestamp_column"),
                "model_id_column": self._resolve_setting(table, "model_id_column"),
            })
        elif profile_type == "TIMESERIES":
            ts_col = self._resolve_setting(table, "timestamp_column")
            if not ts_col:
                ts_col = self.config.profile_defaults.timeseries_timestamp_column
            base_config["timestamp_column"] = ts_col

        return base_config

    def _serialize_resolved_custom_metrics(self, table: DiscoveredTable) -> List[Dict[str, Any]]:
        """Serialize resolved custom metrics (three-tier merge) for comparisons.

        Args:
            table: The discovered table to resolve metrics for.

        Returns:
            List of serialized metric dicts.
        """
        merged = {m.name: m for m in (self.config.profile_defaults.custom_metrics or [])}

        table_config = self.config.monitored_tables.get(table.full_name)
        if table_config and table_config.objective_function_ids:
            for obj_id in table_config.objective_function_ids:
                obj_func = self.config.objective_functions[obj_id]
                merged[obj_func.metric.name] = obj_func.metric

        if table_config and table_config.custom_metrics:
            for m in table_config.custom_metrics:
                merged[m.name] = m

        return [
            {
                "name": metric.name,
                "metric_type": metric.metric_type,
                "input_columns": metric.input_columns,
                "definition": metric.definition,
                "output_type": metric.output_type,
            }
            for metric in merged.values()
        ]

    def _normalize_granularity(self, granularity: Any) -> Optional[str]:
        """Normalize SDK granularity enum/string to config string."""
        if granularity is None:
            return None
        if isinstance(granularity, str):
            if granularity in self.GRANULARITY_MAP:
                return granularity
            enum_to_name = {v.value: k for k, v in self.GRANULARITY_MAP.items()}
            return enum_to_name.get(granularity)
        enum_value = getattr(granularity, "value", None)
        if enum_value:
            enum_to_name = {v.value: k for k, v in self.GRANULARITY_MAP.items()}
            return enum_to_name.get(enum_value)
        return None

    def _extract_existing_config(self, monitor: Monitor) -> Dict[str, Any]:
        """Extract config fields from an existing monitor for drift checks."""
        if not monitor.data_profiling_config:
            return {}

        cfg = monitor.data_profiling_config
        profile_type = "SNAPSHOT"
        if cfg.inference_log:
            profile_type = "INFERENCE"
        elif cfg.time_series:
            profile_type = "TIMESERIES"

        existing = {
            "profile_type": profile_type,
            "output_schema_name": self.output_schema,
            "granularities": None,
            "slicing_exprs": cfg.slicing_exprs or None,
            "baseline_table_name": cfg.baseline_table_name,
            "custom_metrics": [],
        }

        if cfg.custom_metrics:
            metric_type_map = {
                "DATA_PROFILING_CUSTOM_METRIC_TYPE_AGGREGATE": "aggregate",
                "DATA_PROFILING_CUSTOM_METRIC_TYPE_DERIVED": "derived",
                "DATA_PROFILING_CUSTOM_METRIC_TYPE_DRIFT": "drift",
            }
            existing["custom_metrics"] = [
                {
                    "name": m.name,
                    "metric_type": metric_type_map.get(
                        getattr(m.type, "value", str(m.type)), str(m.type).lower()
                    ),
                    "input_columns": m.input_columns or [],
                    "definition": m.definition,
                    "output_type": m.output_data_type,
                }
                for m in cfg.custom_metrics
            ]

        if profile_type == "INFERENCE" and cfg.inference_log:
            inference = cfg.inference_log
            existing["problem_type"] = (
                inference.problem_type.value
                if inference.problem_type else None
            )
            existing["prediction_column"] = inference.prediction_column
            existing["label_column"] = inference.label_column
            existing["timestamp_column"] = inference.timestamp_column
            existing["model_id_column"] = inference.model_id_column
            if inference.granularities:
                existing["granularities"] = sorted(
                    g for g in (
                        self._normalize_granularity(g)
                        for g in inference.granularities
                    ) if g
                ) or None
        elif profile_type == "TIMESERIES" and cfg.time_series:
            series = cfg.time_series
            existing["timestamp_column"] = series.timestamp_column
            if series.granularities:
                existing["granularities"] = sorted(
                    g for g in (
                        self._normalize_granularity(g)
                        for g in series.granularities
                    ) if g
                ) or None
        return existing

    def _has_config_drift(self, existing_monitor: Monitor, table: DiscoveredTable) -> bool:
        """Compare desired monitor config against existing monitor config."""
        try:
            existing_config = self._extract_existing_config(existing_monitor)
            desired_config = self._build_config_dict(table)
            return bool(calculate_config_diff(existing_config, desired_config))
        except Exception as exc:
            logger.warning(
                "Failed to compare monitor config for %s (%s); assuming drift",
                table.full_name,
                exc,
            )
            return True

    def _resolve_problem_type(self, table: DiscoveredTable) -> InferenceProblemType:
        """Resolve problem type: monitored_tables > UC tag > profile_defaults > auto-detect."""
        # 1. Check monitored_tables config
        table_config = self.config.monitored_tables.get(table.full_name)
        if table_config and table_config.problem_type:
            if "REGRESSION" in table_config.problem_type.upper():
                return InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION
            return InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION

        # 2. Check UC tag
        tag_value = table.tags.get("monitor_problem_type")
        if tag_value:
            if "REGRESSION" in tag_value.upper():
                return InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION
            return InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION

        # 3. Check profile_defaults
        default_type = self.config.profile_defaults.problem_type
        if default_type:
            if "REGRESSION" in default_type.upper():
                return InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION
            return InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION

        # 4. Auto-detect from label column type
        label_col = self._resolve_setting(table, "label_column")
        if label_col:
            for col in table.columns:
                if col.name.lower() == label_col.lower():
                    col_type = col.type_text.upper() if col.type_text else ""
                    if any(
                        t in col_type
                        for t in ["DOUBLE", "FLOAT", "DECIMAL", "INT", "LONG"]
                    ):
                        return InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION

        return InferenceProblemType.INFERENCE_PROBLEM_TYPE_CLASSIFICATION

    def _validate_slicing_columns(self, table: DiscoveredTable) -> List[str]:
        """Filter out high-cardinality columns to prevent cost explosion.

        Args:
            table: The table to validate slicing columns for.

        Returns:
            List of quoted column names safe for slicing.
        """
        candidate_columns = self._resolve_setting(table, "slicing_exprs") or []
        table_column_names = [col.name.lower() for col in table.columns]
        safe_columns = []

        for col_name in candidate_columns:
            if col_name.lower() not in table_column_names:
                logger.debug(
                    "Slicing column '%s' not found in %s", col_name, table.full_name
                )
                continue

            try:
                cardinality = self._get_approx_cardinality(table.full_name, col_name)
                max_cardinality = self.config.profile_defaults.max_slicing_cardinality

                if cardinality > max_cardinality:
                    logger.warning(
                        "Skipping slicing column '%s' on %s: "
                        "cardinality %d exceeds threshold %d",
                        col_name,
                        table.full_name,
                        cardinality,
                        max_cardinality,
                    )
                    continue

                safe_columns.append(col_name)

            except Exception as e:
                logger.warning("Failed to check cardinality for %s: %s", col_name, e)
                safe_columns.append(col_name)

        return [f"`{col}`" for col in safe_columns]

    def _get_approx_cardinality(self, table_name: str, column: str) -> int:
        """Use APPROX_COUNT_DISTINCT for fast cardinality check."""
        result = self.w.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=f"SELECT APPROX_COUNT_DISTINCT(`{column}`) FROM {table_name}",
            wait_timeout="30s",
        )
        if result.result and result.result.data_array:
            return int(result.result.data_array[0][0])
        return 0

    def _get_monitor_count(self) -> int:
        """Get current number of monitors."""
        try:
            monitors = list(self.w.data_quality.list_monitor())
            return len(monitors)
        except Exception:
            return 0

    def _get_existing_monitor(self, table: DiscoveredTable) -> Optional[Monitor]:
        """Check if a monitor already exists for this table."""
        try:
            table_info = self._get_table_info(table.full_name)
            if table_info:
                return self.w.data_quality.get_monitor(
                    object_type="table", object_id=table_info.table_id
                )
        except Exception:
            pass
        return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get_table_info(self, full_name: str) -> Optional[TableInfo]:
        """Get table info with retry."""
        try:
            return self.w.tables.get(full_name=full_name)
        except Exception:
            return None

    def _get_schema_info(self):
        """Get schema info for output schema."""
        try:
            return self.w.schemas.get(
                full_name=f"{self.catalog}.{self.output_schema}"
            )
        except Exception:
            return None

    def cleanup_orphans(
        self, discovered_tables: List[DiscoveredTable]
    ) -> List[str]:
        """Find and optionally disable monitors no longer tagged for monitoring.

        Args:
            discovered_tables: Currently discovered tables with monitor tags.

        Returns:
            List of orphaned table names.
        """
        discovered_names = {t.full_name for t in discovered_tables}
        orphans = []

        try:
            all_monitors = list(self.w.data_quality.list_monitor())

            for monitor in all_monitors:
                table_name = self._get_table_name_from_monitor(monitor)
                if table_name and table_name not in discovered_names:
                    orphans.append(table_name)

                    if self.config.cleanup_orphans:
                        logger.warning("Deleting orphaned monitor: %s", table_name)
                        try:
                            self.w.data_quality.delete_monitor(
                                object_type="table", object_id=monitor.object_id
                            )
                        except Exception as e:
                            logger.error("Failed to delete orphaned monitor: %s", e)
                    else:
                        logger.info(
                            "Orphaned monitor found (not deleted): %s", table_name
                        )

        except Exception as e:
            logger.error("Failed to list monitors for orphan cleanup: %s", e)

        return orphans

    def _get_table_name_from_monitor(self, monitor: Monitor) -> Optional[str]:
        """Extract table name from monitor object."""
        try:
            if monitor.object_id:
                table_info = self.w.tables.get(table_id=monitor.object_id)
                return table_info.full_name
        except Exception:
            pass
        return None

    def refresh_all(
        self, tables: List[DiscoveredTable], wait: bool = False
    ) -> List["RefreshResult"]:
        """Trigger a refresh for all monitored tables.

        Args:
            tables: List of discovered tables to refresh.
            wait: If True, wait for each refresh to complete before returning.

        Returns:
            List of RefreshResult with status for each table.
        """
        results = []

        for table in tables:
            result = self._refresh_single(table, wait=wait)
            results.append(result)

        successful = sum(1 for r in results if r.status == "triggered")
        failed = sum(1 for r in results if r.status == "failed")
        logger.info(f"Refresh triggered: {successful} successful, {failed} failed")

        return results

    def _refresh_single(
        self, table: DiscoveredTable, wait: bool = False
    ) -> "RefreshResult":
        """Trigger refresh for a single table."""
        try:
            table_info = self.w.tables.get(full_name=table.full_name)
            table_id = table_info.table_id

            run_info = self.w.data_quality.create_refresh(
                object_type="table",
                object_id=table_id,
                refresh=Refresh(
                    object_type="table",
                    object_id=table_id,
                ),
            )

            if wait:
                import time

                while run_info.state in (
                    RefreshState.MONITOR_REFRESH_STATE_PENDING,
                    RefreshState.MONITOR_REFRESH_STATE_RUNNING,
                ):
                    time.sleep(10)
                    run_info = self.w.data_quality.get_refresh(
                        object_type="table",
                        object_id=table_id,
                        refresh_id=run_info.refresh_id,
                    )

                if run_info.state == RefreshState.MONITOR_REFRESH_STATE_SUCCESS:
                    return RefreshResult(
                        table_name=table.full_name,
                        status="completed",
                        refresh_id=run_info.refresh_id,
                    )
                else:
                    return RefreshResult(
                        table_name=table.full_name,
                        status="failed",
                        error=f"Refresh ended with state: {run_info.state}",
                    )

            return RefreshResult(
                table_name=table.full_name,
                status="triggered",
                refresh_id=run_info.refresh_id,
            )

        except Exception as e:
            logger.error(f"Failed to refresh {table.full_name}: {e}")
            return RefreshResult(
                table_name=table.full_name,
                status="failed",
                error=str(e),
            )


@dataclass
class RefreshResult:
    """Result of a monitor refresh operation."""

    table_name: str
    status: Literal["triggered", "completed", "failed"]
    refresh_id: Optional[str] = None
    error: Optional[str] = None


@dataclass
class MonitorStatus:
    """Status of a data profiling monitor."""

    table_name: str
    status: str
    error_message: Optional[str] = None


def get_monitor_statuses(
    w: WorkspaceClient, tables: List[DiscoveredTable]
) -> List[MonitorStatus]:
    """Get the current status of monitors for all discovered tables.

    Args:
        w: Databricks WorkspaceClient.
        tables: List of discovered tables to check.

    Returns:
        List of MonitorStatus for each table.
    """

    statuses = []

    for table in tables:
        try:
            table_info = w.tables.get(full_name=table.full_name)
            monitor = w.data_quality.get_monitor(
                object_type="table", object_id=table_info.table_id
            )

            if monitor.data_profiling_config:
                config = monitor.data_profiling_config
                status_value = config.status.value if config.status else "UNKNOWN"
                error_msg = config.latest_monitor_failure_message
                statuses.append(
                    MonitorStatus(
                        table_name=table.full_name,
                        status=status_value,
                        error_message=error_msg,
                    )
                )
            else:
                statuses.append(
                    MonitorStatus(table_name=table.full_name, status="NO_PROFILE")
                )

        except Exception as e:
            statuses.append(
                MonitorStatus(
                    table_name=table.full_name,
                    status="ERROR",
                    error_message=str(e),
                )
            )

    return statuses


def wait_for_monitors(
    w: WorkspaceClient,
    tables: List[DiscoveredTable],
    timeout_seconds: int = 600,
    poll_interval: int = 10,
) -> List[MonitorStatus]:
    """Wait for all monitors to reach ACTIVE status or fail.

    Args:
        w: Databricks WorkspaceClient.
        tables: List of discovered tables to wait for.
        timeout_seconds: Maximum time to wait (default 10 minutes).
        poll_interval: Seconds between status checks (default 10).

    Returns:
        Final list of MonitorStatus for each table.
    """
    import time

    from databricks.sdk.service.dataquality import DataProfilingStatus

    start_time = time.time()
    pending_tables = set(t.full_name for t in tables)

    while pending_tables and (time.time() - start_time) < timeout_seconds:
        statuses = get_monitor_statuses(w, tables)

        pending_tables = set()
        for status in statuses:
            if status.status == DataProfilingStatus.DATA_PROFILING_STATUS_PENDING.value:
                pending_tables.add(status.table_name)

        if pending_tables:
            logger.info(
                f"Waiting for {len(pending_tables)} monitors... "
                f"({int(time.time() - start_time)}s elapsed)"
            )
            time.sleep(poll_interval)

    final_statuses = get_monitor_statuses(w, tables)

    active_count = sum(
        1
        for s in final_statuses
        if s.status == DataProfilingStatus.DATA_PROFILING_STATUS_ACTIVE.value
    )
    failed_count = sum(
        1
        for s in final_statuses
        if s.status
        in (
            DataProfilingStatus.DATA_PROFILING_STATUS_FAILED.value,
            DataProfilingStatus.DATA_PROFILING_STATUS_ERROR.value,
        )
    )
    pending_count = sum(
        1
        for s in final_statuses
        if s.status == DataProfilingStatus.DATA_PROFILING_STATUS_PENDING.value
    )

    logger.info(
        f"Monitor status: {active_count} active, {failed_count} failed, "
        f"{pending_count} still pending"
    )

    return final_statuses


def print_monitor_statuses(statuses: List[MonitorStatus]) -> None:
    """Print a formatted table of monitor statuses.

    Args:
        statuses: List of MonitorStatus to display.
    """
    table_data = []
    for s in statuses:
        table_data.append(
            [s.table_name, s.status, s.error_message or ""]
        )

    print(
        tabulate(
            table_data,
            headers=["Table", "Status", "Error"],
            tablefmt="simple",
        )
    )
