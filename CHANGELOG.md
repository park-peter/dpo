# Changelog

All notable changes to **databricks-dpo** are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-09

### Added

- **CLI** (`dpo validate`, `dpo dry-run`, `dpo run`, `dpo coverage`) — safe-by-default `click`-based interface. `dpo run` requires explicit `--confirm` flag.
- **Policy governance** — configurable naming patterns, required tags, forbidden patterns, `require_baseline`, `require_slicing`, and `max_tables_per_config` via `PolicyConfig`.
- **Coverage governance** — `CoverageAnalyzer` detects unmonitored tables, stale monitors, and orphaned monitors. Catalog-scoped and discovery-policy-scoped.
- **Alert enrichment metadata** — `owner`, `runbook_url`, `lineage_url` fields on `MonitoredTableConfig` with UC tag fallback via `resolve_enrichment`.
- **Per-table drift thresholds** — `drift_threshold` field overrides global `alerting.drift_threshold`.
- **Enriched alert payloads** — unified views and SQL alert queries include `runbook_url` and `lineage_url` columns.
- **Coverage dashboard page** — Lakeview dashboards include a dedicated Coverage Governance page.
- **Structured ImpactReport** — dry-run returns a structured report with validation warnings, `to_dict()` for JSON output.
- **Pre-commit example** — `docs/examples/pre-commit-hooks.yaml` reference for downstream deployment repos.
- **Policy templates** — `configs/policies/default_policy.yaml` and `strict_policy.yaml`.
- **Module entry point** — `python -m dpo` via `__main__.py`.

### Changed

- Package renamed from `dpo` to `databricks-dpo` (import name `dpo` unchanged).
- `dry_run_all` now returns `tuple[List[ProvisioningResult], ImpactReport]` with column/slicing validation integrated into the dry-run path.
- `OrchestrationReport` now includes `impact_report` and `coverage_report` fields with `to_dict()` serialization.
- `DiscoveredTable` now carries `owner`, `runbook_url`, `lineage_url` resolved from config overrides or UC tags.
- Stale dashboard/view cleanup is skipped when `views_by_group` is empty to prevent destructive cleanup on transient failures.
- Alerting and dashboard steps in `run_orchestration` now require non-empty `views_by_group`.

### Fixed

- Coverage governance now filters monitors to the configured catalog only (no cross-catalog noise in coverage % or orphan detection).
- Coverage table enumeration respects `discovery.exclude_schemas` and `discovery.include_schemas`.
- `cleanup_stale_dashboards` and `cleanup_stale_views` guard against empty active-group sets.

## [0.1.0] - 2026-01-15

### Added

- Initial release with config-driven and tag-based table discovery.
- Monitor provisioning with create/update/skip/fail lifecycle.
- Unified drift and profile metric views per monitor group.
- SQL alerts with per-group notification routing.
- Lakeview dashboard generation per monitor group.
- Dry-run mode with impact preview.
- Schema validation (column existence checks).
- Monitor group segmentation via UC tags.
- `bulk_provision_only` and `full` operation modes.
