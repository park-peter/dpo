#!/usr/bin/env bash
# =============================================================================
# Databricks Auth Preflight for Downstream CI
#
# Copy this script into your deployment repo's CI pipeline to verify that
# Databricks credentials are configured correctly before running DPO commands.
#
# This is NOT used by the databricks-dpo library CI itself (which only runs
# unit tests against mocked APIs). It's a reference for teams that deploy
# DPO in their own CI/CD pipelines.
#
# Required environment variables (set one auth method):
#   PAT auth:       DATABRICKS_HOST, DATABRICKS_TOKEN
#   OAuth M2M auth: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
#
# Usage:
#   bash ci_preflight_databricks_auth.sh
# =============================================================================
set -euo pipefail

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ -n "${DATABRICKS_HOST:-}" ]] || fail "DATABRICKS_HOST is required."

auth_mode=""
if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
  auth_mode="pat"
fi

if [[ -n "${DATABRICKS_CLIENT_ID:-}" || -n "${DATABRICKS_CLIENT_SECRET:-}" ]]; then
  [[ -n "${DATABRICKS_CLIENT_ID:-}" ]] || fail "DATABRICKS_CLIENT_ID is required for OAuth M2M auth."
  [[ -n "${DATABRICKS_CLIENT_SECRET:-}" ]] || fail "DATABRICKS_CLIENT_SECRET is required for OAuth M2M auth."
  if [[ -n "${auth_mode}" ]]; then
    echo "INFO: Both PAT and OAuth credentials are present; Databricks SDK default resolution order will apply."
  else
    auth_mode="oauth-m2m"
  fi
fi

[[ -n "${auth_mode}" ]] || fail "Set DATABRICKS_TOKEN or both DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET."

echo "Databricks auth environment looks valid (mode: ${auth_mode}). Verifying workspace connectivity..."
python - <<'PY'
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()
principal = getattr(me, "user_name", None) or getattr(me, "id", None) or "unknown"
print(f"Databricks auth preflight OK. Authenticated principal: {principal}")
PY
