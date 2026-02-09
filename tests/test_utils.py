"""Tests for DPO utils module."""

from unittest.mock import MagicMock

import pytest

from dpo.utils import (
    MAX_SQL_IDENTIFIER_LENGTH,
    api_call_with_retry,
    calculate_config_diff,
    create_retry_wrapper,
    format_duration,
    hash_config,
    safe_get_schema_id,
    safe_get_table_id,
    sanitize_sql_identifier,
    verify_output_schema_permissions,
    verify_view_permissions,
)


class TestHashConfig:
    """Tests for config hashing."""

    def test_deterministic_hash(self):
        """Test hash is deterministic."""
        config = {
            "output_schema_name": "monitoring",
            "granularity": "1 day",
            "problem_type": "CLASSIFICATION",
        }

        hash1 = hash_config(config)
        hash2 = hash_config(config)

        assert hash1 == hash2

    def test_different_configs_different_hashes(self):
        """Test different configs produce different hashes."""
        config1 = {"output_schema_name": "monitoring", "granularity": "1 day"}
        config2 = {"output_schema_name": "monitoring", "granularity": "1 hour"}

        hash1 = hash_config(config1)
        hash2 = hash_config(config2)

        assert hash1 != hash2

    def test_ignores_uncontrolled_fields(self):
        """Test hash ignores fields not in controlled_fields set."""
        config1 = {
            "output_schema_name": "monitoring",
            "granularity": "1 day",
            "unknown_field": "value1",
        }
        config2 = {
            "output_schema_name": "monitoring",
            "granularity": "1 day",
            "unknown_field": "value2",
        }

        hash1 = hash_config(config1)
        hash2 = hash_config(config2)

        # Should be equal since unknown_field is not in controlled_fields
        assert hash1 == hash2

    def test_hash_length(self):
        """Test hash is 16 characters (truncated sha256)."""
        config = {"output_schema_name": "test"}

        h = hash_config(config)

        assert len(h) == 16

    def test_handles_nested_structures(self):
        """Test hash handles nested structures."""
        config = {
            "slicing_columns": ["col1", "col2"],
            "inference_log": {"prediction_col": "pred"},
        }

        # Should not raise
        h = hash_config(config)
        assert len(h) == 16


class TestCalculateConfigDiff:
    """Tests for config diff calculation."""

    def test_identical_configs_no_diff(self):
        """Test identical configs produce no diff."""
        config1 = {"granularity": "1 day", "schema": "test"}
        config2 = {"granularity": "1 day", "schema": "test"}

        diff = calculate_config_diff(config1, config2)

        assert len(diff) == 0

    def test_different_values_in_diff(self):
        """Test different values appear in diff."""
        config1 = {"granularity": "1 day", "schema": "test"}
        config2 = {"granularity": "1 hour", "schema": "test"}

        diff = calculate_config_diff(config1, config2)

        assert "granularity" in diff
        assert diff["granularity"] == ("1 day", "1 hour")

    def test_ignores_system_fields(self):
        """Test system fields are ignored."""
        config1 = {
            "granularity": "1 day",
            "monitor_id": "old_id",
            "created_at": "2024-01-01",
            "status": "ACTIVE",
        }
        config2 = {
            "granularity": "1 day",
            "monitor_id": "new_id",
            "created_at": "2024-02-01",
            "status": "PENDING",
        }

        diff = calculate_config_diff(config1, config2)

        # Should be empty since only system fields changed
        assert len(diff) == 0

    def test_new_fields_in_diff(self):
        """Test new fields appear in diff."""
        config1 = {"granularity": "1 day"}
        config2 = {"granularity": "1 day", "new_field": "value"}

        diff = calculate_config_diff(config1, config2)

        assert "new_field" in diff
        assert diff["new_field"] == (None, "value")


class TestVerifyOutputSchemaPermissions:
    """Tests for permission verification."""

    def test_permissions_verified_successfully(self, mock_workspace_client):
        """Test successful permission verification."""
        # Should not raise
        verify_output_schema_permissions(
            mock_workspace_client,
            catalog="test_catalog",
            schema="test_schema",
            warehouse_id="warehouse_123",
        )

    def test_creates_schema_if_missing(self, mock_workspace_client):
        """Test schema creation when it doesn't exist."""
        mock_workspace_client.schemas.get.side_effect = Exception("Not found")

        verify_output_schema_permissions(
            mock_workspace_client,
            catalog="test_catalog",
            schema="new_schema",
            warehouse_id="warehouse_123",
        )

        mock_workspace_client.schemas.create.assert_called_once()

    def test_raises_permission_error_on_failure(self, mock_workspace_client):
        """Test PermissionError raised on access denied."""
        result = MagicMock()
        result.status.state.value = "FAILED"
        result.status.error = MagicMock()
        result.status.error.message = "Permission denied"
        mock_workspace_client.statement_execution.execute_statement.return_value = result

        with pytest.raises(PermissionError):
            verify_output_schema_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="test_schema",
                warehouse_id="warehouse_123",
            )

    def test_raises_permission_error_on_access_exception(self, mock_workspace_client):
        """Test PermissionError raised on access-related exception."""
        mock_workspace_client.statement_execution.execute_statement.side_effect = (
            Exception("Access denied to resource")
        )

        with pytest.raises(PermissionError) as exc_info:
            verify_output_schema_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="test_schema",
                warehouse_id="warehouse_123",
            )

        assert "FATAL" in str(exc_info.value)

    def test_non_permission_exception_is_re_raised(self, mock_workspace_client):
        """Test non-permission failures are not wrapped as PermissionError."""
        mock_workspace_client.statement_execution.execute_statement.side_effect = (
            RuntimeError("warehouse not running")
        )

        with pytest.raises(RuntimeError, match="warehouse not running"):
            verify_output_schema_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="test_schema",
                warehouse_id="warehouse_123",
            )


class TestVerifyViewPermissions:
    """Tests for dedicated CREATE VIEW permission verification."""

    def test_verify_view_permissions_success(self, mock_workspace_client):
        """Test view permission check succeeds and cleans up test view."""
        verify_view_permissions(
            mock_workspace_client,
            catalog="test_catalog",
            schema="global_monitoring",
            warehouse_id="warehouse_123",
        )

        sql = [
            c.kwargs.get("statement", "")
            for c in mock_workspace_client.statement_execution.execute_statement.call_args_list
        ]
        assert any("CREATE VIEW" in s for s in sql)
        assert any("DROP VIEW" in s for s in sql)

    def test_verify_view_permissions_creates_schema_if_missing(self, mock_workspace_client):
        """Test view check creates schema if it does not exist."""
        mock_workspace_client.schemas.get.side_effect = Exception("schema missing")

        verify_view_permissions(
            mock_workspace_client,
            catalog="test_catalog",
            schema="global_monitoring",
            warehouse_id="warehouse_123",
        )

        mock_workspace_client.schemas.create.assert_called_once_with(
            name="global_monitoring", catalog_name="test_catalog"
        )

    def test_verify_view_permissions_failed_statement_raises_permission_error(
        self, mock_workspace_client
    ):
        """FAILED statement status should raise PermissionError."""
        result = MagicMock()
        result.status.state.value = "FAILED"
        result.status.error = "Permission denied for CREATE VIEW"
        mock_workspace_client.statement_execution.execute_statement.return_value = result

        with pytest.raises(PermissionError):
            verify_view_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="global_monitoring",
                warehouse_id="warehouse_123",
            )

    def test_verify_view_permissions_access_exception_is_wrapped(
        self, mock_workspace_client
    ):
        """Access exceptions should be wrapped as PermissionError."""
        mock_workspace_client.statement_execution.execute_statement.side_effect = (
            Exception("Access denied to create view")
        )

        with pytest.raises(PermissionError, match="Cannot create views"):
            verify_view_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="global_monitoring",
                warehouse_id="warehouse_123",
            )

    def test_verify_view_permissions_non_permission_errors_are_re_raised(
        self, mock_workspace_client
    ):
        """Non-permission exceptions should be propagated unchanged."""
        mock_workspace_client.statement_execution.execute_statement.side_effect = (
            RuntimeError("warehouse unavailable")
        )

        with pytest.raises(RuntimeError, match="warehouse unavailable"):
            verify_view_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="global_monitoring",
                warehouse_id="warehouse_123",
            )


class TestRetryHelpers:
    """Tests for retry wrapper helpers."""

    def test_create_retry_wrapper_retries_and_succeeds(self):
        """Test decorator retries once and then returns successful result."""
        attempts = {"count": 0}

        @create_retry_wrapper(max_attempts=3, min_wait=0, max_wait=0)
        def flaky():
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("temporary")
            return "ok"

        assert flaky() == "ok"
        assert attempts["count"] == 2

    def test_api_call_with_retry_executes_callable(self):
        """Test api_call_with_retry delegates args/kwargs and returns value."""
        fn = MagicMock(return_value={"status": "ok"})

        result = api_call_with_retry(fn, 1, mode="fast")

        assert result == {"status": "ok"}
        fn.assert_called_once_with(1, mode="fast")


class TestSafeGetters:
    """Tests for safe getter functions."""

    def test_safe_get_table_id_success(self, mock_workspace_client):
        """Test successful table_id retrieval."""
        table_info = MagicMock()
        table_info.table_id = "table_123"
        mock_workspace_client.tables.get.return_value = table_info

        result = safe_get_table_id(mock_workspace_client, "cat.sch.tbl")

        assert result == "table_123"

    def test_safe_get_table_id_failure(self, mock_workspace_client):
        """Test table_id retrieval returns None on error."""
        mock_workspace_client.tables.get.side_effect = Exception("Not found")

        result = safe_get_table_id(mock_workspace_client, "cat.sch.tbl")

        assert result is None

    def test_safe_get_schema_id_success(self, mock_workspace_client):
        """Test successful schema_id retrieval."""
        schema_info = MagicMock()
        schema_info.schema_id = "schema_123"
        mock_workspace_client.schemas.get.return_value = schema_info

        result = safe_get_schema_id(mock_workspace_client, "cat", "sch")

        assert result == "schema_123"

    def test_safe_get_schema_id_failure(self, mock_workspace_client):
        """Test schema_id retrieval returns None on error."""
        mock_workspace_client.schemas.get.side_effect = Exception("Not found")

        result = safe_get_schema_id(mock_workspace_client, "cat", "sch")

        assert result is None


class TestFormatDuration:
    """Tests for duration formatting."""

    @pytest.mark.parametrize(
        "seconds,expected",
        [
            (30.5, "30.5s"),
            (0.5, "0.5s"),
            (120, "2.0m"),
            (90, "1.5m"),
            (3600, "1.0h"),
            (7200, "2.0h"),
        ],
    )
    def test_format_duration_values(self, seconds, expected):
        """Test representative seconds/minutes/hours formatting outputs."""
        assert format_duration(seconds) == expected

    def test_boundary_conditions(self):
        """Test boundary conditions."""
        assert "s" in format_duration(59)
        assert "m" in format_duration(60)
        assert "m" in format_duration(3599)
        assert "h" in format_duration(3600)


class TestSanitizeSqlIdentifier:
    """Tests for SQL identifier sanitization."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("ml_team", "ml_team"),
            ("team123", "team123"),
            ("ML_Team", "ml_team"),
            ("DataEng", "dataeng"),
            ("Marketing & Sales", "marketing_sales"),
            ("North America (NA)", "north_america_na"),
            ("Team-123", "team_123"),
            ("ml___team", "ml_team"),
            ("a  &  b", "a_b"),
            ("_team_", "team"),
            ("___ml___", "ml"),
            ("", "unnamed"),
            ("   ", "unnamed"),
            ("!@#$%", "unnamed"),
            ("...", "unnamed"),
        ],
    )
    def test_sanitize_transformations(self, raw, expected):
        """Test normalization rules for common identifier inputs."""
        assert sanitize_sql_identifier(raw) == expected

    def test_truncation(self):
        """Test long names are truncated."""
        long_name = "a" * 100
        result = sanitize_sql_identifier(long_name)
        assert len(result) <= MAX_SQL_IDENTIFIER_LENGTH

    def test_truncation_removes_trailing_underscore(self):
        """Test truncation doesn't leave trailing underscore."""
        # Create name that would end with underscore when truncated
        name = "a" * 63 + "_b"
        result = sanitize_sql_identifier(name, max_length=64)
        assert not result.endswith("_")

    def test_custom_max_length(self):
        """Test custom max length is respected."""
        result = sanitize_sql_identifier("abcdefghij", max_length=5)
        assert len(result) == 5
        assert result == "abcde"


class TestVerifyOutputSchemaPermissionsMode:
    """Tests for mode-aware permission verification."""

    def test_bulk_mode_skips_view_check(self, mock_workspace_client):
        """Test bulk mode only checks table creation."""
        verify_output_schema_permissions(
            mock_workspace_client,
            catalog="test_catalog",
            schema="test_schema",
            warehouse_id="warehouse_123",
            mode="bulk_provision_only",
        )

        # Should only create and drop table (2 calls), no view check
        calls = mock_workspace_client.statement_execution.execute_statement.call_args_list
        sql_statements = [call.kwargs.get("statement", "") for call in calls]

        # Should have CREATE TABLE and DROP TABLE, but no CREATE VIEW
        assert any("CREATE TABLE" in s for s in sql_statements)
        assert any("DROP TABLE" in s for s in sql_statements)

    def test_full_mode_checks_view(self, mock_workspace_client):
        """Test full mode checks both table and view creation."""
        verify_output_schema_permissions(
            mock_workspace_client,
            catalog="test_catalog",
            schema="test_schema",
            warehouse_id="warehouse_123",
            mode="full",
        )

        calls = mock_workspace_client.statement_execution.execute_statement.call_args_list
        sql_statements = [call.kwargs.get("statement", "") for call in calls]

        # Should have CREATE TABLE, DROP TABLE, CREATE VIEW, DROP VIEW
        assert any("CREATE TABLE" in s for s in sql_statements)
        assert any("CREATE VIEW" in s for s in sql_statements)

    def test_full_mode_raises_on_view_permission_failure(self, mock_workspace_client):
        """Test full mode raises error when view creation fails."""
        # First call succeeds (table), second fails (view)
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] <= 2:  # Table create and drop
                result.status.state.value = "SUCCEEDED"
            else:  # View create fails
                result.status.state.value = "FAILED"
                result.status.error = "Permission denied for CREATE VIEW"
            return result

        mock_workspace_client.statement_execution.execute_statement.side_effect = side_effect

        with pytest.raises(PermissionError) as exc_info:
            verify_output_schema_permissions(
                mock_workspace_client,
                catalog="test_catalog",
                schema="test_schema",
                warehouse_id="warehouse_123",
                mode="full",
            )

        assert "CREATE VIEW" in str(exc_info.value)
