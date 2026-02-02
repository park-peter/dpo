"""Tests for DPO utils module."""

import pytest
from unittest.mock import MagicMock, patch

from dpo.utils import (
    hash_config,
    calculate_config_diff,
    verify_output_schema_permissions,
    safe_get_table_id,
    safe_get_schema_id,
    format_duration,
    sanitize_sql_identifier,
    MAX_SQL_IDENTIFIER_LENGTH,
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

    def test_format_seconds(self):
        """Test seconds formatting."""
        assert format_duration(30.5) == "30.5s"
        assert format_duration(0.5) == "0.5s"

    def test_format_minutes(self):
        """Test minutes formatting."""
        assert format_duration(120) == "2.0m"
        assert format_duration(90) == "1.5m"

    def test_format_hours(self):
        """Test hours formatting."""
        assert format_duration(3600) == "1.0h"
        assert format_duration(7200) == "2.0h"

    def test_boundary_conditions(self):
        """Test boundary conditions."""
        assert "s" in format_duration(59)
        assert "m" in format_duration(60)
        assert "m" in format_duration(3599)
        assert "h" in format_duration(3600)


class TestSanitizeSqlIdentifier:
    """Tests for SQL identifier sanitization."""

    def test_simple_alphanumeric(self):
        """Test simple alphanumeric names pass through."""
        assert sanitize_sql_identifier("ml_team") == "ml_team"
        assert sanitize_sql_identifier("team123") == "team123"

    def test_converts_to_lowercase(self):
        """Test names are converted to lowercase."""
        assert sanitize_sql_identifier("ML_Team") == "ml_team"
        assert sanitize_sql_identifier("DataEng") == "dataeng"

    def test_replaces_special_characters(self):
        """Test special characters are replaced with underscores."""
        assert sanitize_sql_identifier("Marketing & Sales") == "marketing_sales"
        assert sanitize_sql_identifier("North America (NA)") == "north_america_na"
        assert sanitize_sql_identifier("Team-123") == "team_123"

    def test_collapses_multiple_underscores(self):
        """Test multiple underscores are collapsed into one."""
        assert sanitize_sql_identifier("ml___team") == "ml_team"
        assert sanitize_sql_identifier("a  &  b") == "a_b"

    def test_strips_leading_trailing_underscores(self):
        """Test leading/trailing underscores are stripped."""
        assert sanitize_sql_identifier("_team_") == "team"
        assert sanitize_sql_identifier("___ml___") == "ml"

    def test_empty_input(self):
        """Test empty or whitespace input returns 'unnamed'."""
        assert sanitize_sql_identifier("") == "unnamed"
        assert sanitize_sql_identifier("   ") == "unnamed"

    def test_special_chars_only(self):
        """Test input with only special chars returns 'unnamed'."""
        assert sanitize_sql_identifier("!@#$%") == "unnamed"
        assert sanitize_sql_identifier("...") == "unnamed"

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
