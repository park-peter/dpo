"""Tests for DPO discovery module."""

import pytest
from unittest.mock import MagicMock

from databricks.sdk.service.catalog import TableType

from dpo.config import DiscoveryConfig
from dpo.discovery import TableDiscovery, DiscoveredTable


class TestDiscoveredTable:
    """Tests for DiscoveredTable dataclass."""

    def test_properties(self):
        """Test computed properties."""
        table = DiscoveredTable(full_name="catalog.schema.table_name")
        
        assert table.catalog == "catalog"
        assert table.schema == "schema"
        assert table.table_name == "table_name"

    def test_default_values(self):
        """Test default values."""
        table = DiscoveredTable(full_name="cat.sch.tbl")
        
        assert table.tags == {}
        assert table.has_primary_key is False
        assert table.columns == []
        assert table.priority == 99

    def test_priority_sorting(self):
        """Test tables can be sorted by priority."""
        table1 = DiscoveredTable(full_name="cat.sch.tbl1", priority=10)
        table2 = DiscoveredTable(full_name="cat.sch.tbl2", priority=1)
        table3 = DiscoveredTable(full_name="cat.sch.tbl3", priority=5)
        
        tables = sorted([table1, table2, table3], key=lambda t: t.priority)
        
        assert tables[0].table_name == "tbl2"
        assert tables[1].table_name == "tbl3"
        assert tables[2].table_name == "tbl1"


class TestTableDiscovery:
    """Tests for TableDiscovery class."""

    @pytest.fixture
    def discovery_config(self):
        """Create a discovery configuration."""
        return DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            exclude_schemas=["information_schema", "tmp_*", "dev_*"],
        )

    @pytest.fixture
    def mock_client(self):
        """Create a mock workspace client for discovery."""
        client = MagicMock()
        return client

    def test_exclude_pattern_matching(self, mock_client, discovery_config):
        """Test schema exclusion pattern matching."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        
        assert discovery._matches_exclude_pattern("information_schema") is True
        assert discovery._matches_exclude_pattern("tmp_testing") is True
        assert discovery._matches_exclude_pattern("dev_sandbox") is True
        assert discovery._matches_exclude_pattern("production") is False
        assert discovery._matches_exclude_pattern("ml_models") is False

    def test_has_primary_key_by_name(self, mock_client, discovery_config):
        """Test primary key detection by column name."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        
        col_id = MagicMock()
        col_id.name = "id"
        assert discovery._has_primary_key([col_id]) is True
        
        col_pk = MagicMock()
        col_pk.name = "pk"
        assert discovery._has_primary_key([col_pk]) is True
        
        col_user_id = MagicMock()
        col_user_id.name = "user_id"
        assert discovery._has_primary_key([col_user_id]) is True
        
        col_model_id = MagicMock()
        col_model_id.name = "model_id"
        assert discovery._has_primary_key([col_model_id]) is False
        
        col_regular = MagicMock()
        col_regular.name = "value"
        assert discovery._has_primary_key([col_regular]) is False

    def test_matches_tags_with_cache(self, mock_client, discovery_config):
        """Test tag matching logic using cache."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        
        discovery._table_tags_cache = {
            "test_catalog.schema.matching_table": {"monitor_enabled": "true"},
            "test_catalog.schema.non_matching_table": {"monitor_enabled": "false"},
            "test_catalog.schema.missing_tag_table": {"other_tag": "value"},
        }
        
        assert discovery._matches_tags("test_catalog.schema.matching_table") is True
        assert discovery._matches_tags("test_catalog.schema.non_matching_table") is False
        assert discovery._matches_tags("test_catalog.schema.missing_tag_table") is False
        assert discovery._matches_tags("test_catalog.schema.no_tags_table") is False

    def test_is_supported_table_type(self, mock_client, discovery_config):
        """Test supported table type detection."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        
        for table_type in [
            TableType.MANAGED,
            TableType.EXTERNAL,
            TableType.VIEW,
            TableType.MATERIALIZED_VIEW,
            TableType.STREAMING_TABLE,
        ]:
            table = MagicMock()
            table.table_type = table_type
            assert discovery._is_supported_table_type(table) is True

    def test_fetch_tags_via_sdk(self, mock_client, discovery_config):
        """Test fetching tags via EntityTagAssignmentsAPI."""
        table_summary = MagicMock()
        table_summary.full_name = "test_catalog.ml_models.predictions"
        mock_client.tables.list.return_value = [table_summary]
        
        tag1 = MagicMock()
        tag1.tag_key = "monitor_enabled"
        tag1.tag_value = "true"
        tag2 = MagicMock()
        tag2.tag_key = "monitor_priority"
        tag2.tag_value = "1"
        mock_client.entity_tag_assignments.list.return_value = [tag1, tag2]
        
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        discovery._fetch_tags_via_sdk(["ml_models"])
        
        assert "test_catalog.ml_models.predictions" in discovery._table_tags_cache
        assert discovery._table_tags_cache["test_catalog.ml_models.predictions"]["monitor_enabled"] == "true"
        assert discovery._table_tags_cache["test_catalog.ml_models.predictions"]["monitor_priority"] == "1"

    def test_discover_full_workflow(self, mock_client, discovery_config):
        """Test full discovery workflow."""
        schema1 = MagicMock()
        schema1.name = "ml_models"
        schema2 = MagicMock()
        schema2.name = "tmp_test"
        mock_client.schemas.list.return_value = [schema1, schema2]
        
        table_summary = MagicMock()
        table_summary.full_name = "test_catalog.ml_models.predictions"
        mock_client.tables.list.return_value = [table_summary]
        
        tag1 = MagicMock()
        tag1.tag_key = "monitor_enabled"
        tag1.tag_value = "true"
        tag2 = MagicMock()
        tag2.tag_key = "monitor_priority"
        tag2.tag_value = "1"
        mock_client.entity_tag_assignments.list.return_value = [tag1, tag2]
        
        table_info = MagicMock()
        table_info.full_name = "test_catalog.ml_models.predictions"
        table_info.columns = []
        table_info.table_type = TableType.MANAGED
        mock_client.tables.get.return_value = table_info
        
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        tables = discovery.discover()
        
        assert len(tables) == 1
        assert tables[0].full_name == "test_catalog.ml_models.predictions"
        assert tables[0].priority == 1
        assert tables[0].tags["monitor_enabled"] == "true"

    def test_get_column_names(self, mock_client, discovery_config, sample_discovered_table):
        """Test getting column names from a table."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        names = discovery.get_column_names(sample_discovered_table)
        
        assert "prediction" in names
        assert "label" in names
        assert "timestamp" in names

    def test_find_column(self, mock_client, discovery_config, sample_discovered_table):
        """Test finding a column by name."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        
        col = discovery.find_column(sample_discovered_table, "prediction")
        assert col is not None
        assert col.name == "prediction"
        
        col = discovery.find_column(sample_discovered_table, "PREDICTION")
        assert col is not None
        
        col = discovery.find_column(sample_discovered_table, "nonexistent")
        assert col is None

    def test_get_column_type(self, mock_client, discovery_config, sample_discovered_table):
        """Test getting column type."""
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        
        col_type = discovery.get_column_type(sample_discovered_table, "prediction")
        assert col_type == "DOUBLE"
        
        col_type = discovery.get_column_type(sample_discovered_table, "nonexistent")
        assert col_type is None

    def test_discovery_handles_errors(self, mock_client, discovery_config):
        """Test discovery handles errors gracefully."""
        schema_mock = MagicMock()
        schema_mock.name = "test_schema"
        mock_client.schemas.list.return_value = [schema_mock]
        
        table_summary = MagicMock()
        table_summary.full_name = "test_catalog.test_schema.tbl"
        mock_client.tables.list.return_value = [table_summary]
        
        tag = MagicMock()
        tag.tag_key = "monitor_enabled"
        tag.tag_value = "true"
        mock_client.entity_tag_assignments.list.return_value = [tag]
        
        mock_client.tables.get.side_effect = Exception("API Error")
        
        discovery = TableDiscovery(mock_client, discovery_config, "test_catalog")
        tables = discovery.discover()
        
        assert tables == []


class TestIncludeSchemas:
    """Tests for include_schemas filtering."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock workspace client."""
        return MagicMock()

    def test_include_schemas_filters_correctly(self, mock_client):
        """Test include_schemas whitelist works."""
        config = DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            include_schemas=["ml_models", "data_warehouse"],
            exclude_schemas=["information_schema"],
        )
        
        schema1 = MagicMock()
        schema1.name = "ml_models"
        schema2 = MagicMock()
        schema2.name = "data_warehouse"
        schema3 = MagicMock()
        schema3.name = "other_schema"
        mock_client.schemas.list.return_value = [schema1, schema2, schema3]
        
        discovery = TableDiscovery(mock_client, config, "test_catalog")
        schemas = discovery._get_schemas()
        
        assert "ml_models" in schemas
        assert "data_warehouse" in schemas
        assert "other_schema" not in schemas

    def test_include_schemas_glob_patterns(self, mock_client):
        """Test include_schemas supports glob patterns."""
        config = DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            include_schemas=["ml_*", "warehouse_prod"],
        )
        
        schema1 = MagicMock()
        schema1.name = "ml_models"
        schema2 = MagicMock()
        schema2.name = "ml_features"
        schema3 = MagicMock()
        schema3.name = "warehouse_prod"
        schema4 = MagicMock()
        schema4.name = "other"
        mock_client.schemas.list.return_value = [schema1, schema2, schema3, schema4]
        
        discovery = TableDiscovery(mock_client, config, "test_catalog")
        schemas = discovery._get_schemas()
        
        assert "ml_models" in schemas
        assert "ml_features" in schemas
        assert "warehouse_prod" in schemas
        assert "other" not in schemas

    def test_include_and_exclude_schemas_interaction(self, mock_client):
        """Test include_schemas is applied before exclude_schemas."""
        config = DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            include_schemas=["ml_*"],
            exclude_schemas=["ml_dev"],
        )
        
        schema1 = MagicMock()
        schema1.name = "ml_prod"
        schema2 = MagicMock()
        schema2.name = "ml_dev"
        mock_client.schemas.list.return_value = [schema1, schema2]
        
        discovery = TableDiscovery(mock_client, config, "test_catalog")
        schemas = discovery._get_schemas()
        
        assert "ml_prod" in schemas
        assert "ml_dev" not in schemas

    def test_include_schemas_none_scans_all(self, mock_client):
        """Test None include_schemas scans all schemas."""
        config = DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            include_schemas=None,
            exclude_schemas=["tmp_*"],
        )
        
        schema1 = MagicMock()
        schema1.name = "production"
        schema2 = MagicMock()
        schema2.name = "staging"
        schema3 = MagicMock()
        schema3.name = "tmp_test"
        mock_client.schemas.list.return_value = [schema1, schema2, schema3]
        
        discovery = TableDiscovery(mock_client, config, "test_catalog")
        schemas = discovery._get_schemas()
        
        assert "production" in schemas
        assert "staging" in schemas
        assert "tmp_test" not in schemas

    def test_matches_include_pattern(self, mock_client):
        """Test _matches_include_pattern method."""
        config = DiscoveryConfig(
            include_tags={"monitor_enabled": "true"},
            include_schemas=["ml_*", "warehouse"],
        )
        
        discovery = TableDiscovery(mock_client, config, "test_catalog")
        
        assert discovery._matches_include_pattern("ml_models") is True
        assert discovery._matches_include_pattern("ml_features") is True
        assert discovery._matches_include_pattern("warehouse") is True
        assert discovery._matches_include_pattern("other") is False
