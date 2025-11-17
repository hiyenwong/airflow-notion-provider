# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Integration tests for Notion provider.

Note: These tests require actual Notion API credentials.
Set environment variables:
- NOTION_API_TOKEN: Your Notion integration token
- NOTION_TEST_DATABASE_ID: A test database ID

To run: pytest tests/integration/ -v
To skip: pytest tests/unit/ -v
"""

import pytest
import os
from unittest.mock import patch

from airflow.providers.notion.hooks.notion import NotionHook


# Skip integration tests if credentials not available
pytestmark = pytest.mark.skipif(
    not os.getenv("NOTION_API_TOKEN") or not os.getenv("NOTION_TEST_DATABASE_ID"),
    reason="Notion API credentials not configured",
)


@pytest.fixture
def notion_connection():
    """Real Notion connection from environment."""
    from airflow.models import Connection

    conn = Connection(
        conn_id="notion_integration_test",
        conn_type="http",
        host="https://api.notion.com",
        password=os.getenv("NOTION_API_TOKEN"),
    )
    return conn


@pytest.fixture
def test_database_id():
    """Test database ID from environment."""
    return os.getenv("NOTION_TEST_DATABASE_ID")


class TestNotionIntegration:
    """Integration tests with real Notion API."""

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    def test_real_query_database(
        self, mock_get_connection, notion_connection, test_database_id
    ):
        """Test real database query."""
        mock_get_connection.return_value = notion_connection

        hook = NotionHook(notion_conn_id="notion_integration_test")
        result = hook.query_database(database_id=test_database_id)

        assert "results" in result
        assert "object" in result
        assert result["object"] == "list"

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    def test_real_get_database(
        self, mock_get_connection, notion_connection, test_database_id
    ):
        """Test real database retrieval."""
        mock_get_connection.return_value = notion_connection

        hook = NotionHook(notion_conn_id="notion_integration_test")
        result = hook.get_database(database_id=test_database_id)

        assert result["id"] == test_database_id
        assert "properties" in result
        assert result["object"] == "database"

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    def test_real_create_and_update_page(
        self, mock_get_connection, notion_connection, test_database_id
    ):
        """Test real page creation and update."""
        mock_get_connection.return_value = notion_connection

        hook = NotionHook(notion_conn_id="notion_integration_test")

        # Create page
        properties = {
            "Name": {"title": [{"text": {"content": "Integration Test Page"}}]}
        }

        created_page = hook.create_page(
            database_id=test_database_id, properties=properties
        )

        assert created_page["id"]
        page_id = created_page["id"]

        # Update page
        update_properties = {
            "Name": {"title": [{"text": {"content": "Updated Integration Test Page"}}]}
        }

        updated_page = hook.update_page(page_id=page_id, properties=update_properties)

        assert updated_page["id"] == page_id

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    def test_real_connection_test(self, mock_get_connection, notion_connection):
        """Test real connection testing."""
        mock_get_connection.return_value = notion_connection

        hook = NotionHook(notion_conn_id="notion_integration_test")
        success, message = hook.test_connection()

        assert success is True
        assert "successfully tested" in message
