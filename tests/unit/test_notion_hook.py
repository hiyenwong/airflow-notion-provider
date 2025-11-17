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

"""Unit tests for NotionHook."""

import pytest
from unittest.mock import Mock, patch
import requests

from airflow.providers.notion.hooks.notion import NotionHook


class TestNotionHook:
    """Test NotionHook class."""

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    def test_get_conn_with_password(self, mock_get_connection, mock_notion_connection):
        """Test get_conn method with password authentication."""
        mock_get_connection.return_value = mock_notion_connection

        hook = NotionHook(notion_conn_id="notion_default")
        session = hook.get_conn()

        assert isinstance(session, requests.Session)
        assert "Authorization" in session.headers
        assert session.headers["Authorization"] == "Bearer secret_test_token_12345"
        assert session.headers["Notion-Version"] == "2022-06-28"
        assert session.headers["Content-Type"] == "application/json"

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    def test_get_conn_cached_session(self, mock_get_connection, mock_notion_connection):
        """Test that session is cached and reused."""
        mock_get_connection.return_value = mock_notion_connection

        hook = NotionHook(notion_conn_id="notion_default")
        session1 = hook.get_conn()
        session2 = hook.get_conn()

        assert session1 is session2
        mock_get_connection.assert_called_once()

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.post")
    def test_query_database_success(
        self,
        mock_post,
        mock_get_connection,
        mock_notion_connection,
        mock_database_id,
        sample_query_response,
    ):
        """Test successful database query."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_query_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.query_database(database_id=mock_database_id)

        assert result == sample_query_response
        assert len(result["results"]) == 2
        assert result["has_more"] is False
        mock_post.assert_called_once()

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.post")
    def test_query_database_with_filter(
        self,
        mock_post,
        mock_get_connection,
        mock_notion_connection,
        mock_database_id,
        sample_filter_params,
        sample_query_response,
    ):
        """Test database query with filter parameters."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_query_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.query_database(
            database_id=mock_database_id, filter_params=sample_filter_params
        )

        assert result == sample_query_response
        call_args = mock_post.call_args
        assert "json" in call_args.kwargs
        assert "filter" in call_args.kwargs["json"]
        assert call_args.kwargs["json"]["filter"] == sample_filter_params

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.get")
    def test_get_database_success(
        self,
        mock_get,
        mock_get_connection,
        mock_notion_connection,
        mock_database_id,
        sample_database_response,
    ):
        """Test successful database retrieval."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_database_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.get_database(database_id=mock_database_id)

        assert result == sample_database_response
        assert result["id"] == mock_database_id
        expected_url = f"https://api.notion.com/v1/databases/{mock_database_id}"
        mock_get.assert_called_once_with(expected_url)

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.post")
    def test_create_page_success(
        self,
        mock_post,
        mock_get_connection,
        mock_notion_connection,
        mock_database_id,
        sample_properties,
        sample_page_response,
    ):
        """Test successful page creation."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_page_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.create_page(
            database_id=mock_database_id, properties=sample_properties
        )

        assert result == sample_page_response
        assert result["id"] == sample_page_response["id"]
        call_args = mock_post.call_args
        assert call_args.kwargs["json"]["parent"]["database_id"] == mock_database_id
        assert call_args.kwargs["json"]["properties"] == sample_properties

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.post")
    def test_create_page_with_children(
        self,
        mock_post,
        mock_get_connection,
        mock_notion_connection,
        mock_database_id,
        sample_properties,
        sample_page_response,
    ):
        """Test page creation with children blocks."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_page_response
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        children = [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": "Test content"}}]},
            }
        ]

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.create_page(
            database_id=mock_database_id,
            properties=sample_properties,
            children=children,
        )

        assert result == sample_page_response
        call_args = mock_post.call_args
        assert "children" in call_args.kwargs["json"]
        assert call_args.kwargs["json"]["children"] == children

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.patch")
    def test_update_page_success(
        self,
        mock_patch,
        mock_get_connection,
        mock_notion_connection,
        mock_page_id,
        sample_page_response,
    ):
        """Test successful page update."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_page_response
        mock_response.raise_for_status = Mock()
        mock_patch.return_value = mock_response

        properties = {"Status": {"select": {"name": "Completed"}}}

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.update_page(page_id=mock_page_id, properties=properties)

        assert result == sample_page_response
        expected_url = f"https://api.notion.com/v1/pages/{mock_page_id}"
        mock_patch.assert_called_once()
        call_args = mock_patch.call_args
        assert call_args.args[0] == expected_url
        assert call_args.kwargs["json"]["properties"] == properties

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.get")
    def test_get_page_success(
        self,
        mock_get,
        mock_get_connection,
        mock_notion_connection,
        mock_page_id,
        sample_page_response,
    ):
        """Test successful page retrieval."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.json.return_value = sample_page_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.get_page(page_id=mock_page_id)

        assert result == sample_page_response
        assert result["id"] == mock_page_id
        expected_url = f"https://api.notion.com/v1/pages/{mock_page_id}"
        mock_get.assert_called_once_with(expected_url)

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.get")
    def test_get_block_children_success(
        self, mock_get, mock_get_connection, mock_notion_connection, mock_page_id
    ):
        """Test successful block children retrieval."""
        mock_get_connection.return_value = mock_notion_connection

        block_response = {
            "object": "list",
            "results": [{"object": "block", "type": "paragraph", "id": "block_1"}],
            "has_more": False,
            "next_cursor": None,
        }

        mock_response = Mock()
        mock_response.json.return_value = block_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.get_block_children(block_id=mock_page_id)

        assert result == block_response
        assert len(result["results"]) == 1
        call_args = mock_get.call_args
        assert "params" in call_args.kwargs
        assert call_args.kwargs["params"]["page_size"] == 100

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.patch")
    def test_append_block_children_success(
        self, mock_patch, mock_get_connection, mock_notion_connection, mock_page_id
    ):
        """Test successful block children append."""
        mock_get_connection.return_value = mock_notion_connection

        children = [
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {"rich_text": [{"text": {"content": "New Section"}}]},
            }
        ]

        block_response = {"object": "list", "results": children}

        mock_response = Mock()
        mock_response.json.return_value = block_response
        mock_response.raise_for_status = Mock()
        mock_patch.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        result = hook.append_block_children(block_id=mock_page_id, children=children)

        assert result == block_response
        call_args = mock_patch.call_args
        assert call_args.kwargs["json"]["children"] == children

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.post")
    def test_api_error_handling(
        self, mock_post, mock_get_connection, mock_notion_connection, mock_database_id
    ):
        """Test API error handling."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "API Error"
        )
        mock_post.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")

        with pytest.raises(requests.exceptions.HTTPError):
            hook.query_database(database_id=mock_database_id)

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.get")
    def test_test_connection_success(
        self, mock_get, mock_get_connection, mock_notion_connection
    ):
        """Test connection testing functionality."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        success, message = hook.test_connection()

        assert success is True
        assert "successfully tested" in message

    @patch("airflow.providers.notion.hooks.notion.NotionHook.get_connection")
    @patch("requests.Session.get")
    def test_test_connection_failure(
        self, mock_get, mock_get_connection, mock_notion_connection
    ):
        """Test connection testing failure."""
        mock_get_connection.return_value = mock_notion_connection
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response

        hook = NotionHook(notion_conn_id="notion_default")
        success, message = hook.test_connection()

        assert success is False
        assert "401" in message
