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

"""Unit tests for Notion operators."""

from unittest.mock import Mock, patch

from airflow.providers.notion.operators.notion import (
    NotionQueryDatabaseOperator,
    NotionCreatePageOperator,
    NotionUpdatePageOperator,
)


class TestNotionQueryDatabaseOperator:
    """Test NotionQueryDatabaseOperator class."""

    def test_init(self, mock_dag, mock_database_id):
        """Test operator initialization."""
        operator = NotionQueryDatabaseOperator(
            task_id="test_query", database_id=mock_database_id, dag=mock_dag
        )

        assert operator.task_id == "test_query"
        assert operator.database_id == mock_database_id
        assert operator.filter_params is None
        assert operator.notion_conn_id == "notion_default"

    def test_init_with_filter(self, mock_dag, mock_database_id, sample_filter_params):
        """Test operator initialization with filter."""
        operator = NotionQueryDatabaseOperator(
            task_id="test_query",
            database_id=mock_database_id,
            filter_params=sample_filter_params,
            dag=mock_dag,
        )

        assert operator.filter_params == sample_filter_params

    def test_template_fields(self, mock_dag, mock_database_id):
        """Test that template fields are properly defined."""
        operator = NotionQueryDatabaseOperator(
            task_id="test_query", database_id=mock_database_id, dag=mock_dag
        )

        assert "database_id" in operator.template_fields
        assert "filter_params" in operator.template_fields

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_success(
        self,
        mock_hook_class,
        mock_dag,
        mock_database_id,
        sample_query_response,
        mock_context,
    ):
        """Test successful operator execution."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.query_database.return_value = sample_query_response

        operator = NotionQueryDatabaseOperator(
            task_id="test_query", database_id=mock_database_id, dag=mock_dag
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        assert result == sample_query_response
        mock_hook_class.assert_called_once_with(notion_conn_id="notion_default")
        mock_hook_instance.query_database.assert_called_once_with(
            database_id=mock_database_id, filter_params=None
        )

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_with_filter(
        self,
        mock_hook_class,
        mock_dag,
        mock_database_id,
        sample_filter_params,
        sample_query_response,
        mock_context,
    ):
        """Test operator execution with filter parameters."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.query_database.return_value = sample_query_response

        operator = NotionQueryDatabaseOperator(
            task_id="test_query",
            database_id=mock_database_id,
            filter_params=sample_filter_params,
            dag=mock_dag,
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        assert result == sample_query_response
        mock_hook_instance.query_database.assert_called_once_with(
            database_id=mock_database_id, filter_params=sample_filter_params
        )

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_custom_connection(
        self,
        mock_hook_class,
        mock_dag,
        mock_database_id,
        sample_query_response,
        mock_context,
    ):
        """Test operator with custom connection ID."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.query_database.return_value = sample_query_response

        operator = NotionQueryDatabaseOperator(
            task_id="test_query",
            database_id=mock_database_id,
            notion_conn_id="custom_notion_conn",
            dag=mock_dag,
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        mock_hook_class.assert_called_once_with(notion_conn_id="custom_notion_conn")


class TestNotionCreatePageOperator:
    """Test NotionCreatePageOperator class."""

    def test_init(self, mock_dag, mock_database_id, sample_properties):
        """Test operator initialization."""
        operator = NotionCreatePageOperator(
            task_id="test_create",
            database_id=mock_database_id,
            properties=sample_properties,
            dag=mock_dag,
        )

        assert operator.task_id == "test_create"
        assert operator.database_id == mock_database_id
        assert operator.properties == sample_properties
        assert operator.children is None
        assert operator.notion_conn_id == "notion_default"

    def test_template_fields(self, mock_dag, mock_database_id, sample_properties):
        """Test that template fields are properly defined."""
        operator = NotionCreatePageOperator(
            task_id="test_create",
            database_id=mock_database_id,
            properties=sample_properties,
            dag=mock_dag,
        )

        assert "database_id" in operator.template_fields
        assert "properties" in operator.template_fields
        assert "children" in operator.template_fields

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_success(
        self,
        mock_hook_class,
        mock_dag,
        mock_database_id,
        sample_properties,
        sample_page_response,
        mock_context,
    ):
        """Test successful page creation."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.create_page.return_value = sample_page_response

        operator = NotionCreatePageOperator(
            task_id="test_create",
            database_id=mock_database_id,
            properties=sample_properties,
            dag=mock_dag,
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        assert result == sample_page_response
        mock_hook_instance.create_page.assert_called_once_with(
            database_id=mock_database_id, properties=sample_properties, children=None
        )
        # Verify XCom push
        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="page_id", value=sample_page_response["id"]
        )

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_with_children(
        self,
        mock_hook_class,
        mock_dag,
        mock_database_id,
        sample_properties,
        sample_page_response,
        mock_context,
    ):
        """Test page creation with children blocks."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.create_page.return_value = sample_page_response

        children = [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": "Content"}}]},
            }
        ]

        operator = NotionCreatePageOperator(
            task_id="test_create",
            database_id=mock_database_id,
            properties=sample_properties,
            children=children,
            dag=mock_dag,
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        assert result == sample_page_response
        mock_hook_instance.create_page.assert_called_once_with(
            database_id=mock_database_id,
            properties=sample_properties,
            children=children,
        )


class TestNotionUpdatePageOperator:
    """Test NotionUpdatePageOperator class."""

    def test_init(self, mock_dag, mock_page_id):
        """Test operator initialization."""
        properties = {"Status": {"select": {"name": "Completed"}}}

        operator = NotionUpdatePageOperator(
            task_id="test_update",
            page_id=mock_page_id,
            properties=properties,
            dag=mock_dag,
        )

        assert operator.task_id == "test_update"
        assert operator.page_id == mock_page_id
        assert operator.properties == properties
        assert operator.notion_conn_id == "notion_default"

    def test_template_fields(self, mock_dag, mock_page_id):
        """Test that template fields are properly defined."""
        properties = {"Status": {"select": {"name": "Completed"}}}

        operator = NotionUpdatePageOperator(
            task_id="test_update",
            page_id=mock_page_id,
            properties=properties,
            dag=mock_dag,
        )

        assert "page_id" in operator.template_fields
        assert "properties" in operator.template_fields

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_success(
        self,
        mock_hook_class,
        mock_dag,
        mock_page_id,
        sample_page_response,
        mock_context,
    ):
        """Test successful page update."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.update_page.return_value = sample_page_response

        properties = {"Status": {"select": {"name": "Completed"}}}

        operator = NotionUpdatePageOperator(
            task_id="test_update",
            page_id=mock_page_id,
            properties=properties,
            dag=mock_dag,
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        assert result == sample_page_response
        mock_hook_class.assert_called_once_with(notion_conn_id="notion_default")
        mock_hook_instance.update_page.assert_called_once_with(
            page_id=mock_page_id, properties=properties
        )

    @patch("airflow.providers.notion.operators.notion.NotionHook")
    def test_execute_multiple_properties(
        self,
        mock_hook_class,
        mock_dag,
        mock_page_id,
        sample_page_response,
        mock_context,
    ):
        """Test page update with multiple properties."""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.update_page.return_value = sample_page_response

        properties = {
            "Status": {"select": {"name": "Completed"}},
            "Priority": {"select": {"name": "Low"}},
            "Notes": {"rich_text": [{"text": {"content": "Updated notes"}}]},
        }

        operator = NotionUpdatePageOperator(
            task_id="test_update",
            page_id=mock_page_id,
            properties=properties,
            dag=mock_dag,
        )

        # Act
        result = operator.execute(context=mock_context)

        # Assert
        assert result == sample_page_response
        mock_hook_instance.update_page.assert_called_once_with(
            page_id=mock_page_id, properties=properties
        )
