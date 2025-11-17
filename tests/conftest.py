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

"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import Mock, MagicMock
from airflow.models import Connection
import pendulum


@pytest.fixture
def mock_notion_connection():
    """Mock Airflow Notion connection."""
    conn = Mock(spec=Connection)
    conn.conn_id = "notion_default"
    conn.host = "https://api.notion.com"
    conn.password = "secret_test_token_12345"
    conn.extra = '{"headers": {"Notion-Version": "2022-06-28"}}'
    return conn


@pytest.fixture
def mock_database_id():
    """Mock database ID."""
    return "test_database_id_12345"


@pytest.fixture
def mock_page_id():
    """Mock page ID."""
    return "test_page_id_67890"


@pytest.fixture
def sample_database_response():
    """Sample database API response."""
    return {
        "object": "database",
        "id": "test_database_id_12345",
        "created_time": "2024-01-01T00:00:00.000Z",
        "last_edited_time": "2024-01-01T00:00:00.000Z",
        "title": [{"text": {"content": "Test Database"}}],
        "properties": {
            "Name": {"id": "title", "type": "title"},
            "Status": {"id": "status", "type": "select"},
            "Priority": {"id": "priority", "type": "select"},
        },
    }


@pytest.fixture
def sample_page_response():
    """Sample page API response."""
    return {
        "object": "page",
        "id": "test_page_id_67890",
        "created_time": "2024-01-01T00:00:00.000Z",
        "last_edited_time": "2024-01-01T00:00:00.000Z",
        "properties": {
            "Name": {
                "id": "title",
                "type": "title",
                "title": [{"text": {"content": "Test Page"}}],
            },
            "Status": {
                "id": "status",
                "type": "select",
                "select": {"name": "In Progress"},
            },
        },
        "url": "https://notion.so/test-page",
    }


@pytest.fixture
def sample_query_response():
    """Sample query database API response."""
    return {
        "object": "list",
        "results": [
            {
                "object": "page",
                "id": "page_1",
                "properties": {
                    "Name": {"title": [{"text": {"content": "Task 1"}}]},
                    "Status": {"select": {"name": "Done"}},
                },
            },
            {
                "object": "page",
                "id": "page_2",
                "properties": {
                    "Name": {"title": [{"text": {"content": "Task 2"}}]},
                    "Status": {"select": {"name": "Done"}},
                },
            },
        ],
        "has_more": False,
        "next_cursor": None,
    }


@pytest.fixture
def sample_properties():
    """Sample properties for page creation."""
    return {
        "Name": {"title": [{"text": {"content": "New Task"}}]},
        "Status": {"select": {"name": "In Progress"}},
        "Priority": {"select": {"name": "High"}},
    }


@pytest.fixture
def sample_filter_params():
    """Sample filter parameters."""
    return {"property": "Status", "select": {"equals": "Done"}}


@pytest.fixture
def mock_dag():
    """Mock Airflow DAG."""
    from airflow import DAG

    return DAG(
        dag_id="test_dag",
        start_date=pendulum.datetime(2024, 1, 1),
        schedule_interval=None,
    )


@pytest.fixture
def mock_task_instance():
    """Mock Airflow TaskInstance."""
    ti = MagicMock()
    ti.xcom_push = MagicMock()
    ti.xcom_pull = MagicMock(return_value={"id": "test_page_id"})
    return ti


@pytest.fixture
def mock_context(mock_task_instance):
    """Mock Airflow context."""
    return {
        "task_instance": mock_task_instance,
        "ds": "2024-01-01",
        "execution_date": pendulum.datetime(2024, 1, 1),
        "dag_run": MagicMock(),
    }
