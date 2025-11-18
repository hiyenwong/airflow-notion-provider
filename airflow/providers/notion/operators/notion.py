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

from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.notion.hooks.notion import NotionHook
from airflow.utils.context import Context


class NotionQueryDatabaseOperator(BaseOperator):
    """
    Query a Notion database or data source.

    For API 2025-09-03+, prefer using data_source_id. If only database_id is provided,
    the operator will automatically discover the first data source.

    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    :param database_id: The ID of the database to query (deprecated, use data_source_id)
    :type database_id: str
    :param data_source_id: The ID of the data source to query (recommended)
    :type data_source_id: str
    :param filter_params: Optional filter parameters
    :type filter_params: dict
    :param sorts: Optional sort parameters
    :type sorts: list
    :param start_cursor: Optional cursor for pagination
    :type start_cursor: str
    :param page_size: Number of results per page
    :type page_size: int
    """

    template_fields = ["database_id", "data_source_id", "filter_params", "sorts"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        *,
        notion_conn_id: str = "notion_default",
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        filter_params: Optional[dict] = None,
        sorts: Optional[list] = None,
        start_cursor: Optional[str] = None,
        page_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.notion_conn_id = notion_conn_id
        self.database_id = database_id
        self.data_source_id = data_source_id
        self.filter_params = filter_params
        self.sorts = sorts
        self.start_cursor = start_cursor
        self.page_size = page_size

    def execute(self, context: dict) -> dict:
        hook = NotionHook(notion_conn_id=self.notion_conn_id)
        self.log.info(
            f"Querying Notion {'data source' if self.data_source_id else 'database'}: "
            f"{self.data_source_id or self.database_id}"
        )
        result = hook.query_database(
            database_id=self.database_id,
            data_source_id=self.data_source_id,
            filter_params=self.filter_params,
            sorts=self.sorts,
            start_cursor=self.start_cursor,
            page_size=self.page_size,
        )
        self.log.info(f"Query returned {len(result.get('results', []))} results")
        return result


class NotionCreatePageOperator(BaseOperator):
    """
    Create a new page in a Notion database or data source.

    For API 2025-09-03+, prefer using data_source_id. If only database_id is provided,
    the operator will automatically discover the first data source.

    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    :param database_id: The ID of the parent database (deprecated, use data_source_id)
    :type database_id: str
    :param data_source_id: The ID of the parent data source (recommended)
    :type data_source_id: str
    :param properties: The properties of the new page
    :type properties: dict
    :param children: Optional page content blocks
    :type children: list
    """

    template_fields = ["database_id", "data_source_id", "properties"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        *,
        notion_conn_id: str = "notion_default",
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        properties: Optional[dict] = None,
        children: Optional[list] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.notion_conn_id = notion_conn_id
        self.database_id = database_id
        self.data_source_id = data_source_id
        self.properties = properties
        self.children = children

    def execute(self, context: dict) -> dict:
        hook = NotionHook(notion_conn_id=self.notion_conn_id)
        self.log.info(
            f"Creating page in Notion {'data source' if self.data_source_id else 'database'}: "
            f"{self.data_source_id or self.database_id}"
        )
        result = hook.create_page(
            database_id=self.database_id,
            data_source_id=self.data_source_id,
            properties=self.properties,
            children=self.children,
        )
        page_id = result.get("id")
        self.log.info(f"Created page with ID: {page_id}")
        # Push the page_id to XCom for downstream tasks
        context["task_instance"].xcom_push(key="page_id", value=page_id)
        return result


class NotionUpdatePageOperator(BaseOperator):
    """
    Update an existing Notion page.

    :param page_id: The ID of the page to update
    :type page_id: str
    :param properties: The properties to update
    :type properties: dict
    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    """

    template_fields = ["page_id", "properties"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        page_id: str,
        properties: Dict[str, Any],
        notion_conn_id: str = "notion_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.page_id = page_id
        self.properties = properties
        self.notion_conn_id = notion_conn_id

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the operator."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        self.log.info(f"Updating Notion page: {self.page_id}")

        result = hook.update_page(page_id=self.page_id, properties=self.properties)

        self.log.info(f"Successfully updated Notion page: {self.page_id}")
        return result
