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
    Query a Notion database and return the results.

    :param database_id: The ID of the database to query
    :type database_id: str
    :param filter_params: Optional filter parameters for the query
    :type filter_params: dict
    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    """

    template_fields = ["database_id", "filter_params"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        database_id: str,
        filter_params: Optional[Dict[str, Any]] = None,
        notion_conn_id: str = "notion_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.database_id = database_id
        self.filter_params = filter_params
        self.notion_conn_id = notion_conn_id

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the operator."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        self.log.info(f"Querying Notion database: {self.database_id}")
        if self.filter_params:
            self.log.info(f"With filter: {self.filter_params}")

        result = hook.query_database(
            database_id=self.database_id,
            filter_params=self.filter_params
        )

        self.log.info(f"Query returned {len(result.get('results', []))} results")
        return result


class NotionCreatePageOperator(BaseOperator):
    """
    Create a new page in a Notion database.

    :param database_id: The ID of the parent database
    :type database_id: str
    :param properties: The properties of the new page
    :type properties: dict
    :param children: Optional content blocks for the page
    :type children: list
    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    """

    template_fields = ["database_id", "properties", "children"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        database_id: str,
        properties: Dict[str, Any],
        children: Optional[list] = None,
        notion_conn_id: str = "notion_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.database_id = database_id
        self.properties = properties
        self.children = children
        self.notion_conn_id = notion_conn_id

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the operator."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        self.log.info(f"Creating page in Notion database: {self.database_id}")

        result = hook.create_page(
            database_id=self.database_id,
            properties=self.properties,
            children=self.children
        )

        page_id = result.get('id')
        self.log.info(f"Created Notion page with ID: {page_id}")

        context['task_instance'].xcom_push(key='page_id', value=page_id)
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
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.page_id = page_id
        self.properties = properties
        self.notion_conn_id = notion_conn_id

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the operator."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        self.log.info(f"Updating Notion page: {self.page_id}")

        result = hook.update_page(
            page_id=self.page_id,
            properties=self.properties
        )

        self.log.info(f"Successfully updated Notion page: {self.page_id}")
        return result