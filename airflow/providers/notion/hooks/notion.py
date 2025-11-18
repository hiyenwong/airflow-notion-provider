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

import json
from typing import Any, Dict, Optional

import requests
from airflow.hooks.base import BaseHook


class NotionHook(BaseHook):
    """
    Interact with Notion API.

    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    """

    conn_name_attr = "notion_conn_id"
    default_conn_name = "notion_default"
    conn_type = "notion"
    hook_name = "Notion"

    def __init__(self, notion_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.notion_conn_id = notion_conn_id
        self.base_url = "https://api.notion.com/v1"
        self.session: Optional[requests.Session] = None

    def get_conn(self) -> requests.Session:
        """Get the connection to Notion API."""
        if self.session is None:
            self.session = requests.Session()

            # Get connection details
            conn = self.get_connection(self.notion_conn_id)

            # Set up headers
            headers = {
                "Content-Type": "application/json",
                "Notion-Version": "2025-09-03",
            }

            # Get token from extra field or password
            if conn.extra:
                try:
                    extra = json.loads(conn.extra)
                    if "headers" in extra:
                        headers.update(extra["headers"])
                except json.JSONDecodeError:
                    pass

            if conn.password:
                headers["Authorization"] = f"Bearer {conn.password}"

            self.session.headers.update(headers)

            # Set base URL
            if conn.host:
                self.base_url = conn.host.rstrip("/")

        return self.session

    def test_connection(self):
        """Test the connection to Notion API."""
        try:
            response = self.get_conn().get(f"{self.base_url}/users")
            if response.status_code == 200:
                return True, "Connection successfully tested"
            else:
                return False, f"HTTP Error: {response.status_code} - {response.text}"
        except Exception as e:
            return False, str(e)

    def get_data_sources(self, database_id: str) -> Dict[str, Any]:
        """
        Get data sources for a database (API 2025-09-03+).

        :param database_id: The ID of the database
        :type database_id: str
        :return: The database object with data_sources list
        :rtype: dict
        """
        url = f"{self.base_url}/databases/{database_id}"
        response = self.get_conn().get(url)
        response.raise_for_status()
        return response.json()

    def query_data_source(
        self,
        data_source_id: str,
        filter_params: Optional[Dict[str, Any]] = None,
        sorts: Optional[list] = None,
        start_cursor: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Query a Notion data source (API 2025-09-03+).

        :param data_source_id: The ID of the data source to query
        :type data_source_id: str
        :param filter_params: Optional filter parameters
        :type filter_params: dict
        :param sorts: Optional sort parameters
        :type sorts: list
        :param start_cursor: Optional cursor for pagination
        :type start_cursor: str
        :param page_size: Number of results per page
        :type page_size: int
        :return: The query result
        :rtype: dict
        """
        url = f"{self.base_url}/data_sources/{data_source_id}/query"
        data = {}
        if filter_params:
            data["filter"] = filter_params
        if sorts:
            data["sorts"] = sorts
        if start_cursor:
            data["start_cursor"] = start_cursor
        if page_size:
            data["page_size"] = page_size

        response = self.get_conn().post(url, json=data)
        response.raise_for_status()
        return response.json()

    def query_database(
        self,
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        filter_params: Optional[Dict[str, Any]] = None,
        sorts: Optional[list] = None,
        start_cursor: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Query a Notion database or data source.

        For API 2025-09-03+, this method automatically discovers the data_source_id if only
        database_id is provided (uses the first data source).

        :param database_id: The ID of the database (deprecated, use data_source_id)
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
        :return: The query result
        :rtype: dict
        """
        # If data_source_id is provided, use it directly
        if data_source_id is not None:
            return self.query_data_source(
                data_source_id, filter_params, sorts, start_cursor, page_size
            )

        # If only database_id is provided, discover the first data source
        if database_id is not None:
            self.log.info(f"Auto-discovering data_source_id for database {database_id}")
            db_info = self.get_data_sources(database_id)
            data_sources = db_info.get("data_sources", [])

            if not data_sources:
                raise ValueError(f"No data sources found for database {database_id}")

            # Use the first data source
            discovered_data_source_id = data_sources[0]["id"]
            self.log.info(f"Using data_source_id: {discovered_data_source_id}")
            return self.query_data_source(
                discovered_data_source_id, filter_params, sorts, start_cursor, page_size
            )

        raise ValueError("Either database_id or data_source_id must be provided")

    def get_database(self, database_id: str) -> Dict[str, Any]:
        """
        Get a database by ID.

        :param database_id: The ID of the database
        :type database_id: str
        :return: The database object
        :rtype: dict
        """
        url = f"{self.base_url}/databases/{database_id}"
        response = self.get_conn().get(url)
        response.raise_for_status()
        return response.json()

    def create_page(
        self,
        database_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        children: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Create a new page in a database or data source.

        For API 2025-09-03+, prefer using data_source_id. If only database_id is provided,
        the method will automatically discover the first data source.

        :param database_id: The ID of the parent database (deprecated, use data_source_id)
        :type database_id: str
        :param data_source_id: The ID of the parent data source (recommended)
        :type data_source_id: str
        :param properties: The properties of the new page
        :type properties: dict
        :param children: Optional page content blocks
        :type children: list
        :return: The created page
        :rtype: dict
        """
        url = f"{self.base_url}/pages"

        # Determine parent
        if data_source_id is not None:
            parent = {"type": "data_source_id", "data_source_id": data_source_id}
        elif database_id is not None:
            # Auto-discover data_source_id from database_id
            self.log.info(f"Auto-discovering data_source_id for database {database_id}")
            db_info = self.get_data_sources(database_id)
            data_sources = db_info.get("data_sources", [])

            if not data_sources:
                raise ValueError(f"No data sources found for database {database_id}")

            discovered_id = data_sources[0]["id"]
            self.log.info(f"Using data_source_id: {discovered_id}")
            parent = {"type": "data_source_id", "data_source_id": discovered_id}
        else:
            raise ValueError("Either database_id or data_source_id must be provided")

        data = {"parent": parent, "properties": properties or {}}
        if children:
            data["children"] = children

        response = self.get_conn().post(url, json=data)
        response.raise_for_status()
        return response.json()

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a page.

        :param page_id: The ID of the page to update
        :type page_id: str
        :param properties: The properties to update
        :type properties: dict
        :return: The updated page
        :rtype: dict
        """
        url = f"{self.base_url}/pages/{page_id}"
        data = {"properties": properties}

        response = self.get_conn().patch(url, json=data)
        response.raise_for_status()
        return response.json()

    def get_page(self, page_id: str) -> Dict[str, Any]:
        """
        Get a page by ID.

        :param page_id: The ID of the page
        :type page_id: str
        :return: The page object
        :rtype: dict
        """
        url = f"{self.base_url}/pages/{page_id}"
        response = self.get_conn().get(url)
        response.raise_for_status()
        return response.json()

    def get_block_children(
        self, block_id: str, start_cursor: Optional[str] = None, page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Get the children of a block.

        :param block_id: The ID of the block
        :type block_id: str
        :param start_cursor: Optional cursor for pagination
        :type start_cursor: str
        :param page_size: Number of results per page (max 100)
        :type page_size: int
        :return: The block children
        :rtype: dict
        """
        url = f"{self.base_url}/blocks/{block_id}/children"
        params: Dict[str, Any] = {"page_size": page_size}
        if start_cursor:
            params["start_cursor"] = start_cursor

        response = self.get_conn().get(url, params=params)
        response.raise_for_status()
        return response.json()

    def append_block_children(self, block_id: str, children: list) -> Dict[str, Any]:
        """
        Append children to a block.

        :param block_id: The ID of the block
        :type block_id: str
        :param children: The children blocks to append
        :type children: list
        :return: The updated block
        :rtype: dict
        """
        url = f"{self.base_url}/blocks/{block_id}/children"
        data = {"children": children}

        response = self.get_conn().patch(url, json=data)
        response.raise_for_status()
        return response.json()
