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

    conn_name_attr = 'notion_conn_id'
    default_conn_name = 'notion_default'
    conn_type = 'notion'
    hook_name = 'Notion'

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
                'Content-Type': 'application/json',
                'Notion-Version': '2022-06-28'
            }

            # Get token from extra field or password
            if conn.extra:
                try:
                    extra = json.loads(conn.extra)
                    if 'headers' in extra:
                        headers.update(extra['headers'])
                except json.JSONDecodeError:
                    pass

            if conn.password:
                headers['Authorization'] = f'Bearer {conn.password}'

            self.session.headers.update(headers)

            # Set base URL
            if conn.host:
                self.base_url = conn.host.rstrip('/')

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

    def query_database(self, database_id: str, filter_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Query a Notion database.

        :param database_id: The ID of the database to query
        :type database_id: str
        :param filter_params: Optional filter parameters
        :type filter_params: dict
        :return: The query result
        :rtype: dict
        """
        url = f"{self.base_url}/databases/{database_id}/query"
        data = {}
        if filter_params:
            data['filter'] = filter_params

        response = self.get_conn().post(url, json=data)
        response.raise_for_status()
        return response.json()

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

    def create_page(self, database_id: str, properties: Dict[str, Any], children: Optional[list] = None) -> Dict[str, Any]:
        """
        Create a new page in a database.

        :param database_id: The ID of the parent database
        :type database_id: str
        :param properties: The properties of the new page
        :type properties: dict
        :param children: Optional page content blocks
        :type children: list
        :return: The created page
        :rtype: dict
        """
        url = f"{self.base_url}/pages"
        data = {
            'parent': {'database_id': database_id},
            'properties': properties
        }
        if children:
            data['children'] = children

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
        data = {'properties': properties}

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

    def get_block_children(self, block_id: str, start_cursor: Optional[str] = None, page_size: int = 100) -> Dict[str, Any]:
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
        params = {'page_size': page_size}
        if start_cursor:
            params['start_cursor'] = start_cursor

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
        data = {'children': children}

        response = self.get_conn().patch(url, json=data)
        response.raise_for_status()
        return response.json()