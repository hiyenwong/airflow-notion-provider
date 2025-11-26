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
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        """Return custom field behaviour for Notion connection."""
        return {
            "hidden_fields": ["host", "schema", "login", "port"],
            "relabeling": {
                "password": "Integration Secret",
            },
            "placeholders": {
                "password": "secret_xxxxx... (from Notion integrations)",
                "extra": '{"verify_ssl": true}',
            },
        }

    def __init__(self, notion_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.notion_conn_id = notion_conn_id
        self.base_url = "https://api.notion.com/v1"
        self.session: Optional[requests.Session] = None
        self.verify_ssl = True  # 默认启用 SSL 验证

    def get_conn(self) -> requests.Session:
        """Get the connection to Notion API with retry and SSL configuration."""
        if self.session is None:
            self.session = requests.Session()

            # Configure retry strategy for network resilience
            retry_strategy = Retry(
                total=3,  # Total number of retries
                backoff_factor=1,  # Wait 1s, 2s, 4s between retries
                status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
                allowed_methods=[
                    "HEAD",
                    "GET",
                    "PUT",
                    "DELETE",
                    "OPTIONS",
                    "TRACE",
                    "POST",
                ],
                raise_on_status=False,  # Don't raise on status, let requests handle it
            )

            # Create HTTP adapter with retry configuration
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=10,
                pool_maxsize=10,
            )

            # Mount adapter for both http and https
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)

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
                    # 检查是否禁用 SSL 验证
                    if "verify_ssl" in extra:
                        self.verify_ssl = extra["verify_ssl"]
                        if not self.verify_ssl:
                            self.log.warning(
                                "⚠️  SSL 证书验证已禁用! 这不安全，仅用于开发/测试环境"
                            )
                            # 禁用 urllib3 的 SSL 警告
                            import urllib3

                            urllib3.disable_warnings(
                                urllib3.exceptions.InsecureRequestWarning
                            )
                except json.JSONDecodeError:
                    pass

            if conn.password:
                headers["Authorization"] = f"Bearer {conn.password}"
                # Log token info (masked for security)
                token_masked = (
                    conn.password[:10] + "***" + conn.password[-4:]
                    if len(conn.password) > 14
                    else "***"
                )
                self.log.info(f"Using Notion API token: {token_masked}")
            else:
                self.log.warning("No API token found in connection password field!")

            self.session.headers.update(headers)
            self.log.info(f"Session headers configured: {list(headers.keys())}")
            self.log.info(
                "Configured retry strategy: 3 retries with exponential backoff"
            )

            # Set base URL
            if conn.host:
                self.base_url = conn.host.rstrip("/")
            self.log.info(f"Base URL: {self.base_url}")

        return self.session

    def test_connection(self):
        """Test the connection to Notion API."""
        try:
            response = self.get_conn().get(
                f"{self.base_url}/users", verify=self.verify_ssl
            )
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
        if not database_id or not database_id.strip():
            raise ValueError(f"Invalid database_id: '{database_id}' (empty or None)")

        url = f"{self.base_url}/databases/{database_id}"
        self.log.info(f"Getting data sources for database: {database_id}")
        self.log.info(f"Request URL: {url}")

        response = None
        try:
            response = self.get_conn().get(url, verify=self.verify_ssl)
            response.raise_for_status()
            result = response.json()
            data_sources = result.get("data_sources", [])
            self.log.info(f"Found {len(data_sources)} data sources")
            if data_sources:
                self.log.info(f"Data source IDs: {[ds['id'] for ds in data_sources]}")
            return result
        except requests.exceptions.HTTPError as e:
            self.log.error(f"HTTP Error getting data sources: {e}")
            if response is not None:
                self.log.error(f"Status Code: {response.status_code}")
                self.log.error(f"Response Body: {response.text}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error getting data sources: {e}")
            raise

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
        # Validate data_source_id
        if not data_source_id or not data_source_id.strip():
            raise ValueError(
                f"Invalid data_source_id: '{data_source_id}' (empty or None)"
            )

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

        # Log request details
        self.log.info(f"Querying data source: {data_source_id}")
        self.log.info(f"Request URL: {url}")
        self.log.info(f"Request body: {json.dumps(data, indent=2)}")

        session = self.get_conn()
        auth_header = session.headers.get("Authorization", "")
        if auth_header:
            # Ensure auth_header is string for masking
            auth_str = str(auth_header)
            token_masked = (
                auth_str[:17] + "***" + auth_str[-4:]
                if len(auth_str) > 21
                else "Bearer ***"
            )
            self.log.info(f"Authorization header: {token_masked}")

        response = None
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = session.post(
                    url, json=data, timeout=30, verify=self.verify_ssl
                )
                response.raise_for_status()
                self.log.info(
                    f"Query successful, received {len(response.json().get('results', []))} results"
                )
                return response.json()
            except requests.exceptions.SSLError as ssl_error:
                retry_count += 1
                self.log.warning(
                    f"SSL Error on attempt {retry_count}/{max_retries}: {ssl_error}"
                )
                if retry_count < max_retries:
                    wait_time = 2**retry_count  # Exponential backoff: 2s, 4s, 8s
                    self.log.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    self.log.error("Max SSL retries exceeded")
                    self.log.error(f"SSL Error details: {ssl_error}")
                    self.log.error("Possible causes:")
                    self.log.error("1. Network instability or firewall blocking")
                    self.log.error("2. VPN or proxy interfering with SSL")
                    self.log.error("3. Outdated SSL/TLS libraries")
                    self.log.error("Solutions:")
                    self.log.error("- Check network connection")
                    self.log.error("- Disable VPN temporarily")
                    self.log.error(
                        "- Update Python SSL: pip install --upgrade urllib3 requests"
                    )
                    raise
            except requests.exceptions.HTTPError as e:
                # Log detailed error information
                self.log.error(f"HTTP Error occurred: {e}")
                if response is not None:
                    self.log.error(f"Status Code: {response.status_code}")
                    self.log.error(f"Response Headers: {dict(response.headers)}")
                    self.log.error(f"Response Body: {response.text}")
                self.log.error(f"Request URL: {url}")
                self.log.error(f"Request Body: {json.dumps(data)}")
                raise
            except Exception as e:
                self.log.error(f"Unexpected error during query: {e}")
                self.log.error(f"Request URL: {url}")
                raise

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
        # Log input parameters for debugging
        self.log.info(
            f"query_database called with: database_id={database_id!r}, data_source_id={data_source_id!r}"
        )

        # If data_source_id is provided, validate and use it directly
        if data_source_id is not None:
            # Check if it's an empty string
            if not data_source_id.strip():
                self.log.error(f"data_source_id is empty string: {data_source_id!r}")
                raise ValueError(
                    f"data_source_id cannot be empty. Received: {data_source_id!r}"
                )

            self.log.info(f"Using provided data_source_id: {data_source_id}")
            return self.query_data_source(
                data_source_id, filter_params, sorts, start_cursor, page_size
            )

        # If only database_id is provided, discover the first data source
        if database_id is not None:
            if not database_id.strip():
                self.log.error(f"database_id is empty string: {database_id!r}")
                raise ValueError(
                    f"database_id cannot be empty. Received: {database_id!r}"
                )

            self.log.info(f"Auto-discovering data_source_id for database {database_id}")
            try:
                db_info = self.get_data_sources(database_id)
                data_sources = db_info.get("data_sources", [])

                if not data_sources:
                    self.log.error(f"No data sources found for database {database_id}")
                    self.log.error(f"Database info: {json.dumps(db_info, indent=2)}")
                    raise ValueError(
                        f"No data sources found for database {database_id}"
                    )

                # Use the first data source
                discovered_data_source_id = data_sources[0]["id"]
                self.log.info(f"Discovered data_source_id: {discovered_data_source_id}")
                self.log.info(f"Total data sources available: {len(data_sources)}")
                return self.query_data_source(
                    discovered_data_source_id,
                    filter_params,
                    sorts,
                    start_cursor,
                    page_size,
                )
            except Exception as e:
                self.log.error(f"Failed to discover data_source_id: {e}")
                raise

        # Neither parameter provided
        self.log.error("Neither database_id nor data_source_id provided")
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
        response = self.get_conn().get(url, verify=self.verify_ssl)
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
        # Log input parameters for debugging
        self.log.info(
            f"create_page called with: database_id={database_id!r}, data_source_id={data_source_id!r}"
        )

        url = f"{self.base_url}/pages"

        # Determine parent
        if data_source_id is not None:
            # Validate data_source_id
            if not data_source_id.strip():
                self.log.error(f"data_source_id is empty string: {data_source_id!r}")
                raise ValueError(
                    f"data_source_id cannot be empty. Received: {data_source_id!r}"
                )
            self.log.info(f"Using provided data_source_id: {data_source_id}")
            parent = {"type": "data_source_id", "data_source_id": data_source_id}
        elif database_id is not None:
            # Validate database_id
            if not database_id.strip():
                self.log.error(f"database_id is empty string: {database_id!r}")
                raise ValueError(
                    f"database_id cannot be empty. Received: {database_id!r}"
                )
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

        # Log the full request for debugging
        self.log.info(f"Creating page with parent: {json.dumps(parent, indent=2)}")
        self.log.info(f"Properties: {json.dumps(properties, indent=2)}")
        if children:
            self.log.info(f"Children blocks: {len(children)} blocks")
            self.log.info(
                f"First block (if any): {json.dumps(children[0] if children else {}, indent=2)}"
            )

        response = self.get_conn().post(url, json=data, verify=self.verify_ssl)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.error(f"Failed to create page: {e}")
            self.log.error(f"Request URL: {url}")
            self.log.error(f"Request Body: {json.dumps(data, indent=2)}")
            self.log.error(f"Response Status: {response.status_code}")
            self.log.error(f"Response Headers: {dict(response.headers)}")
            self.log.error(f"Response Body: {response.text}")

            # Try to parse error message
            try:
                error_data = response.json()
                self.log.error(
                    f"Notion Error Code: {error_data.get('code', 'unknown')}"
                )
                self.log.error(
                    f"Notion Error Message: {error_data.get('message', 'No message')}"
                )
            except json.JSONDecodeError as json_err:
                self.log.error(f"Failed to parse error response as JSON: {json_err}")
                self.log.error(
                    f"Raw response text: {response.text[:500]}"
                )  # First 500 chars
            except Exception as parse_err:
                self.log.error(f"Unexpected error parsing error response: {parse_err}")
            raise
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

        response = self.get_conn().patch(url, json=data, verify=self.verify_ssl)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.error(f"Failed to update page: {e}")
            self.log.error(f"Response Status: {response.status_code}")
            self.log.error(f"Response Body: {response.text}")
            raise
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
        response = self.get_conn().get(url, verify=self.verify_ssl)
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

        response = self.get_conn().get(url, params=params, verify=self.verify_ssl)
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

        response = self.get_conn().patch(url, json=data, verify=self.verify_ssl)
        response.raise_for_status()
        return response.json()
