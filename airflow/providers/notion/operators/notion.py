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

from airflow.sdk.bases.operator import BaseOperator
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
        # Convert empty strings to None to avoid validation errors
        self.database_id = database_id if database_id and database_id.strip() else None
        self.data_source_id = (
            data_source_id if data_source_id and data_source_id.strip() else None
        )
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
        # Convert empty strings to None to avoid validation errors
        self.database_id = database_id if database_id and database_id.strip() else None
        self.data_source_id = (
            data_source_id if data_source_id and data_source_id.strip() else None
        )
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
        # Convert empty string to raise error early
        if not page_id or not page_id.strip():
            raise ValueError(f"page_id cannot be empty. Received: {page_id!r}")
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


class NotionSearchOperator(BaseOperator):
    """
    Search pages and databases in Notion workspace.

    This operator uses the Notion Search API to find pages and databases that
    the integration has access to. Results can be filtered by object type and
    sorted by last edited time.

    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str
    :param query: Search query text (optional, leave empty to return all)
    :type query: str
    :param filter_object_type: Filter by object type: "page", "database", or None for both
    :type filter_object_type: str
    :param sort_direction: Sort direction: "ascending" or "descending"
    :type sort_direction: str
    :param start_cursor: Pagination cursor for continuing previous search
    :type start_cursor: str
    :param page_size: Number of results per page (max 100)
    :type page_size: int

    Example:
        # Search all pages
        search_pages = NotionSearchOperator(
            task_id="search_pages",
            filter_object_type="page",
            page_size=100
        )

        # Search pages with keyword
        search_projects = NotionSearchOperator(
            task_id="search_projects",
            query="project",
            filter_object_type="page"
        )

        # Search everything (pages + databases)
        search_all = NotionSearchOperator(
            task_id="search_all",
            page_size=50
        )
    """

    template_fields = ["query", "filter_object_type", "start_cursor"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        *,
        notion_conn_id: str = "notion_default",
        query: Optional[str] = None,
        filter_object_type: Optional[str] = None,
        sort_direction: str = "descending",
        start_cursor: Optional[str] = None,
        page_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.notion_conn_id = notion_conn_id
        self.query = query
        self.filter_object_type = filter_object_type
        self.sort_direction = sort_direction
        self.start_cursor = start_cursor
        self.page_size = page_size

        # Validate filter_object_type
        if self.filter_object_type and self.filter_object_type not in [
            "page",
            "database",
        ]:
            raise ValueError(
                f"filter_object_type must be 'page', 'database', or None. Got: {self.filter_object_type}"
            )

        # Validate sort_direction
        if self.sort_direction not in ["ascending", "descending"]:
            raise ValueError(
                f"sort_direction must be 'ascending' or 'descending'. Got: {self.sort_direction}"
            )

    def execute(self, context: dict) -> dict:
        """Execute the search operation."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        # Build filter params
        filter_params = None
        if self.filter_object_type:
            filter_params = {"property": "object", "value": self.filter_object_type}

        # Build sort params
        sort = {"direction": self.sort_direction, "timestamp": "last_edited_time"}

        # Log search parameters
        search_desc = f"Notion workspace"
        if self.query:
            search_desc += f" with query '{self.query}'"
        if self.filter_object_type:
            search_desc += f" (filter: {self.filter_object_type})"

        self.log.info(f"Searching {search_desc}")

        # Execute search
        result = hook.search(
            query=self.query,
            filter_params=filter_params,
            sort=sort,
            start_cursor=self.start_cursor,
            page_size=self.page_size,
        )

        # Log results
        results = result.get("results", [])
        has_more = result.get("has_more", False)
        next_cursor = result.get("next_cursor")

        self.log.info(f"Search returned {len(results)} results")
        self.log.info(f"Has more results: {has_more}")

        if has_more:
            self.log.info(f"Next cursor: {next_cursor}")
            # Push next cursor to XCom for pagination
            context["task_instance"].xcom_push(key="next_cursor", value=next_cursor)

        # Push results count to XCom
        context["task_instance"].xcom_push(key="results_count", value=len(results))

        return result


class NotionCreateCommentOperator(BaseOperator):
    """
    Create a comment on a Notion page or reply to an existing discussion.

    This operator uses the Notion Comments API to add comments. Comments created
    via the API will appear in the Notion UI's comment sidebar and trigger
    notifications to users.

    Note: The integration must have "Insert comments" capability enabled in
    the Notion Integrations dashboard.

    :param rich_text: The comment content in rich text format.
        Example: [{"type": "text", "text": {"content": "Hello!"}}]
    :type rich_text: list
    :param page_id: The ID of the page to add a top-level comment to.
        Use this to add a new comment to a page (not to reply to existing).
    :type page_id: str
    :param discussion_id: The ID of an existing discussion thread to reply to.
        Use this to reply to an existing inline comment/discussion.
        Cannot be used together with page_id.
    :type discussion_id: str
    :param comment_text: Simple text content for the comment (convenience parameter).
        If provided, will be converted to rich_text format automatically.
        Cannot be used together with rich_text.
    :type comment_text: str
    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str

    Example:
        # Add a simple text comment to a page
        add_comment = NotionCreateCommentOperator(
            task_id="add_comment",
            page_id="your-page-id",
            comment_text="Hello from Airflow workflow!",
        )

        # Add a rich text comment
        add_rich_comment = NotionCreateCommentOperator(
            task_id="add_rich_comment",
            page_id="your-page-id",
            rich_text=[
                {"type": "text", "text": {"content": "Task completed by "}},
                {"type": "text", "text": {"content": "Airflow"}, "annotations": {"bold": True}},
                {"type": "text", "text": {"content": " at {{ ts }}"}}
            ],
        )

        # Reply to an existing discussion
        reply_to_discussion = NotionCreateCommentOperator(
            task_id="reply",
            discussion_id="discussion-id-here",
            comment_text="This is a reply to the discussion.",
        )

    See: https://developers.notion.com/docs/working-with-comments
    """

    template_fields = ["page_id", "discussion_id", "rich_text", "comment_text"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        *,
        notion_conn_id: str = "notion_default",
        page_id: Optional[str] = None,
        discussion_id: Optional[str] = None,
        rich_text: Optional[list] = None,
        comment_text: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.notion_conn_id = notion_conn_id
        self.page_id = page_id if page_id and page_id.strip() else None
        self.discussion_id = (
            discussion_id if discussion_id and discussion_id.strip() else None
        )
        self.rich_text = rich_text
        self.comment_text = comment_text

        # Validate: either page_id or discussion_id must be provided
        if not self.page_id and not self.discussion_id:
            raise ValueError(
                "Either page_id or discussion_id must be provided. "
                "Use page_id for new page comments, discussion_id for replies."
            )

        if self.page_id and self.discussion_id:
            raise ValueError(
                "Cannot provide both page_id and discussion_id. "
                "Use page_id for new page comments, discussion_id for replies."
            )

        # Validate: either rich_text or comment_text must be provided
        if not self.rich_text and not self.comment_text:
            raise ValueError(
                "Either rich_text or comment_text must be provided."
            )

        if self.rich_text and self.comment_text:
            raise ValueError(
                "Cannot provide both rich_text and comment_text. Use one or the other."
            )

    def execute(self, context: dict) -> dict:
        """Execute the operator to create a comment."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        # Build rich_text from comment_text if needed
        rich_text = self.rich_text
        if self.comment_text:
            rich_text = [{"type": "text", "text": {"content": self.comment_text}}]

        # Log the action
        if self.page_id:
            self.log.info(f"Creating comment on page: {self.page_id}")
        else:
            self.log.info(f"Replying to discussion: {self.discussion_id}")

        # Create the comment
        result = hook.create_comment(
            rich_text=rich_text,
            page_id=self.page_id,
            discussion_id=self.discussion_id,
        )

        comment_id = result.get("id")
        discussion_id = result.get("discussion_id")

        self.log.info(f"Comment created successfully with ID: {comment_id}")
        self.log.info(f"Discussion ID: {discussion_id}")

        # Push to XCom for downstream tasks
        context["task_instance"].xcom_push(key="comment_id", value=comment_id)
        context["task_instance"].xcom_push(key="discussion_id", value=discussion_id)

        return result


class NotionListCommentsOperator(BaseOperator):
    """
    Retrieve comments for a Notion page or block.

    This operator retrieves all un-resolved (open) comments on a page or block.
    Since pages are technically blocks, you can use a page_id as the block_id.

    Note: The integration must have "Read comments" capability enabled in
    the Notion Integrations dashboard.

    :param block_id: The ID of the block or page to retrieve comments for.
        Pages are also blocks, so you can pass a page_id here.
    :type block_id: str
    :param start_cursor: Pagination cursor from a previous request.
    :type start_cursor: str
    :param page_size: Number of results per page (max 100, default 100).
    :type page_size: int
    :param notion_conn_id: The connection ID to use for Notion API
    :type notion_conn_id: str

    Example:
        # Get all comments on a page
        get_comments = NotionListCommentsOperator(
            task_id="get_comments",
            block_id="your-page-id",
        )

        # Get comments with pagination
        get_more_comments = NotionListCommentsOperator(
            task_id="get_more_comments",
            block_id="your-page-id",
            start_cursor="{{ task_instance.xcom_pull(task_ids='get_comments', key='next_cursor') }}",
        )

    See: https://developers.notion.com/docs/working-with-comments
    """

    template_fields = ["block_id", "start_cursor"]
    ui_color = "#3B7FB6"

    def __init__(
        self,
        block_id: str,
        *,
        notion_conn_id: str = "notion_default",
        start_cursor: Optional[str] = None,
        page_size: int = 100,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if not block_id or not block_id.strip():
            raise ValueError(f"block_id cannot be empty. Received: {block_id!r}")

        self.block_id = block_id
        self.notion_conn_id = notion_conn_id
        self.start_cursor = start_cursor
        self.page_size = page_size

    def execute(self, context: dict) -> dict:
        """Execute the operator to list comments."""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        self.log.info(f"Retrieving comments for block/page: {self.block_id}")

        result = hook.list_comments(
            block_id=self.block_id,
            start_cursor=self.start_cursor,
            page_size=self.page_size,
        )

        comments = result.get("results", [])
        has_more = result.get("has_more", False)
        next_cursor = result.get("next_cursor")

        self.log.info(f"Retrieved {len(comments)} comments")

        # Count unique discussions
        discussion_ids = set()
        for comment in comments:
            disc_id = comment.get("discussion_id")
            if disc_id:
                discussion_ids.add(disc_id)

        if discussion_ids:
            self.log.info(f"Found {len(discussion_ids)} discussion thread(s)")

        # Push to XCom
        context["task_instance"].xcom_push(key="comments_count", value=len(comments))
        context["task_instance"].xcom_push(
            key="discussion_count", value=len(discussion_ids)
        )

        if has_more:
            self.log.info(f"Has more comments, next cursor: {next_cursor}")
            context["task_instance"].xcom_push(key="next_cursor", value=next_cursor)

        return result
