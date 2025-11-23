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

"""
Example DAG: Basic Notion Operations

This example demonstrates basic CRUD operations with Notion:
- Query database with filters
- Get a specific page
- Create a new page
- Update page properties
- Get page blocks/content

Prerequisites:
1. Set up Notion connection in Airflow UI:
   - Connection ID: notion_default
   - Connection Type: notion
   - Password: your_notion_api_token (ntn_xxxxx or secret_xxxxx)

2. Set Airflow Variables:
   - notion_database_id: Your Notion database ID
   - notion_data_source_id: Your data source ID (recommended for API 2025-09-03+)
   - notion_test_page_id: An existing page ID for testing get/update operations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.notion.operators.notion import (
    NotionQueryDatabaseOperator,
    NotionCreatePageOperator,
    NotionUpdatePageOperator,
)
from airflow.providers.notion.hooks.notion import NotionHook

# Default arguments for all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "example_notion_basic",
    default_args=default_args,
    description="Basic Notion API operations examples",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "notion", "basic"],
)


# Task 1: Query Database with Filters
# ====================================
# Query a Notion database and filter results by status
query_database = NotionQueryDatabaseOperator(
    task_id="query_database",
    # Option 1: Use data_source_id (recommended for API 2025-09-03+)
    data_source_id="{{ var.value.notion_data_source_id }}",
    # Option 2: Use database_id (legacy, auto-discovers first data source)
    # database_id="{{ var.value.notion_database_id }}",
    # Filter pages where Status = "In Progress"
    filter_params={"property": "Status", "status": {"equals": "In progress"}},
    # Sort by creation date, newest first
    sorts=[{"property": "Created time", "direction": "descending"}],
    page_size=10,  # Limit to 10 results
    dag=dag,
)


# Task 2: Get Specific Page
# ==========================
# Retrieve a specific page by ID to read its properties
def get_page_details(**context):
    """
    Get page details using NotionHook directly.

    Returns page object containing:
    - properties: All page properties (title, status, dates, etc.)
    - url: Page URL
    - created_time, last_edited_time: Timestamps
    """
    hook = NotionHook(notion_conn_id="notion_default")
    page_id = context["var"]["value"].get("notion_test_page_id")

    if not page_id:
        raise ValueError("Please set 'notion_test_page_id' in Airflow Variables")

    page = hook.get_page(page_id=page_id)

    # Extract and log key information
    print(f"Page ID: {page['id']}")
    print(f"Page URL: {page['url']}")
    print(f"Created: {page['created_time']}")
    print(f"Last Edited: {page['last_edited_time']}")

    # Print all properties
    print("\nPage Properties:")
    for prop_name, prop_value in page["properties"].items():
        print(f"  {prop_name}: {prop_value}")

    return page


get_page = PythonOperator(
    task_id="get_page",
    python_callable=get_page_details,
    dag=dag,
)


# Task 3: Create New Page
# ========================
# Create a new page in the database with properties
create_page = NotionCreatePageOperator(
    task_id="create_page",
    data_source_id="{{ var.value.notion_data_source_id }}",
    properties={
        # 必填：Name 是 title 类型
        "Name": {"title": [{"text": {"content": "Example Task - Created by Airflow"}}]},
        # 可选：Text 字段
        "Text": {
            "rich_text": [
                {
                    "text": {
                        "content": "This page was created by Airflow DAG as an example."
                    }
                }
            ]
        },
        # 注意：Created time 是只读属性，不能设置
        # 注意：Status 字段没有配置选项，暂时不能使用
    },
    children=[
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": "Task Details"}}]
            },
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "This task was automatically created by an Airflow workflow."
                        },
                    }
                ]
            },
        },
        {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [
                    {"type": "text", "text": {"content": "Execution date: { ds }"}}
                ]
            },
        },
        {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [
                    {"type": "text", "text": {"content": "Status: Created by Airflow"}}
                ]
            },
        },
    ],
    dag=dag,
)


# Task 4: Update Page Properties
# ===============================
# Update the page we just created (uses XCom to get page_id)
update_page = NotionUpdatePageOperator(
    task_id="update_page",
    page_id="{{ task_instance.xcom_pull(task_ids='create_page', key='page_id') }}",
    properties={
        # 更新 Text 字段
        "Text": {
            "rich_text": [
                {
                    "text": {
                        "content": "Updated: This page was modified by Airflow on {{ ds }}."
                    }
                }
            ]
        },
        # Status 字段暂时不能使用，因为没有配置选项
    },
    dag=dag,
)


# Task 5: Get Page Blocks (Content)
# ==================================
# Retrieve all blocks (content) from a page
def get_page_blocks(**context):
    """
    Get all blocks (content) from a page.

    Blocks can be:
    - paragraph, heading_1, heading_2, heading_3
    - bulleted_list_item, numbered_list_item
    - to_do, toggle, quote, divider
    - code, callout, table, etc.
    """
    hook = NotionHook(notion_conn_id="notion_default")

    # Get page_id from previous task
    page_id = context["task_instance"].xcom_pull(task_ids="create_page", key="page_id")

    if not page_id:
        print("No page_id found, skipping block retrieval")
        return

    # Get all child blocks
    blocks_response = hook.get_block_children(block_id=page_id, page_size=100)

    print(f"\nPage has {len(blocks_response['results'])} blocks:")

    for i, block in enumerate(blocks_response["results"], 1):
        block_type = block["type"]
        print(f"\n{i}. Block type: {block_type}")

        # Extract text content based on block type
        if block_type in [
            "paragraph",
            "heading_1",
            "heading_2",
            "heading_3",
            "bulleted_list_item",
            "numbered_list_item",
            "to_do",
            "toggle",
            "quote",
        ]:
            rich_text = block[block_type].get("rich_text", [])
            if rich_text:
                text = "".join([t.get("plain_text", "") for t in rich_text])
                print(f"   Content: {text}")
        elif block_type == "code":
            code = block["code"].get("rich_text", [])
            if code:
                text = "".join([t.get("plain_text", "") for t in code])
                language = block["code"].get("language", "plain text")
                print(f"   Language: {language}")
                print(f"   Code: {text[:100]}...")  # First 100 chars

    return blocks_response


get_blocks = PythonOperator(
    task_id="get_page_blocks",
    python_callable=get_page_blocks,
    dag=dag,
)


# Task 6: Add Comment (via Block)
# ================================
# Add a comment-like block to the page
def add_comment_block(**context):
    """
    Add a comment using callout block (visual comment).

    Note: Notion API 2025-09-03 doesn't have dedicated comment endpoints.
    Use callout blocks for comment-like functionality.
    """
    hook = NotionHook(notion_conn_id="notion_default")

    # Get page_id from previous task
    page_id = context["task_instance"].xcom_pull(task_ids="create_page", key="page_id")

    if not page_id:
        print("No page_id found, skipping comment addition")
        return

    # Add a callout block as a comment
    comment_blocks = [
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "💬 Comment: This task was processed successfully by the Airflow workflow."
                        },
                    }
                ],
                "icon": {"emoji": "💬"},
                "color": "blue_background",
            },
        }
    ]

    result = hook.append_block_children(block_id=page_id, children=comment_blocks)
    print(f"Added comment block to page {page_id}")

    return result


add_comment = PythonOperator(
    task_id="add_comment",
    python_callable=add_comment_block,
    dag=dag,
)


# Define task dependencies
# =========================
# Linear flow: query → get → create → update → get blocks → add comment
query_database >> get_page >> create_page >> update_page >> get_blocks >> add_comment
