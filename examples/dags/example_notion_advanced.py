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
Example DAG: Advanced Notion Features

This example demonstrates advanced operations:
- Pagination for large result sets
- Complex filters (AND/OR combinations)
- Block operations (nested content)
- Multi-database coordination
- Error handling and retries
- Database schema introspection

Prerequisites:
- notion_database_id or notion_data_source_id in Airflow Variables
- notion_secondary_database_id for multi-database example (optional)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.notion.operators.notion import NotionQueryDatabaseOperator
from airflow.providers.notion.hooks.notion import NotionHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "example_notion_advanced",
    default_args=default_args,
    description="Advanced Notion API operations",
    schedule=None,  # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "notion", "advanced"],
)


# Task 1: Complex Filtering (AND/OR)
# ===================================
query_complex = NotionQueryDatabaseOperator(
    task_id="query_with_complex_filters",
    data_source_id='{{ var.value.get("notion_data_source_id", "") }}',
    # Complex filter: (High priority OR urgent) AND (not completed) AND (due this week)
    filter_params={
        "and": [
            {
                "or": [
                    {
                        "property": "Priority",
                        "number": {
                            "equals": 1  # High priority
                        },
                    },
                    {"property": "Tags", "multi_select": {"contains": "Urgent"}},
                ]
            },
            {"property": "Status", "select": {"does_not_equal": "Completed"}},
            {
                "property": "Due Date",
                "date": {
                    "on_or_before": "{{ macros.ds_add(ds, 7) }}"  # Within 7 days
                },
            },
        ]
    },
    sorts=[
        {"property": "Priority", "direction": "ascending"},
        {"property": "Due Date", "direction": "ascending"},
    ],
    page_size=50,
    dag=dag,
)


# Task 2: Pagination Handling
# ============================
def query_all_pages_with_pagination(**context):
    """
    Fetch all pages from a database using pagination.

    Notion API limits results to 100 per request.
    This demonstrates how to paginate through all results.
    """
    hook = NotionHook(notion_conn_id="notion_default")
    data_source_id = context["var"]["value"].get("notion_data_source_id")

    if not data_source_id:
        raise ValueError("notion_data_source_id not set in Airflow Variables")

    all_pages = []
    has_more = True
    start_cursor = None
    page_num = 0

    print("Starting pagination...")

    while has_more:
        page_num += 1
        print(f"\nFetching page {page_num}...")

        result = hook.query_data_source(
            data_source_id=data_source_id,
            start_cursor=start_cursor,
            page_size=100,  # Max allowed by Notion
        )

        pages = result.get("results", [])
        all_pages.extend(pages)

        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")

        print(f"  Retrieved {len(pages)} pages")
        print(f"  Total so far: {len(all_pages)}")
        print(f"  Has more: {has_more}")

    print(f"\n✅ Pagination complete: Retrieved {len(all_pages)} total pages")

    # Store for downstream tasks
    context["task_instance"].xcom_push(key="all_pages", value=all_pages)
    context["task_instance"].xcom_push(key="total_count", value=len(all_pages))

    return len(all_pages)


query_paginated = PythonOperator(
    task_id="query_with_pagination",
    python_callable=query_all_pages_with_pagination,
    dag=dag,
)


# Task 3: Database Schema Introspection
# ======================================
def inspect_database_schema(**context):
    """
    Retrieve and analyze database schema (API 2025-09-03).

    Shows:
    - Database properties and their types
    - Data sources available
    - Property configurations
    """
    hook = NotionHook(notion_conn_id="notion_default")
    database_id = context["var"]["value"].get("notion_database_id")

    if not database_id:
        raise ValueError("notion_database_id not set in Airflow Variables")

    # Get database with data sources
    db_info = hook.get_data_sources(database_id=database_id)

    print("\n" + "=" * 60)
    print("DATABASE INFORMATION")
    print("=" * 60)

    print(f"\nDatabase ID: {db_info['id']}")
    print(f"Created: {db_info.get('created_time', 'N/A')}")
    print(f"Last Edited: {db_info.get('last_edited_time', 'N/A')}")

    # Database title
    title = db_info.get("title", [])
    if title:
        db_name = "".join([t.get("plain_text", "") for t in title])
        print(f"Name: {db_name}")

    # Data sources (new in API 2025-09-03)
    print("\n" + "-" * 60)
    print("DATA SOURCES")
    print("-" * 60)

    data_sources = db_info.get("data_sources", [])
    if data_sources:
        for i, ds in enumerate(data_sources, 1):
            print(f"\n{i}. Data Source:")
            print(f"   ID: {ds['id']}")
            print(f"   Name: {ds.get('name', 'Unnamed')}")
    else:
        print("No data sources found (may be using legacy single-source database)")

    # Properties schema
    print("\n" + "-" * 60)
    print("PROPERTIES SCHEMA")
    print("-" * 60)

    properties = db_info.get("properties", {})
    for prop_name, prop_config in properties.items():
        prop_type = prop_config.get("type", "unknown")
        prop_id = prop_config.get("id", "N/A")

        print(f"\n• {prop_name}")
        print(f"  Type: {prop_type}")
        print(f"  ID: {prop_id}")

        # Show type-specific configuration
        if prop_type == "select":
            options = prop_config.get("select", {}).get("options", [])
            if options:
                option_names = [opt["name"] for opt in options]
                print(f"  Options: {', '.join(option_names)}")

        elif prop_type == "multi_select":
            options = prop_config.get("multi_select", {}).get("options", [])
            if options:
                option_names = [opt["name"] for opt in options]
                print(f"  Options: {', '.join(option_names)}")

        elif prop_type == "number":
            format_type = prop_config.get("number", {}).get("format", "number")
            print(f"  Format: {format_type}")

        elif prop_type == "formula":
            expression = prop_config.get("formula", {}).get("expression", "N/A")
            print(f"  Expression: {expression}")

    print("\n" + "=" * 60)

    # Store schema for downstream tasks
    context["task_instance"].xcom_push(key="database_schema", value=db_info)

    return db_info


inspect_schema = PythonOperator(
    task_id="inspect_database_schema",
    python_callable=inspect_database_schema,
    dag=dag,
)


# Task 4: Nested Block Operations
# ================================
def create_page_with_nested_blocks(**context):
    """
    Create a page with complex nested block structure.

    Demonstrates:
    - Nested lists (parent-child blocks)
    - Different block types
    - Rich text formatting
    """
    hook = NotionHook(notion_conn_id="notion_default")
    data_source_id = context["var"]["value"].get("notion_data_source_id")

    if not data_source_id:
        raise ValueError("notion_data_source_id not set")

    # Create page with complex nested structure
    page = hook.create_page(
        data_source_id=data_source_id,
        properties={
            "Name": {
                "title": [{"text": {"content": "Advanced Example: Nested Content"}}]
            },
            "Status": {"select": {"name": "In Progress"}},
        },
        children=[
            # Heading
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": "📚 Project Structure"},
                            "annotations": {"bold": True},
                        }
                    ]
                },
            },
            # Toggle block (collapsible)
            {
                "object": "block",
                "type": "toggle",
                "toggle": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "Click to expand details"}}
                    ]
                },
            },
            # Numbered list
            {
                "object": "block",
                "type": "numbered_list_item",
                "numbered_list_item": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": "First step: Setup environment"},
                        }
                    ]
                },
            },
            {
                "object": "block",
                "type": "numbered_list_item",
                "numbered_list_item": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": "Second step: Configure connections"},
                        }
                    ]
                },
            },
            # Code block
            {
                "object": "block",
                "type": "code",
                "code": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": 'from airflow.providers.notion.hooks import NotionHook\n\nhook = NotionHook()\nresult = hook.query_database(database_id="...")'
                            },
                        }
                    ],
                    "language": "python",
                },
            },
            # Callout (info box)
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "💡 Tip: Use data_source_id for API 2025-09-03+"
                            },
                        }
                    ],
                    "icon": {"emoji": "💡"},
                    "color": "yellow_background",
                },
            },
            # Quote
            {
                "object": "block",
                "type": "quote",
                "quote": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "Automation is not about replacing people, it's about empowering them."
                            },
                            "annotations": {"italic": True},
                        }
                    ]
                },
            },
            # Divider
            {"object": "block", "type": "divider", "divider": {}},
            # To-do items
            {
                "object": "block",
                "type": "to_do",
                "to_do": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "Complete documentation"}}
                    ],
                    "checked": False,
                },
            },
            {
                "object": "block",
                "type": "to_do",
                "to_do": {
                    "rich_text": [{"type": "text", "text": {"content": "Write tests"}}],
                    "checked": True,
                },
            },
        ],
    )

    page_id = page["id"]
    print(f"✅ Created page with nested blocks: {page_id}")
    print(f"   URL: {page['url']}")

    # Now add child blocks to the toggle (demonstrating nested structure)
    # Note: This requires a second API call since we need the toggle block's ID
    blocks = hook.get_block_children(block_id=page_id)
    toggle_block = next((b for b in blocks["results"] if b["type"] == "toggle"), None)

    if toggle_block:
        hook.append_block_children(
            block_id=toggle_block["id"],
            children=[
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": "This is nested content inside the toggle!"
                                },
                            }
                        ]
                    },
                }
            ],
        )
        print("✅ Added nested content to toggle block")

    context["task_instance"].xcom_push(key="page_id", value=page_id)
    return page


create_nested = PythonOperator(
    task_id="create_nested_blocks",
    python_callable=create_page_with_nested_blocks,
    dag=dag,
)


# Task 5: Multi-Database Coordination
# ====================================
def sync_across_databases(**context):
    """
    Coordinate data across multiple databases.

    Example use case: Sync tasks from project database to archive database.
    """
    hook = NotionHook(notion_conn_id="notion_default")

    primary_ds_id = context["var"]["value"].get("notion_data_source_id")
    secondary_ds_id = context["var"]["value"].get("notion_secondary_data_source_id")

    if not secondary_ds_id:
        print("⚠️  Secondary database not configured, skipping sync")
        return

    # Query completed tasks from primary database
    completed_tasks = hook.query_data_source(
        data_source_id=primary_ds_id,
        filter_params={
            "and": [
                {"property": "Status", "select": {"equals": "Completed"}},
                {"property": "Archived", "checkbox": {"equals": False}},
            ]
        },
    )

    tasks = completed_tasks.get("results", [])
    archived_count = 0

    print(f"Found {len(tasks)} completed tasks to archive")

    for task in tasks:
        task_id = task["id"]
        task_props = task["properties"]

        # Extract key properties
        title_prop = task_props.get("Name", {}).get("title", [])
        title = (
            "".join([t.get("plain_text", "") for t in title_prop])
            if title_prop
            else "Untitled"
        )

        # Create copy in archive database
        try:
            archived_page = hook.create_page(
                data_source_id=secondary_ds_id,
                properties={
                    "Name": {"title": [{"text": {"content": f"[Archived] {title}"}}]},
                    "Status": {"select": {"name": "Archived"}},
                    "Original ID": {"rich_text": [{"text": {"content": task_id}}]},
                    "Archived Date": {"date": {"start": context["ds"]}},
                },
            )

            # Mark original as archived
            hook.update_page(
                page_id=task_id, properties={"Archived": {"checkbox": True}}
            )

            archived_count += 1
            print(f"  ✅ Archived: {title}")

        except Exception as e:
            print(f"  ❌ Failed to archive {title}: {str(e)}")
            continue

    print(f"\n✅ Archived {archived_count} tasks")
    return archived_count


sync_databases = PythonOperator(
    task_id="sync_across_databases",
    python_callable=sync_across_databases,
    dag=dag,
)


# Task 6: Error Handling Example
# ===============================
def demonstrate_error_handling(**context):
    """
    Demonstrate proper error handling with Notion API.
    """
    hook = NotionHook(notion_conn_id="notion_default")

    try:
        # Try to get a non-existent page
        invalid_page_id = "invalid-page-id-12345"
        page = hook.get_page(page_id=invalid_page_id)

    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)

        print("\n❌ Expected error caught:")
        print(f"   Type: {error_type}")
        print(f"   Message: {error_msg}")

        # Check for specific error types
        if "404" in error_msg or "not found" in error_msg.lower():
            print("   → This is a 'Not Found' error")
        elif "401" in error_msg or "unauthorized" in error_msg.lower():
            print("   → This is an 'Authentication' error")
        elif "429" in error_msg or "rate limit" in error_msg.lower():
            print("   → This is a 'Rate Limit' error - consider retry logic")

        print("\n✅ Error handling working correctly")
        return "error_handled"

    return "no_error"


error_handling = PythonOperator(
    task_id="demonstrate_error_handling",
    python_callable=demonstrate_error_handling,
    dag=dag,
)


# Define task dependencies
# =========================
# Run all advanced examples in sequence
(
    query_complex
    >> query_paginated
    >> inspect_schema
    >> create_nested
    >> sync_databases
    >> error_handling
)
