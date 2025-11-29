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
Example DAG: Notion Search Operations

This example demonstrates how to use the Notion Search API to find pages
and databases in your workspace.

Prerequisites:
1. Set up Notion connection in Airflow UI:
   - Connection ID: notion_default
   - Connection Type: notion
   - Password: your_notion_api_token (ntn_xxxxx or secret_xxxxx)

2. Ensure your Notion Integration has access to the pages/databases you want to search.
   The Search API only returns results that the integration has permission to access.

Features demonstrated:
- Search all pages in workspace
- Search with keyword filter
- Search specific object types (pages vs databases)
- Pagination for large result sets
- Process search results
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.notion.operators.notion import NotionSearchOperator
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
    "example_notion_search",
    default_args=default_args,
    description="Notion Search API examples",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "notion", "search"],
)


# Task 1: Search All Pages
# =========================
# Search for all pages that the integration has access to
search_all_pages = NotionSearchOperator(
    task_id="search_all_pages",
    notion_conn_id="notion_default",
    filter_object_type="page",  # Only return pages (not databases)
    sort_direction="descending",  # Most recently edited first
    page_size=100,  # Maximum results per request
    dag=dag,
)


# Task 2: Search with Keyword
# ============================
# Search for pages containing specific keywords
search_with_keyword = NotionSearchOperator(
    task_id="search_with_keyword",
    notion_conn_id="notion_default",
    query="project",  # Search for pages containing "project"
    filter_object_type="page",
    page_size=50,
    dag=dag,
)


# Task 3: Search All Databases
# =============================
# Find all databases in the workspace
search_databases = NotionSearchOperator(
    task_id="search_databases",
    notion_conn_id="notion_default",
    filter_object_type="database",  # Only return databases
    page_size=100,
    dag=dag,
)


# Task 4: Search Everything
# ==========================
# Search both pages and databases (no filter)
search_everything = NotionSearchOperator(
    task_id="search_everything",
    notion_conn_id="notion_default",
    # No filter_object_type = returns both pages and databases
    page_size=50,
    dag=dag,
)


# Task 5: Process Search Results
# ===============================
# Demonstrate how to process search results in a custom function
def process_search_results(**context):
    """
    Process search results from previous task.
    
    This shows how to:
    - Extract search results from XCom
    - Parse page/database information
    - Handle pagination
    """
    # Get results from previous task
    search_result = context["task_instance"].xcom_pull(task_ids="search_all_pages")
    
    if not search_result:
        print("No search results found")
        return
    
    results = search_result.get("results", [])
    has_more = search_result.get("has_more", False)
    next_cursor = search_result.get("next_cursor")
    
    print(f"\n{'='*60}")
    print(f"Search Results Summary")
    print(f"{'='*60}")
    print(f"Total results in this batch: {len(results)}")
    print(f"Has more results: {has_more}")
    if has_more:
        print(f"Next cursor: {next_cursor}")
    print()
    
    # Process each result
    for i, item in enumerate(results, 1):
        object_type = item.get("object")
        item_id = item.get("id")
        url = item.get("url", "N/A")
        created_time = item.get("created_time", "N/A")
        last_edited_time = item.get("last_edited_time", "N/A")
        
        print(f"{i}. {object_type.upper()}")
        print(f"   ID: {item_id}")
        print(f"   URL: {url}")
        
        # Extract title
        title = "Untitled"
        if object_type == "page":
            properties = item.get("properties", {})
            # Try to find title property
            for prop_name, prop_value in properties.items():
                if prop_value.get("type") == "title":
                    title_array = prop_value.get("title", [])
                    if title_array:
                        title = "".join([t.get("plain_text", "") for t in title_array])
                    break
        elif object_type == "database":
            title_array = item.get("title", [])
            if title_array:
                title = "".join([t.get("plain_text", "") for t in title_array])
        
        print(f"   Title: {title}")
        print(f"   Created: {created_time}")
        print(f"   Last Edited: {last_edited_time}")
        print()
    
    print(f"{'='*60}\n")
    
    return {
        "total_results": len(results),
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


process_results = PythonOperator(
    task_id="process_results",
    python_callable=process_search_results,
    dag=dag,
)


# Task 6: Paginated Search
# =========================
# Demonstrate how to handle pagination for large result sets
def search_all_pages_paginated(**context):
    """
    Search all pages with automatic pagination.
    
    This function demonstrates how to retrieve all pages when
    there are more than 100 results (Notion's page size limit).
    """
    hook = NotionHook(notion_conn_id="notion_default")
    
    all_pages = []
    start_cursor = None
    page_num = 0
    
    print("\n" + "="*60)
    print("Paginated Search: Fetching All Pages")
    print("="*60 + "\n")
    
    while True:
        page_num += 1
        print(f"Fetching page {page_num}...")
        
        # Search with pagination
        result = hook.search(
            filter_params={"property": "object", "value": "page"},
            sort={"direction": "descending", "timestamp": "last_edited_time"},
            start_cursor=start_cursor,
            page_size=100,  # Maximum allowed
        )
        
        pages = result.get("results", [])
        all_pages.extend(pages)
        
        has_more = result.get("has_more", False)
        start_cursor = result.get("next_cursor")
        
        print(f"  Retrieved {len(pages)} pages")
        print(f"  Total so far: {len(all_pages)}")
        print(f"  Has more: {has_more}\n")
        
        if not has_more:
            break
    
    print("="*60)
    print(f"✅ Pagination complete!")
    print(f"   Total pages retrieved: {len(all_pages)}")
    print("="*60 + "\n")
    
    # Store all pages in XCom
    context["task_instance"].xcom_push(key="all_pages", value=all_pages)
    context["task_instance"].xcom_push(key="total_count", value=len(all_pages))
    
    return len(all_pages)


paginated_search = PythonOperator(
    task_id="paginated_search",
    python_callable=search_all_pages_paginated,
    dag=dag,
)


# Task 7: Search and Filter
# ==========================
# Search pages and then filter results based on custom criteria
def search_and_filter(**context):
    """
    Search pages and apply custom filtering logic.
    
    Example: Find pages edited in the last 7 days
    """
    hook = NotionHook(notion_conn_id="notion_default")
    
    # Search all pages
    result = hook.search(
        filter_params={"property": "object", "value": "page"},
        sort={"direction": "descending", "timestamp": "last_edited_time"},
        page_size=100,
    )
    
    pages = result.get("results", [])
    
    # Filter: Pages edited in last 7 days
    from datetime import datetime, timezone
    
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
    recent_pages = []
    
    for page in pages:
        last_edited = page.get("last_edited_time")
        if last_edited:
            edited_time = datetime.fromisoformat(last_edited.replace("Z", "+00:00"))
            if edited_time > seven_days_ago:
                recent_pages.append(page)
    
    print(f"\n{'='*60}")
    print(f"Pages edited in last 7 days: {len(recent_pages)}")
    print(f"{'='*60}\n")
    
    for page in recent_pages:
        title = "Untitled"
        properties = page.get("properties", {})
        for prop_name, prop_value in properties.items():
            if prop_value.get("type") == "title":
                title_array = prop_value.get("title", [])
                if title_array:
                    title = "".join([t.get("plain_text", "") for t in title_array])
                break
        
        print(f"- {title}")
        print(f"  Last edited: {page.get('last_edited_time')}")
        print(f"  URL: {page.get('url')}\n")
    
    return {
        "total_pages": len(pages),
        "recent_pages": len(recent_pages),
    }


search_filter = PythonOperator(
    task_id="search_and_filter",
    python_callable=search_and_filter,
    dag=dag,
)


# Define task dependencies
# =========================
# Run searches in parallel, then process results
[search_all_pages, search_with_keyword, search_databases, search_everything] >> process_results

# Run paginated search and filter independently
paginated_search
search_filter

