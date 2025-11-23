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
Example DAG: Daily Notion Workflow

This example demonstrates a real-world workflow:
1. Query database for tasks due today
2. Create a daily summary page
3. Update task statuses based on conditions
4. Add completion report

Use case: Daily task management and reporting automation

Schedule: Runs daily at 9 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.notion.operators.notion import (
    NotionQueryDatabaseOperator,
    NotionCreatePageOperator,
)
from airflow.providers.notion.hooks.notion import NotionHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "example_notion_daily_workflow",
    default_args=default_args,
    description="Daily Notion workflow with conditional branching",
    schedule="0 9 * * *",  # Run daily at 9 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "notion", "workflow", "daily"],
)


# Task 1: Query Tasks Due Today
# ==============================
query_due_today = NotionQueryDatabaseOperator(
    task_id="query_tasks_due_today",
    data_source_id='{{ var.value.get("notion_data_source_id", "") }}',
    # Complex filter: Due today AND not completed
    filter_params={
        "and": [
            {
                "property": "Due Date",
                "date": {
                    "equals": "{{ ds }}"  # Airflow execution date (YYYY-MM-DD)
                },
            },
            {"property": "Status", "select": {"does_not_equal": "Completed"}},
        ]
    },
    sorts=[
        {
            "property": "Priority",
            "direction": "ascending",  # High priority first
        }
    ],
    dag=dag,
)


# Task 2: Check if Tasks Exist (Branching Logic)
# ===============================================
def check_tasks_exist(**context):
    """
    Branch based on whether tasks were found.

    Returns:
        'create_summary' if tasks exist
        'no_tasks_skip' if no tasks
    """
    query_result = context["task_instance"].xcom_pull(task_ids="query_tasks_due_today")

    tasks = query_result.get("results", [])
    num_tasks = len(tasks)

    print(f"Found {num_tasks} tasks due today")

    if num_tasks > 0:
        # Store count for later use
        context["task_instance"].xcom_push(key="task_count", value=num_tasks)
        return "create_summary"
    else:
        return "no_tasks_skip"


check_tasks = BranchPythonOperator(
    task_id="check_tasks",
    python_callable=check_tasks_exist,
    dag=dag,
)


# Task 3a: No Tasks (Skip Branch)
# ================================
no_tasks = EmptyOperator(
    task_id="no_tasks_skip",
    dag=dag,
)


# Task 3b: Create Daily Summary Page
# ===================================
def generate_summary_properties(**context):
    """
    Generate properties for summary page based on query results.
    """
    query_result = context["task_instance"].xcom_pull(task_ids="query_tasks_due_today")
    tasks = query_result.get("results", [])
    task_count = len(tasks)

    # Count tasks by status
    status_counts = {}
    for task in tasks:
        status_prop = task["properties"].get("Status", {})
        status = status_prop.get("select", {}).get("name", "Unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    # Build summary text
    summary_lines = [
        f"📊 Daily Task Summary for {context['ds']}",
        "",
        f"Total tasks due: {task_count}",
        "",
        "Breakdown by status:",
    ]

    for status, count in sorted(status_counts.items()):
        summary_lines.append(f"  • {status}: {count}")

    summary_text = "\n".join(summary_lines)

    # Store for summary page creation
    context["task_instance"].xcom_push(key="summary_text", value=summary_text)

    return {
        "Name": {"title": [{"text": {"content": f"Daily Summary - {context['ds']}"}}]},
        "Status": {"select": {"name": "Generated"}},
        "Description": {
            "rich_text": [
                {
                    "text": {
                        "content": f"Automated daily summary: {task_count} tasks due today"
                    }
                }
            ]
        },
    }


# Generate summary data
generate_summary = PythonOperator(
    task_id="generate_summary",
    python_callable=generate_summary_properties,
    dag=dag,
)


# Create summary page
create_summary = NotionCreatePageOperator(
    task_id="create_summary",
    data_source_id='{{ var.value.get("notion_data_source_id", "") }}',
    properties="{{ task_instance.xcom_pull(task_ids='generate_summary') }}",
    children=[
        {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [
                    {"type": "text", "text": {"content": "📋 Daily Task Report"}}
                ]
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
                            "content": '{{ task_instance.xcom_pull(task_ids="generate_summary", key="summary_text") }}'
                        },
                    }
                ]
            },
        },
        {"object": "block", "type": "divider", "divider": {}},
    ],
    dag=dag,
)


# Task 4: Process Each Task
# ==========================
def process_tasks(**context):
    """
    Process each task and update status if needed.

    Example logic:
    - If task is "To Do" and due today, mark as "In Progress"
    - Add automated comment to each processed task
    """
    hook = NotionHook(notion_conn_id="notion_default")
    query_result = context["task_instance"].xcom_pull(task_ids="query_tasks_due_today")
    tasks = query_result.get("results", [])

    processed_count = 0

    for task in tasks:
        page_id = task["id"]
        status_prop = task["properties"].get("Status", {})
        current_status = status_prop.get("select", {}).get("name", "")

        # Update logic: If "To Do", change to "In Progress"
        if current_status == "To Do":
            print(f"Updating task {page_id} from 'To Do' to 'In Progress'")

            hook.update_page(
                page_id=page_id,
                properties={"Status": {"select": {"name": "In Progress"}}},
            )

            # Add automated comment
            hook.append_block_children(
                block_id=page_id,
                children=[
                    {
                        "object": "block",
                        "type": "callout",
                        "callout": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {
                                        "content": f'🤖 Automated: Status changed to "In Progress" by daily workflow on {context["ds"]}'
                                    },
                                }
                            ],
                            "icon": {"emoji": "🤖"},
                            "color": "gray_background",
                        },
                    }
                ],
            )

            processed_count += 1

    print(f"Processed {processed_count} tasks")
    context["task_instance"].xcom_push(key="processed_count", value=processed_count)

    return processed_count


process_tasks_op = PythonOperator(
    task_id="process_tasks",
    python_callable=process_tasks,
    dag=dag,
)


# Task 5: Update Summary with Processing Results
# ===============================================
def update_summary_with_results(**context):
    """
    Add processing results to the summary page.
    """
    hook = NotionHook(notion_conn_id="notion_default")

    # Get summary page ID
    summary_page_id = context["task_instance"].xcom_pull(
        task_ids="create_summary", key="page_id"
    )

    processed_count = context["task_instance"].xcom_pull(
        task_ids="process_tasks", key="processed_count"
    )

    # Add completion block
    hook.append_block_children(
        block_id=summary_page_id,
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "✅ Processing Complete"}}
                    ]
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
                                "content": f"Successfully processed {processed_count} tasks.\n"
                                f"Workflow completed at {datetime.now().strftime('%H:%M:%S')}"
                            },
                        }
                    ]
                },
            },
        ],
    )

    # Update summary page status
    hook.update_page(
        page_id=summary_page_id,
        properties={"Status": {"select": {"name": "Completed"}}},
    )

    print(f"Updated summary page {summary_page_id}")


update_summary = PythonOperator(
    task_id="update_summary",
    python_callable=update_summary_with_results,
    trigger_rule="none_failed",  # Run even if some tasks were skipped
    dag=dag,
)


# Task 6: Cleanup / Final Step
# =============================
workflow_complete = EmptyOperator(
    task_id="workflow_complete",
    trigger_rule="none_failed",
    dag=dag,
)


# Define task dependencies
# =========================
# Query → Check → Branch into [no_tasks OR (generate→create→process→update)] → complete
query_due_today >> check_tasks

# Branch 1: No tasks
check_tasks >> no_tasks >> workflow_complete

# Branch 2: Process tasks
(
    check_tasks
    >> generate_summary
    >> create_summary
    >> process_tasks_op
    >> update_summary
    >> workflow_complete
)
