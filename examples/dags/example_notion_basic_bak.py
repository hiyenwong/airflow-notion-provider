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
Example DAG: Complete Notion CRUD Operations

本示例演示 Notion 的完整 CRUD 操作，包括：

一、Database CRUD 操作：
  1. Query Database (读取) - 查询数据库并过滤结果
  2. Get Database (读取) - 获取数据库元数据
  3. Create Page in Database (创建) - 在数据库中创建页面
  4. Update Page in Database (更新) - 更新数据库中的页面
  5. Add Comment to Database Page (评论) - 给数据库页面添加评论

二、Page CRUD 操作（独立页面，非数据库页面）：
  6. Create Standalone Page (创建) - 创建独立页面
  7. Get Page (读取) - 读取页面详情
  8. Update Page (更新) - 更新页面属性
  9. Add Comment to Page (评论) - 给页面添加评论
  10. Delete Page (删除) - 归档/删除页面

前置条件：
1. 在 Airflow UI 中配置 Notion 连接：
   - Connection ID: notion_default
   - Connection Type: notion
   - Password: your_notion_api_token (ntn_xxxxx 或 secret_xxxxx)

2. 配置 Airflow Variables：
   - notion_database_id: 你的 Notion 数据库 ID
   - notion_data_source_id: 你的数据源 ID (推荐用于 API 2025-09-03+)
   - notion_parent_page_id: 用于创建独立页面的父页面 ID
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
    description="Complete Notion CRUD operations for Database and Page",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "notion", "crud", "database", "page"],
)


# ============================================================================
# 第一部分：Database CRUD 操作
# ============================================================================


# Database Task 1: Query Database (读取 - Read)
# ==============================================
def query_database_task(**context):
    """
    查询 Notion 数据库并过滤结果。

    功能：
    - 使用 NotionQueryDatabaseOperator 查询数据库
    - 支持过滤条件（如按状态筛选）
    - 支持排序（如按创建时间排序）
    - 支持分页（page_size 限制返回数量）

    返回：包含查询结果的字典，包括 results 数组
    """
    print("=== Database CRUD: Query Database ===")
    return "query_database"


query_database = NotionQueryDatabaseOperator(
    task_id="db_query_database",
    data_source_id="{{ var.value.notion_data_source_id }}",
    # 过滤条件：查询状态为 "In progress" 的页面
    filter_params={"property": "Status", "status": {"equals": "In progress"}},
    # 排序：按创建时间降序
    sorts=[{"property": "Created time", "direction": "descending"}],
    page_size=10,  # 限制返回 10 条结果
    dag=dag,
)


# Database Task 2: Get Database Info (读取 - Read)
# =================================================
def get_database_info(**context):
    """
    获取数据库的元数据信息。

    功能：
    - 获取数据库的标题、描述
    - 获取数据库的属性定义（properties schema）
    - 获取数据库的 data_sources 信息

    返回：数据库对象，包含 title, properties, data_sources 等信息
    """
    import traceback

    print("=== Database CRUD: Get Database Info ===")
    hook = NotionHook(notion_conn_id="notion_default")
    database_id = context["var"]["value"].get("notion_database_id")

    if not database_id:
        raise ValueError("Please set 'notion_database_id' in Airflow Variables")

    print(f"尝试访问数据库 ID: {database_id}")
    print(f"API Base URL: {hook.base_url}")

    try:
        # 获取数据库信息
        database = hook.get_database(database_id=database_id)

        print("\n✅ 数据库访问成功!")
        print(f"数据库 ID: {database['id']}")
        print(
            f"数据库标题: {database.get('title', [{}])[0].get('plain_text', 'Untitled')}"
        )
        print(f"创建时间: {database['created_time']}")
        print(f"最后编辑时间: {database['last_edited_time']}")

        # 打印数据库属性
        print("\n数据库属性 (Properties):")
        for prop_name, prop_config in database.get("properties", {}).items():
            print(f"  - {prop_name}: {prop_config['type']}")

        # 打印 data_sources
        print(f"\nData Sources 数量: {len(database.get('data_sources', []))}")
        for ds in database.get("data_sources", []):
            print(f"  - Data Source ID: {ds['id']}")
            print(f"    Type: {ds.get('type', 'N/A')}")

        return database

    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ 错误详情")
        print("=" * 80)
        print(f"错误类型: {type(e).__name__}")
        print(f"错误消息: {str(e)}")
        print("\n完整堆栈跟踪:")
        print(traceback.format_exc())

        error_msg = str(e)

        if "404" in error_msg:
            print("\n" + "=" * 80)
            print("🔍 404 错误诊断")
            print("=" * 80)
            print(f"请求的 Database ID: {database_id}")
            print(f"请求的 URL: https://api.notion.com/v1/databases/{database_id}")

            # 尝试作为 Page 读取
            print("\n" + "-" * 80)
            print("尝试将其作为 Page（页面）读取...")
            print("-" * 80)
            try:
                page = hook.get_page(page_id=database_id)

                print("✅ 成功！这是一个 Page（页面），不是 Database（数据库）!")
                print("\nPage 详情:")
                print(f"  - Page ID: {page['id']}")
                print(f"  - Page URL: {page['url']}")
                print(f"  - Object Type: {page['object']}")
                print(f"  - Created: {page['created_time']}")
                print(f"  - Last Edited: {page['last_edited_time']}")
                print(f"  - Archived: {page.get('archived', False)}")

                # 尝试获取页面标题
                properties = page.get("properties", {})
                if "title" in properties:
                    title_prop = properties["title"]
                    if title_prop.get("title"):
                        title_text = title_prop["title"][0].get("plain_text", "")
                        print(f"  - Title: {title_text}")

                # 获取父对象信息
                parent = page.get("parent", {})
                parent_type = parent.get("type", "unknown")
                print("\nParent 信息:")
                print(f"  - Type: {parent_type}")
                if parent_type == "page_id":
                    print(f"  - Parent Page ID: {parent.get('page_id')}")
                elif parent_type == "workspace":
                    print("  - Parent: Workspace Root")

                print("\n" + "=" * 80)
                print("📌 解决方案")
                print("=" * 80)
                print("\n方案 1: 查找页面中的内嵌数据库")
                print("-" * 40)
                print("如果这个页面包含表格/数据库视图，运行以下代码查找内嵌数据库:")
                print("\nfrom airflow.providers.notion.hooks.notion import NotionHook")
                print("hook = NotionHook('notion_default')")
                print(f"blocks = hook.get_block_children(block_id='{database_id}')")
                print("for block in blocks['results']:")
                print("    if block['type'] == 'child_database':")
                print("        print(f\"Found database: {block['id']}\")")

                print("\n方案 2: 使用此 Page ID 作为父页面")
                print("-" * 40)
                print("如果您想在此页面下创建子页面，配置为:")
                print(f"airflow variables set notion_parent_page_id '{database_id}'")

                print("\n方案 3: 手动在 Notion 中查找 Database")
                print("-" * 40)
                print("1. 打开此页面: " + page["url"])
                print("2. 向下滚动，找到表格/数据库视图")
                print("3. 点击数据库右上角的 '⋮⋮' (三个点) 图标")
                print("4. 选择 'Copy link to view'")
                print("5. 从新 URL 中提取 database_id (在 ?v= 之前的部分)")
                print("6. 设置变量:")
                print(
                    "   airflow variables set notion_database_id '<从URL提取的database_id>'"
                )

                print("\n方案 4: 列出所有可访问的数据库")
                print("-" * 40)
                print("运行以下脚本查看所有数据库:")
                print("cd /Users/hiyenwong/projects/funda_ai/airflow-notion-provider")
                print("python scripts/list_databases.py")
                print("=" * 80 + "\n")

            except Exception as page_error:
                print(
                    f"\n⚠️  无法作为 Page 读取: {type(page_error).__name__}: {str(page_error)}"
                )
                print("\n" + "=" * 80)
                print("可能的原因")
                print("=" * 80)
                print("\n1. ❌ Integration 没有访问权限")
                print("   解决方法:")
                print("   a) 在 Notion 中打开这个页面/数据库")
                print("   b) 点击右上角的 '...' 菜单")
                print("   c) 选择 'Add connections' 或 'Connections'")
                print("   d) 添加你的 Notion Integration")
                print("   e) 确认授权")

                print("\n2. ❌ ID 格式错误或不存在")
                print(f"   当前 ID: {database_id}")
                print("   ID 应该是 32 位十六进制字符串 (带或不带连字符)")
                print("   正确格式:")
                print("   - 2afd1aa7fe2f80b0af0e000b1a23eb97 (不带连字符)")
                print("   - 2afd1aa7-fe2f-80b0-af0e-000b1a23eb97 (带连字符)")

                print("\n3. ❌ API Token 无效")
                print(
                    "   检查 Airflow Connection: Admin → Connections → notion_default"
                )
                print("   Password 字段应该包含有效的 Notion API Token")
                print("   格式: ntn_xxxxxxxxxx 或 secret_xxxxxxxxxx")
                print("=" * 80 + "\n")

        elif "401" in error_msg or "Unauthorized" in error_msg:
            print("\n" + "=" * 80)
            print("🔍 401 Unauthorized 错误诊断")
            print("=" * 80)
            print("API Token 无效、已过期或未正确配置")
            print("\n解决方法:")
            print("1. 检查 Airflow Connection:")
            print("   - 在 Airflow UI: Admin → Connections → notion_default")
            print("   - 确认 Password 字段有值")
            print("   - Token 格式: ntn_xxx 或 secret_xxx")
            print("\n2. 重新生成 API Token:")
            print("   - 访问: https://www.notion.so/my-integrations")
            print("   - 创建或重新生成 Integration Token")
            print("   - 复制新 Token 到 Airflow Connection")
            print("\n3. 确认 Integration 的 Capabilities:")
            print("   - 在 Notion Integration 设置中")
            print("   - 确保启用了 'Read content', 'Update content' 等权限")
            print("=" * 80 + "\n")

        elif "403" in error_msg or "Forbidden" in error_msg:
            print("\n" + "=" * 80)
            print("🔍 403 Forbidden 错误诊断")
            print("=" * 80)
            print("Integration 有 Token，但没有访问此资源的权限")
            print("\n解决方法:")
            print("1. 在 Notion 中添加 Integration 到页面/数据库:")
            print("   a) 打开目标页面/数据库")
            print("   b) 点击右上角 '...' → 'Add connections'")
            print("   c) 选择你的 Integration")
            print("=" * 80 + "\n")

        else:
            print("\n" + "=" * 80)
            print("🔍 其他错误")
            print("=" * 80)
            print("这可能是网络问题、API 限流或其他未知错误")
            print("\n建议:")
            print("1. 检查网络连接")
            print("2. 稍后重试（可能是 API 限流）")
            print("3. 查看 Notion API 状态: https://status.notion.so/")
            print("=" * 80 + "\n")

        # 重新抛出异常，让 Airflow 标记任务为失败
        raise


get_database = PythonOperator(
    task_id="db_get_database",
    python_callable=get_database_info,
    dag=dag,
)


# Database Task 3: Create Page in Database (创建 - Create)
# =========================================================
create_db_page = NotionCreatePageOperator(
    task_id="db_create_page",
    data_source_id="{{ var.value.notion_data_source_id }}",
    properties={
        # 必填：Name 是 title 类型
        "Name": {
            "title": [
                {"text": {"content": "Database Page - Created by Airflow on {{ ds }}"}}
            ]
        },
        # 可选：Text 字段
        "Text": {
            "rich_text": [
                {
                    "text": {
                        "content": "这是一个在数据库中创建的页面示例。This page is created in database."
                    }
                }
            ]
        },
    },
    children=[
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [
                    {"type": "text", "text": {"content": "📊 Database Page Content"}}
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
                            "content": "这个页面是通过 Airflow 在数据库中自动创建的。"
                        },
                    }
                ]
            },
        },
    ],
    dag=dag,
)


# Database Task 4: Update Database Page (更新 - Update)
# ======================================================
update_db_page = NotionUpdatePageOperator(
    task_id="db_update_page",
    # 从前一个任务获取 page_id（通过 XCom）
    page_id="{{ task_instance.xcom_pull(task_ids='db_create_page', key='page_id') }}",
    properties={
        # 更新 Text 字段
        "Text": {
            "rich_text": [
                {
                    "text": {
                        "content": "✅ Updated: This database page was modified by Airflow on {{ ds }}."
                    }
                }
            ]
        },
    },
    dag=dag,
)


# Database Task 5: Add Comment to Database Page (评论 - Comment)
# ===============================================================
def add_comment_to_db_page(**context):
    """
    给数据库页面添加评论（使用 callout block）。

    功能：
    - 使用 callout block 模拟评论功能
    - 添加带有表情符号和背景色的评论块

    注意：Notion API 2025-09-03 没有专门的评论 API，
          需要使用 callout block 来实现评论功能。

    返回：添加评论后的响应对象
    """
    print("=== Database CRUD: Add Comment to Database Page ===")
    hook = NotionHook(notion_conn_id="notion_default")

    # 从前一个任务获取 page_id
    page_id = context["task_instance"].xcom_pull(
        task_ids="db_create_page", key="page_id"
    )

    if not page_id:
        print("⚠️  No page_id found, skipping comment addition")
        return

    # 创建评论块
    comment_blocks = [
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "💬 Comment: Database page processed successfully at {{ ts }}!"
                        },
                    }
                ],
                "icon": {"emoji": "✅"},
                "color": "green_background",
            },
        }
    ]

    result = hook.append_block_children(block_id=page_id, children=comment_blocks)
    print(f"✅ Added comment to database page: {page_id}")

    return result


add_db_comment = PythonOperator(
    task_id="db_add_comment",
    python_callable=add_comment_to_db_page,
    dag=dag,
)


# ============================================================================
# 第二部分：Page CRUD 操作（独立页面，非数据库页面）
# ============================================================================


# Page Task 6: Create Standalone Page (创建 - Create)
# ====================================================
def create_standalone_page(**context):
    """
    创建一个独立的 Notion 页面（不在数据库中）。

    功能：
    - 在指定的父页面下创建子页面
    - 设置页面标题和内容块
    - 不包含数据库属性（properties）

    返回：创建的页面对象，包含 page_id
    """
    print("=== Page CRUD: Create Standalone Page ===")
    hook = NotionHook(notion_conn_id="notion_default")
    parent_page_id = context["var"]["value"].get("notion_parent_page_id")

    if not parent_page_id:
        raise ValueError("Please set 'notion_parent_page_id' in Airflow Variables")

    # 创建独立页面的数据
    page_data = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "properties": {
            "title": {
                "title": [
                    {
                        "text": {
                            "content": f"Standalone Page - Created on {context['ds']}"
                        }
                    }
                ]
            }
        },
        "children": [
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [
                        {"type": "text", "text": {"content": "📄 独立页面示例"}}
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
                                "content": "这是一个独立的页面，不属于任何数据库。"
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
                        {
                            "type": "text",
                            "text": {"content": f"执行日期: {context['ds']}"},
                        }
                    ]
                },
            },
            {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": f"任务实例: {context['task_instance'].task_id}"
                            },
                        }
                    ]
                },
            },
        ],
    }

    # 使用 requests 直接调用 API（因为 create_page 默认用于数据库）
    session = hook.get_conn()
    url = f"{hook.base_url}/pages"
    response = session.post(url, json=page_data)
    response.raise_for_status()
    page = response.json()

    page_id = page["id"]
    print(f"✅ Created standalone page: {page_id}")
    print(f"   Page URL: {page['url']}")

    # 将 page_id 推送到 XCom 供后续任务使用
    context["task_instance"].xcom_push(key="standalone_page_id", value=page_id)

    return page


create_page = PythonOperator(
    task_id="page_create_standalone",
    python_callable=create_standalone_page,
    dag=dag,
)


# Page Task 7: Get Page (读取 - Read)
# ====================================
def get_page_details(**context):
    """
    读取页面的详细信息。

    功能：
    - 获取页面的所有属性（properties）
    - 获取页面的 URL、创建时间、最后编辑时间
    - 获取页面的父对象信息（parent）

    返回：页面对象，包含完整的页面信息
    """
    print("=== Page CRUD: Get Page Details ===")
    hook = NotionHook(notion_conn_id="notion_default")

    # 从前一个任务获取 page_id
    page_id = context["task_instance"].xcom_pull(
        task_ids="page_create_standalone", key="standalone_page_id"
    )

    if not page_id:
        print("⚠️  No page_id found, skipping")
        return

    # 获取页面详情
    page = hook.get_page(page_id=page_id)

    print(f"\n页面 ID: {page['id']}")
    print(f"页面 URL: {page['url']}")
    print(f"创建时间: {page['created_time']}")
    print(f"最后编辑时间: {page['last_edited_time']}")
    print(f"是否已归档: {page.get('archived', False)}")

    # 打印父对象信息
    parent = page.get("parent", {})
    print(f"\n父对象类型: {parent.get('type', 'N/A')}")

    # 打印页面属性
    print("\n页面属性 (Properties):")
    for prop_name, prop_value in page.get("properties", {}).items():
        print(f"  - {prop_name}: {prop_value.get('type', 'N/A')}")

    return page


get_page = PythonOperator(
    task_id="page_get_details",
    python_callable=get_page_details,
    dag=dag,
)


# Page Task 8: Update Page (更新 - Update)
# =========================================
def update_page_properties(**context):
    """
    更新页面的属性。

    功能：
    - 更新页面的 title（如果是独立页面）
    - 可以更新任何可编辑的属性

    注意：独立页面只有 title 属性，数据库页面有更多属性

    返回：更新后的页面对象
    """
    print("=== Page CRUD: Update Page ===")
    hook = NotionHook(notion_conn_id="notion_default")

    # 从前一个任务获取 page_id
    page_id = context["task_instance"].xcom_pull(
        task_ids="page_create_standalone", key="standalone_page_id"
    )

    if not page_id:
        print("⚠️  No page_id found, skipping")
        return

    # 更新页面属性
    updated_page = hook.update_page(
        page_id=page_id,
        properties={
            "title": {
                "title": [
                    {
                        "text": {
                            "content": f"✅ Updated Standalone Page - Modified on {context['ds']}"
                        }
                    }
                ]
            }
        },
    )

    print(f"✅ Updated page: {page_id}")
    print(f"   New title: Updated Standalone Page - Modified on {context['ds']}")

    return updated_page


update_page = PythonOperator(
    task_id="page_update_properties",
    python_callable=update_page_properties,
    dag=dag,
)


# Page Task 9: Add Comment to Page (评论 - Comment)
# ==================================================
def add_comment_to_page(**context):
    """
    给独立页面添加评论（使用 callout block）。

    功能：
    - 在页面末尾添加评论块
    - 使用不同颜色和图标来区分评论类型

    返回：添加评论后的响应对象
    """
    print("=== Page CRUD: Add Comment to Page ===")
    hook = NotionHook(notion_conn_id="notion_default")

    # 从前一个任务获取 page_id
    page_id = context["task_instance"].xcom_pull(
        task_ids="page_create_standalone", key="standalone_page_id"
    )

    if not page_id:
        print("⚠️  No page_id found, skipping comment addition")
        return

    # 创建评论块（使用不同样式）
    comment_blocks = [
        {
            "object": "block",
            "type": "divider",
            "divider": {},
        },
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": f"💬 评论: 页面处理完成 - {context['ts']}"},
                    }
                ],
                "icon": {"emoji": "💭"},
                "color": "blue_background",
            },
        },
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": "✅ 所有页面操作已成功完成！All page operations completed successfully!"
                        },
                    }
                ],
                "icon": {"emoji": "🎉"},
                "color": "green_background",
            },
        },
    ]

    result = hook.append_block_children(block_id=page_id, children=comment_blocks)
    print(f"✅ Added comments to page: {page_id}")

    return result


add_page_comment = PythonOperator(
    task_id="page_add_comment",
    python_callable=add_comment_to_page,
    dag=dag,
)


# Page Task 10: Delete Page (删除 - Delete/Archive)
# ==================================================
def delete_page(**context):
    """
    删除（归档）页面。

    功能：
    - 将页面设置为 archived 状态
    - 归档的页面可以恢复

    注意：Notion API 使用 "archive" 而不是真正的删除

    返回：归档后的页面对象
    """
    print("=== Page CRUD: Delete (Archive) Page ===")
    hook = NotionHook(notion_conn_id="notion_default")

    # 从前一个任务获取 page_id
    page_id = context["task_instance"].xcom_pull(
        task_ids="page_create_standalone", key="standalone_page_id"
    )

    if not page_id:
        print("⚠️  No page_id found, skipping deletion")
        return

    # 归档页面（通过更新 archived 属性）
    session = hook.get_conn()
    url = f"{hook.base_url}/pages/{page_id}"
    response = session.patch(url, json={"archived": True})
    response.raise_for_status()
    archived_page = response.json()

    print(f"✅ Archived (deleted) page: {page_id}")
    print(f"   Archived status: {archived_page.get('archived', False)}")

    return archived_page


delete_page = PythonOperator(
    task_id="page_delete_archive",
    python_callable=delete_page,
    dag=dag,
)


# ============================================================================
# 定义任务依赖关系
# ============================================================================

# Database CRUD 流程（线性）
query_database >> get_database >> create_db_page >> update_db_page >> add_db_comment

# Page CRUD 流程（线性）
create_page >> get_page >> update_page >> add_page_comment >> delete_page

# 两个流程并行执行（Database CRUD 完成后再执行 Page CRUD）
add_db_comment >> create_page
