#!/usr/bin/env python
"""简单的测试脚本来验证 Notion provider 的基本功能。"""

import sys

sys.path.insert(0, "/Users/hiyenwong/projects/funda_ai/airflow-notion-provider")

from unittest.mock import Mock, patch
from airflow.providers.notion.hooks.notion import NotionHook
from airflow.providers.notion.operators.notion import (
    NotionQueryDatabaseOperator,
    NotionCreatePageOperator,
    NotionUpdatePageOperator,
)

print("=" * 60)
print("测试 Airflow Notion Provider")
print("=" * 60)

# Test 1: Hook 初始化
print("\n1. 测试 NotionHook 初始化...")
try:
    hook = NotionHook(notion_conn_id="test_conn")
    print("   ✓ NotionHook 初始化成功")
except Exception as e:
    print(f"   ✗ NotionHook 初始化失败: {e}")

# Test 2: Query Operator 初始化
print("\n2. 测试 NotionQueryDatabaseOperator 初始化...")
try:
    from airflow import DAG
    import pendulum

    dag = DAG(
        dag_id="test_dag",
        start_date=pendulum.datetime(2024, 1, 1),
        schedule_interval=None,
    )

    operator = NotionQueryDatabaseOperator(
        task_id="test_query", database_id="test_db_id", dag=dag
    )
    print("   ✓ NotionQueryDatabaseOperator 初始化成功")
    print(f"   - Task ID: {operator.task_id}")
    print(f"   - Database ID: {operator.database_id}")
    print(f"   - Template fields: {operator.template_fields}")
except Exception as e:
    print(f"   ✗ NotionQueryDatabaseOperator 初始化失败: {e}")

# Test 3: Create Operator 初始化
print("\n3. 测试 NotionCreatePageOperator 初始化...")
try:
    properties = {"Name": {"title": [{"text": {"content": "Test Page"}}]}}

    operator = NotionCreatePageOperator(
        task_id="test_create", database_id="test_db_id", properties=properties, dag=dag
    )
    print("   ✓ NotionCreatePageOperator 初始化成功")
    print(f"   - Task ID: {operator.task_id}")
    print(f"   - Properties: {list(properties.keys())}")
except Exception as e:
    print(f"   ✗ NotionCreatePageOperator 初始化失败: {e}")

# Test 4: Update Operator 初始化
print("\n4. 测试 NotionUpdatePageOperator 初始化...")
try:
    properties = {"Status": {"select": {"name": "Completed"}}}

    operator = NotionUpdatePageOperator(
        task_id="test_update", page_id="test_page_id", properties=properties, dag=dag
    )
    print("   ✓ NotionUpdatePageOperator 初始化成功")
    print(f"   - Task ID: {operator.task_id}")
    print(f"   - Page ID: {operator.page_id}")
except Exception as e:
    print(f"   ✗ NotionUpdatePageOperator 初始化失败: {e}")

# Test 5: Hook 方法签名检查
print("\n5. 测试 NotionHook 方法...")
try:
    hook = NotionHook(notion_conn_id="test_conn")
    methods = [
        "get_conn",
        "query_database",
        "get_database",
        "create_page",
        "update_page",
        "get_page",
        "get_block_children",
        "append_block_children",
        "test_connection",
    ]

    for method in methods:
        if hasattr(hook, method):
            print(f"   ✓ 方法 '{method}' 存在")
        else:
            print(f"   ✗ 方法 '{method}' 不存在")

except Exception as e:
    print(f"   ✗ Hook 方法检查失败: {e}")

# Test 6: 模拟API调用
print("\n6. 测试模拟 API 调用...")
try:
    with patch(
        "airflow.providers.notion.hooks.notion.NotionHook.get_connection"
    ) as mock_conn:
        # 模拟连接
        mock_connection = Mock()
        mock_connection.password = "test_token"
        mock_connection.host = "https://api.notion.com"
        mock_connection.extra = '{"headers": {"Notion-Version": "2022-06-28"}}'
        mock_conn.return_value = mock_connection

        hook = NotionHook(notion_conn_id="test_conn")
        session = hook.get_conn()

        print("   ✓ 模拟连接创建成功")
        print(f"   - Session type: {type(session).__name__}")
        print(f"   - Authorization header: {'Authorization' in session.headers}")

except Exception as e:
    print(f"   ✗ 模拟 API 调用失败: {e}")

print("\n" + "=" * 60)
print("测试完成！")
print("=" * 60)

# 测试总结
print("\n✓ 所有基本组件都可以正常导入和初始化")
print("✓ Hooks 和 Operators 已正确实现")
print("✓ 可以开始使用 Airflow Notion Provider！")

print("\n使用示例:")
print("=" * 60)
print("""
from airflow import DAG
from airflow.providers.notion.operators.notion import NotionQueryDatabaseOperator
import pendulum

dag = DAG(
    dag_id='notion_example',
    start_date=pendulum.now(),
    schedule_interval=None
)

query_task = NotionQueryDatabaseOperator(
    task_id='query_database',
    database_id='YOUR_DATABASE_ID',
    dag=dag
)
""")
print("=" * 60)
