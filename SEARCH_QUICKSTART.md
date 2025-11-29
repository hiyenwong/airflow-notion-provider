# Notion Search 快速开始指南

## 🚀 5 分钟快速上手

### 1. 确保已安装 Provider

```bash
pip install airflow-provider-notion
# 或从源码安装
pip install -e .
```

### 2. 配置 Airflow Connection

在 `airflow_settings.yaml` 中：

```yaml
airflow:
  connections:
    - conn_id: notion_default
      conn_type: notion
      conn_password: ${NOTION_API_TOKEN}
```

或通过环境变量：

```bash
export NOTION_API_TOKEN=ntn_your_token_here
```

### 3. 在 Notion 中授权 Integration

⚠️ **重要**：Search API 只能搜索 Integration 有权限的页面。

1. 打开 Notion 中的页面/数据库
2. 点击右上角 `...` → `Connections`
3. 添加你的 Integration

### 4. 使用 Operator（推荐）

创建 DAG：`my_search_dag.py`

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.notion.operators import NotionSearchOperator

with DAG(
    dag_id="notion_search_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 搜索所有页面
    search_pages = NotionSearchOperator(
        task_id="search_all_pages",
        filter_object_type="page",
        page_size=100
    )
```

### 5. 使用 Hook（高级）

在 PythonOperator 中：

```python
from airflow.operators.python import PythonOperator
from airflow.providers.notion.hooks import NotionHook

def search_notion(**context):
    hook = NotionHook()
    
    # 搜索所有页面
    result = hook.search(
        filter_params={"property": "object", "value": "page"},
        page_size=100
    )
    
    pages = result.get("results", [])
    print(f"找到 {len(pages)} 个页面")
    
    return pages

search_task = PythonOperator(
    task_id="search_pages",
    python_callable=search_notion
)
```

## 📋 常用场景

### 场景 1：搜索所有页面

```python
NotionSearchOperator(
    task_id="search_all_pages",
    filter_object_type="page",
    page_size=100
)
```

### 场景 2：搜索包含关键词的页面

```python
NotionSearchOperator(
    task_id="search_projects",
    query="项目",  # 搜索关键词
    filter_object_type="page"
)
```

### 场景 3：搜索所有数据库

```python
NotionSearchOperator(
    task_id="search_databases",
    filter_object_type="database"
)
```

### 场景 4：分页获取所有结果

```python
def search_all_with_pagination(**context):
    hook = NotionHook()
    all_results = []
    start_cursor = None
    
    while True:
        result = hook.search(
            filter_params={"property": "object", "value": "page"},
            start_cursor=start_cursor,
            page_size=100
        )
        
        all_results.extend(result.get("results", []))
        
        if not result.get("has_more", False):
            break
        start_cursor = result.get("next_cursor")
    
    return all_results
```

## 🔍 参数说明

### NotionSearchOperator 参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `task_id` | str | ✅ | 任务 ID |
| `notion_conn_id` | str | ❌ | 连接 ID（默认 "notion_default"） |
| `query` | str | ❌ | 搜索关键词 |
| `filter_object_type` | str | ❌ | "page", "database", 或 None（全部） |
| `sort_direction` | str | ❌ | "ascending" 或 "descending"（默认） |
| `start_cursor` | str | ❌ | 分页游标 |
| `page_size` | int | ❌ | 每页结果数（最大 100） |

### Hook.search() 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `query` | str | 搜索关键词 |
| `filter_params` | dict | `{"property": "object", "value": "page"}` |
| `sort` | dict | `{"direction": "descending", "timestamp": "last_edited_time"}` |
| `start_cursor` | str | 分页游标 |
| `page_size` | int | 每页结果数 |

## 📊 返回结果格式

```python
{
    "results": [
        {
            "object": "page",
            "id": "page-id",
            "url": "https://notion.so/...",
            "created_time": "2025-01-01T00:00:00.000Z",
            "last_edited_time": "2025-01-02T00:00:00.000Z",
            "properties": {
                "Name": {
                    "type": "title",
                    "title": [
                        {
                            "plain_text": "页面标题",
                            "text": {"content": "页面标题"}
                        }
                    ]
                }
            }
        }
    ],
    "has_more": false,
    "next_cursor": null
}
```

## 🛠️ 测试

### 快速测试脚本

```bash
# 设置 Token
export NOTION_API_TOKEN=ntn_your_token

# 运行测试
python scripts/test_search.py
```

### 在 Python 中测试

```python
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()
result = hook.search(
    filter_params={"property": "object", "value": "page"},
    page_size=10
)

print(f"找到 {len(result.get('results', []))} 个页面")
```

## ⚠️ 常见问题

### 1. 搜索结果为空

**原因**：Integration 没有权限访问页面。

**解决**：
1. 在 Notion 中打开页面
2. 点击 `...` → `Connections`
3. 添加你的 Integration

### 2. 401 Unauthorized

**原因**：Token 配置错误。

**解决**：
- 检查 `conn_password` 字段是否正确
- 确认 Token 格式：`ntn_xxxxx...`
- 验证 Token 没有过期

### 3. 只返回部分结果

**原因**：结果超过 100 条（分页限制）。

**解决**：使用分页逻辑获取所有结果（见场景 4）。

## 📚 更多资源

- 完整示例：`examples/dags/example_notion_search.py`
- 详细文档：`SEARCH_FEATURE.md`
- API 文档：生成的 API 文档
- Notion API：https://developers.notion.com/reference/post-search

## 💡 提示

1. **权限优先**：确保 Integration 有权限访问要搜索的内容
2. **分页处理**：超过 100 条结果时使用分页
3. **关键词搜索**：搜索标题和内容，不搜索属性值
4. **过滤类型**：明确指定 `filter_object_type` 提高效率
5. **错误处理**：在生产环境中添加适当的错误处理

## 🎯 下一步

1. 查看完整示例：`examples/dags/example_notion_search.py`
2. 阅读详细文档：`SEARCH_FEATURE.md`
3. 运行测试脚本验证功能
4. 在你的 DAG 中使用搜索功能

---

**需要帮助？** 查看项目 README 或提交 Issue。

