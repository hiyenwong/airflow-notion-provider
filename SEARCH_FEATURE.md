# Notion Search Feature

## 概述

本次更新为 Airflow Notion Provider 添加了完整的搜索功能，允许用户搜索 Notion 工作区中的所有页面和数据库。

## 新增功能

### 1. NotionHook.search() 方法

在 `airflow/providers/notion/hooks/notion.py` 中添加了新的 `search()` 方法。

**功能特性**：
- 搜索工作区中的所有页面和数据库
- 支持关键词搜索
- 支持按对象类型过滤（page/database）
- 支持排序（按最后编辑时间）
- 支持分页（最大 100 条/页）
- 完整的错误处理和日志记录

**方法签名**：
```python
def search(
    self,
    query: Optional[str] = None,
    filter_params: Optional[Dict[str, Any]] = None,
    sort: Optional[Dict[str, str]] = None,
    start_cursor: Optional[str] = None,
    page_size: Optional[int] = None,
) -> Dict[str, Any]
```

**使用示例**：
```python
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()

# 搜索所有页面
results = hook.search(
    filter_params={"property": "object", "value": "page"},
    page_size=100
)

# 搜索包含关键词的页面
results = hook.search(
    query="project",
    filter_params={"property": "object", "value": "page"}
)

# 搜索所有数据库
results = hook.search(
    filter_params={"property": "object", "value": "database"}
)
```

### 2. NotionSearchOperator

在 `airflow/providers/notion/operators/notion.py` 中添加了新的 Operator。

**功能特性**：
- 简化的 Operator 接口
- 自动处理过滤器和排序参数
- 支持 Jinja 模板
- 自动将结果推送到 XCom
- 支持分页（通过 next_cursor）

**参数**：
- `notion_conn_id`: 连接 ID（默认 "notion_default"）
- `query`: 搜索关键词（可选）
- `filter_object_type`: 对象类型过滤（"page", "database", 或 None）
- `sort_direction`: 排序方向（"ascending" 或 "descending"）
- `start_cursor`: 分页游标（可选）
- `page_size`: 每页结果数（最大 100）

**使用示例**：
```python
from airflow.providers.notion.operators import NotionSearchOperator

# 搜索所有页面
search_pages = NotionSearchOperator(
    task_id="search_pages",
    filter_object_type="page",
    page_size=100
)

# 搜索包含关键词的页面
search_projects = NotionSearchOperator(
    task_id="search_projects",
    query="project",
    filter_object_type="page"
)
```

### 3. 示例 DAG

创建了完整的示例 DAG：`examples/dags/example_notion_search.py`

**包含的示例**：
1. 搜索所有页面
2. 搜索包含关键词的页面
3. 搜索所有数据库
4. 搜索所有内容（页面 + 数据库）
5. 处理搜索结果
6. 分页搜索（获取所有结果）
7. 搜索并过滤（自定义逻辑）

### 4. 测试脚本

创建了测试脚本：`scripts/test_search.py`

**功能**：
- 验证 search() 方法是否正常工作
- 测试不同的搜索参数
- 显示搜索结果

**使用方法**：
```bash
export NOTION_API_TOKEN=ntn_your_token
python scripts/test_search.py
```

## 更新的文件

### 核心代码
1. ✅ `airflow/providers/notion/hooks/notion.py` - 添加 search() 方法
2. ✅ `airflow/providers/notion/operators/notion.py` - 添加 NotionSearchOperator
3. ✅ `airflow/providers/notion/operators/__init__.py` - 导出新 Operator

### 文档和示例
4. ✅ `README.md` - 更新文档，添加搜索功能说明
5. ✅ `examples/dags/example_notion_search.py` - 完整的搜索示例 DAG
6. ✅ `scripts/test_search.py` - 测试脚本
7. ✅ `SEARCH_FEATURE.md` - 本文档

## API 限制

### 权限限制
Search API **只能搜索 Integration 有权限访问的页面和数据库**。

要搜索整个工作区：
1. 在 Notion 中打开要搜索的页面/数据库
2. 点击右上角 `...` → `Connections`
3. 添加你的 Integration

### 分页限制
- 每次请求最多返回 100 个结果
- 使用 `start_cursor` 进行分页
- `has_more` 字段指示是否有更多结果

### 搜索范围
- 搜索标题和内容
- 不搜索属性值
- 不支持复杂的过滤条件（不像 database query）

## 使用场景

### 1. 查找所有页面
```python
search_all = NotionSearchOperator(
    task_id="search_all_pages",
    filter_object_type="page",
    page_size=100
)
```

### 2. 搜索特定内容
```python
search_projects = NotionSearchOperator(
    task_id="search_projects",
    query="项目",
    filter_object_type="page"
)
```

### 3. 分页获取所有结果
```python
def search_all_pages_paginated(**context):
    hook = NotionHook()
    all_pages = []
    start_cursor = None
    
    while True:
        result = hook.search(
            filter_params={"property": "object", "value": "page"},
            start_cursor=start_cursor,
            page_size=100
        )
        
        all_pages.extend(result.get("results", []))
        
        if not result.get("has_more", False):
            break
        start_cursor = result.get("next_cursor")
    
    return all_pages
```

### 4. 查找所有数据库
```python
search_dbs = NotionSearchOperator(
    task_id="search_databases",
    filter_object_type="database"
)
```

## 兼容性

- ✅ Notion API 版本：2025-09-03（及更早版本）
- ✅ Python 版本：3.8+
- ✅ Airflow 版本：2.11.0+
- ✅ 向后兼容：不影响现有功能

## 测试

### 语法检查
```bash
python -m py_compile airflow/providers/notion/hooks/notion.py
python -m py_compile airflow/providers/notion/operators/notion.py
```
✅ 通过

### 导入测试
```python
from airflow.providers.notion.hooks.notion import NotionHook
from airflow.providers.notion.operators.notion import NotionSearchOperator
```
✅ 通过

### 功能测试
运行测试脚本：
```bash
export NOTION_API_TOKEN=ntn_your_token
python scripts/test_search.py
```

## 下一步

### 建议的改进
1. 添加单元测试（`tests/unit/test_notion_search.py`）
2. 添加集成测试（`tests/integration/test_notion_search_integration.py`）
3. 添加更多搜索过滤选项
4. 支持搜索结果缓存
5. 添加搜索结果统计功能

### 文档改进
1. 更新 API 文档
2. 添加更多使用场景示例
3. 创建视频教程

## 贡献者

- Hi Yen Wong (hiyenwong@gmail.com)

## 版本

- 功能添加版本：2.11.0.post8（建议）
- 最后更新：2025-11-29

## 参考资料

- [Notion API - Search](https://developers.notion.com/reference/post-search)
- [Notion API Version 2025-09-03](https://developers.notion.com/reference/versioning)

