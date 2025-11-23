# 如何获取 Notion Database ID

## 方法 1: 从 Notion URL 获取（推荐）

1. 在 Notion 中打开你的数据库
2. 查看浏览器地址栏的 URL：

```
https://www.notion.so/workspace/1234567890abcdef1234567890abcdef?v=...
                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                              这就是 database_id
```

3. 复制这个 32 位的十六进制字符串

**示例**：
```
URL: https://www.notion.so/myworkspace/a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d?v=...
Database ID: a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d
```

## 方法 2: 使用辅助脚本列出所有数据库

### 步骤 1: 设置 API Token

```bash
export NOTION_API_TOKEN="ntn_xxxxx..."
```

### 步骤 2: 运行脚本

```bash
# 列出所有可访问的数据库
python scripts/list_databases.py

# 查看特定数据库的详细信息
python scripts/list_databases.py <database_id>
```

### 输出示例

```
🔍 Searching for Notion databases...

✅ Found 3 database(s):

================================================================================

1. My Tasks Database
   Database ID: a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d
   URL: https://www.notion.so/...
   Data Sources: 1
      1. Data Source ID: ds_a1b2c3d4e5f64a5b8c9d0e1f2a3b4c5d
         Type: database_data_source
   ----------------------------------------------------------------------------

2. Project Tracker
   Database ID: b2c3d4e5-f6a7-5b6c-9d0e-1f2a3b4c5d6e
   URL: https://www.notion.so/...
   Data Sources: 1
      1. Data Source ID: ds_b2c3d4e5f6a75b6c9d0e1f2a3b4c5d6e
         Type: database_data_source
   ----------------------------------------------------------------------------
```

## 方法 3: 在 Notion 中分享链接

1. 在 Notion 中打开数据库
2. 点击右上角的 **Share** 按钮
3. 点击 **Copy link**
4. 粘贴链接，从中提取 Database ID

## 在 Airflow 中配置

### 方法 A: 使用 Airflow Variables（推荐）

在 Airflow Web UI 中：
1. 进入 **Admin → Variables**
2. 添加新变量：
   - Key: `notion_database_id`
   - Value: `a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d`

或使用命令行：
```bash
airflow variables set notion_database_id "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d"
```

### 方法 B: 在 DAG 中直接硬编码

```python
from airflow.providers.notion.operators.notion import NotionQueryDatabaseOperator

query_task = NotionQueryDatabaseOperator(
    task_id="query_database",
    database_id="a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",  # 直接指定
    # ...
)
```

### 方法 C: 使用 Data Source ID（推荐用于 API 2025-09-03+）

```python
query_task = NotionQueryDatabaseOperator(
    task_id="query_database",
    data_source_id="ds_a1b2c3d4e5f64a5b8c9d0e1f2a3b4c5d",  # 使用 data source ID
    # ...
)
```

## 常见问题

### Q: 我找不到我的数据库？

**A:** 确保你的 Notion Integration 已经添加到该数据库：

1. 在 Notion 中打开数据库页面
2. 点击右上角的 **•••** (三个点)
3. 选择 **Add connections**
4. 找到并选择你的 Integration

### Q: Database ID 中的短横线要保留吗？

**A:** 两种格式都可以：
- 带短横线：`a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d` ✅
- 不带短横线：`a1b2c3d4e5f64a5b8c9d0e1f2a3b4c5d` ✅

Notion API 会自动处理。

### Q: Database ID 和 Data Source ID 有什么区别？

**A:** 
- **Database ID**: 数据库容器的 ID（旧 API）
- **Data Source ID**: 数据源（表）的 ID（新 API 2025-09-03+）

一个数据库可以包含多个数据源。在新 API 中推荐使用 Data Source ID。

我们的 provider 会自动处理兼容性：
- 如果只提供 `database_id`，会自动发现第一个 data source
- 如果提供 `data_source_id`，直接使用

## 获取 Notion API Token

如果还没有 API Token：

1. 访问 https://www.notion.so/my-integrations
2. 点击 **+ New integration**
3. 填写名称和选择工作空间
4. 点击 **Submit**
5. 复制 **Internal Integration Token**（格式：`ntn_xxxxx...` 或 `secret_xxxxx...`）

**重要**: 创建 Integration 后，必须将其添加到你想访问的数据库页面！
