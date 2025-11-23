# Notion API Integration Tests

本目录包含用于测试 Notion API 集成的测试脚本。

## 快速开始

### 1. 基本连接测试

测试 API 连接和搜索数据库：

```bash
python tests/test_notion_api_quick.py
```

这将：
- ✓ 测试 API 连接
- ✓ 搜索可访问的数据库
- ✓ 如果找到数据库，运行完整测试套件

### 2. 使用特定数据库 ID 测试

如果你已经有数据库 ID：

```bash
python tests/test_with_database_id.py YOUR_DATABASE_ID
```

示例：
```bash
python tests/test_with_database_id.py 123e4567e89b12d3a456426614174000
```

这将：
- ✓ 获取数据库信息和数据源
- ✓ 查询数据源
- ✓ 测试向后兼容性（自动发现）
- ✓ 可选：创建测试页面

### 3. 完整集成测试（需要 Airflow）

运行完整的 pytest 测试套件：

```bash
# 设置环境变量
export NOTION_API_TOKEN="ntn_your_token"
export NOTION_TEST_DATABASE_ID="your_database_id"

# 运行测试
pytest tests/integration/test_notion_real_api.py -v
```

## 如何获取数据库 ID

### 方法 1: 从 Notion URL
1. 在 Notion 中打开数据库
2. 点击右上角 `...` → `Copy link`
3. URL 格式：`https://notion.so/xxxxx?v=yyyyy`
4. `xxxxx` 部分就是数据库 ID（移除中划线）

### 方法 2: 使用 API
```python
import requests

headers = {
    'Authorization': 'Bearer ntn_your_token',
    'Notion-Version': '2025-09-03'
}

response = requests.post(
    'https://api.notion.com/v1/search',
    headers=headers,
    json={'filter': {'value': 'data_source', 'property': 'object'}}
)

# 查看返回的数据库列表
print(response.json())
```

## 配置 Notion 集成

### 1. 创建集成
1. 访问 https://www.notion.so/my-integrations
2. 点击 "New integration"
3. 命名并创建
4. 复制 API token（格式：`ntn_xxxxx...`）

### 2. 授权数据库访问
1. 在 Notion 中打开数据库
2. 点击右上角 `...`
3. 选择 `Add connections`
4. 找到并选择你的集成

### 3. 验证权限
```bash
python tests/test_notion_api_quick.py
```

应该看到：
```
✓ Connection successful!
  API Version: 2025-09-03
  Users found: 2
```

## 测试内容

### 基本功能测试
- [x] API 连接
- [x] 用户列表
- [x] 搜索数据库
- [x] 获取数据库信息
- [x] 数据源发现
- [x] 查询数据源
- [x] **读取页面（get_page）**
- [x] 创建页面
- [x] 更新页面
- [x] 获取块内容

### API 2025-09-03 特性
- [x] 数据源 (data_source) 端点
- [x] 多数据源数据库支持
- [x] 自动发现机制
- [x] 向后兼容性（database_id → data_source_id）

### Hook 功能测试
- [x] `get_data_sources(database_id)`
- [x] `query_data_source(data_source_id, ...)`
- [x] `query_database(database_id=...)` - 自动发现
- [x] `query_database(data_source_id=...)` - 直接使用
- [x] **`get_page(page_id)` - 读取页面详情**
- [x] `create_page(data_source_id=...)`
- [x] `create_page(database_id=...)` - 自动发现
- [x] `update_page(page_id, properties)` - 更新页面
- [x] `get_block_children(block_id)` - 获取页面内容块
- [x] `append_block_children(block_id, children)` - 添加内容块

## 故障排除

### 错误：401 Unauthorized
- 检查 API token 是否正确
- 确认 token 格式为 `ntn_xxxxx...` 或 `secret_xxxxx...`

### 错误：404 Not Found
- 数据库 ID 可能不正确
- 检查是否移除了 ID 中的中划线
- 确认数据库未被删除

### 错误：403 Forbidden
- 集成没有访问数据库的权限
- 在 Notion 中：数据库 → `...` → `Add connections` → 选择你的集成

### 搜索返回 0 个数据库
- 确保至少有一个数据库与集成共享
- 在数据库中添加集成连接
- 等待几秒后重试

### 创建页面失败：validation_error
- 检查属性名称是否与数据库模式匹配
- 确保必填字段都已提供
- 查看 `properties` 结构是否正确

## 示例输出

### 成功的测试运行
```
╔══════════════════════════════════════════════════════════╗
║             Notion API Integration Test Suite            ║
╚══════════════════════════════════════════════════════════╝

============================================================
TEST 1: Testing Notion API Connection
============================================================
✓ Connection successful!
  API Version: 2025-09-03
  Users found: 2

============================================================
TEST 2: Searching for Databases
============================================================
✓ Search successful!
  Databases found: 3
  Available Databases:
    1. Task Tracker
       ID: 123e4567e89b12d3a456426614174000
    2. Project Database
       ID: 234e5678e89b12d3a456426614174001

============================================================
TEST 3: Getting Database Information
============================================================
✓ Database retrieved successfully!
  Title: Task Tracker
  Data sources: 1
    1. Main Table
       ID: xyz789...
  Properties: 5
    - Name (title)
    - Status (select)
    - Priority (select)

============================================================
TEST 4: Querying Data Source
============================================================
✓ Query successful!
  Pages returned: 5
  Has more: true

✓ All tests completed successfully!
```

## 环境变量

可用的环境变量：

```bash
# Notion API token（必需）
export NOTION_API_TOKEN="ntn_your_token_here"

# 测试数据库 ID（可选，用于完整测试）
export NOTION_TEST_DATABASE_ID="your_database_id"

# Airflow 连接（可选，用于 Airflow 集成测试）
export AIRFLOW_CONN_NOTION_DEFAULT='{"conn_type": "notion", "password": "ntn_your_token"}'
```

## 相关文档

- [README.md](../README.md) - 使用指南
- [MIGRATION_2025-09-03.md](../MIGRATION_2025-09-03.md) - API 迁移指南
- [TESTING.md](../TESTING.md) - 完整测试指南
- [Notion API 文档](https://developers.notion.com/reference)
