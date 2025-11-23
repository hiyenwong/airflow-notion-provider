# Airflow Notion Provider Tests

本目录包含 Airflow Notion Provider 的完整测试套件。

## 测试结构

```
tests/
├── conftest.py              # Pytest配置和共享fixtures
├── unit/                    # 单元测试
│   ├── test_notion_hook.py      # NotionHook测试
│   └── test_notion_operators.py # Operators测试
└── integration/             # 集成测试
    └── test_notion_integration.py # 真实API集成测试
```

## 运行测试

### 安装测试依赖

```bash
pip install -e ".[dev]"
```

### 运行所有单元测试

```bash
pytest tests/unit/ -v
```

### 运行特定测试文件

```bash
pytest tests/unit/test_notion_hook.py -v
pytest tests/unit/test_notion_operators.py -v
```

### 运行特定测试

```bash
pytest tests/unit/test_notion_hook.py::TestNotionHook::test_get_conn_with_password -v
```

### 查看测试覆盖率

```bash
pytest tests/unit/ --cov=airflow/providers/notion --cov-report=html
# 打开 htmlcov/index.html 查看详细报告
```

### 运行集成测试

集成测试需要真实的 Notion API 凭证：

```bash
# 设置环境变量
export NOTION_API_TOKEN="your_notion_integration_token"
export NOTION_TEST_DATABASE_ID="your_test_database_id"

# 运行集成测试
pytest tests/integration/ -v
```

### 运行所有测试

```bash
pytest tests/ -v
```

## 测试覆盖范围

### NotionHook 测试 (test_notion_hook.py)

- ✅ 连接管理 (get_conn)
  - 密码认证
  - Session 缓存
  - 连接测试
- ✅ 数据库操作
  - 查询数据库 (query_database)
  - 获取数据库 (get_database)
  - 带过滤器查询
- ✅ 页面操作
  - 创建页面 (create_page)
  - 更新页面 (update_page)
  - 获取页面 (get_page)
  - 创建带子块的页面
- ✅ 块操作
  - 获取块子元素 (get_block_children)
  - 添加块子元素 (append_block_children)
- ✅ 错误处理
  - HTTP 错误
  - 连接失败

### Operators 测试 (test_notion_operators.py)

#### NotionQueryDatabaseOperator
- ✅ 初始化和配置
- ✅ Template fields
- ✅ 基本查询执行
- ✅ 带过滤器查询
- ✅ 自定义连接ID

#### NotionCreatePageOperator
- ✅ 初始化和配置
- ✅ Template fields
- ✅ 基本页面创建
- ✅ 带子块创建
- ✅ XCom 数据推送

#### NotionUpdatePageOperator
- ✅ 初始化和配置
- ✅ Template fields
- ✅ 页面更新
- ✅ 多属性更新

### 集成测试 (test_notion_integration.py)

- ✅ 真实API查询
- ✅ 真实数据库检索
- ✅ 真实页面创建和更新
- ✅ 连接测试

## 测试 Fixtures

### conftest.py 提供的共享 Fixtures

- `mock_notion_connection`: 模拟 Airflow 连接
- `mock_database_id`: 测试数据库ID
- `mock_page_id`: 测试页面ID
- `sample_database_response`: 示例数据库API响应
- `sample_page_response`: 示例页面API响应
- `sample_query_response`: 示例查询结果
- `sample_properties`: 示例页面属性
- `sample_filter_params`: 示例过滤参数
- `mock_dag`: 模拟 Airflow DAG
- `mock_task_instance`: 模拟 TaskInstance
- `mock_context`: 模拟 Airflow context

## 测试最佳实践

### 1. 使用 Mock 进行单元测试

```python
@patch('airflow.providers.notion.hooks.notion.NotionHook')
def test_operator(mock_hook_class):
    mock_hook_instance = Mock()
    mock_hook_class.return_value = mock_hook_instance
    mock_hook_instance.query_database.return_value = {'results': [...]}
    # 测试逻辑
```

### 2. 测试结构 (Arrange-Act-Assert)

```python
def test_example(self):
    # Arrange - 准备测试数据
    operator = NotionQueryDatabaseOperator(...)
    
    # Act - 执行操作
    result = operator.execute(context={})
    
    # Assert - 验证结果
    assert result == expected_result
```

### 3. 参数化测试

```python
@pytest.mark.parametrize("input,expected", [
    ("value1", "result1"),
    ("value2", "result2"),
])
def test_multiple_cases(input, expected):
    assert process(input) == expected
```

## 持续集成

测试配置为在 CI/CD 中自动运行：

```yaml
# .github/workflows/tests.yml 示例
- name: Run tests
  run: |
    pytest tests/unit/ -v --cov=airflow/providers/notion
```

## 测试覆盖率目标

- 单元测试覆盖率: **> 90%**
- 所有公共方法都应有测试
- 关键错误路径都应测试
- 边界条件应覆盖

## 故障排除

### ImportError: No module named 'airflow'

```bash
pip install apache-airflow>=2.3.0
```

### 测试失败: Connection not found

确保 mock 正确设置：
```python
@patch('airflow.providers.notion.hooks.notion.NotionHook.get_connection')
def test_example(mock_get_connection):
    mock_get_connection.return_value = mock_notion_connection
```

### 集成测试被跳过

设置必需的环境变量：
```bash
export NOTION_API_TOKEN="your_token"
export NOTION_TEST_DATABASE_ID="your_database_id"
```

## 贡献测试

添加新功能时：

1. 为新方法添加单元测试
2. 确保测试覆盖率 > 90%
3. 添加集成测试（如适用）
4. 更新此 README

## 参考资料

- [Pytest Documentation](https://docs.pytest.org/)
- [Airflow Testing Guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
