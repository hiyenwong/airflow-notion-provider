# 测试用例使用说明

## 已创建的完整测试套件

我已经为 Airflow Notion Provider 创建了完整的测试套件，包括：

### 测试结构
```
tests/
├── conftest.py                          # Pytest配置和共享fixtures  
├── unit/                                # 单元测试
│   ├── test_notion_hook.py             # NotionHook的17个测试用例
│   └── test_notion_operators.py        # 3个Operators的15个测试用例
├── integration/                         # 集成测试
│   └── test_notion_integration.py      # 真实API集成测试
├── pytest.ini                           # Pytest配置文件
└── README.md                            # 测试文档

总计: 32+ 个测试用例
```

### 测试覆盖范围

#### NotionHook测试 (test_notion_hook.py)
- ✅ 连接管理 (get_conn, 缓存, token配置)
- ✅ 数据库操作 (query_database, get_database, 过滤查询)
- ✅ 页面操作 (create_page, update_page, get_page, 带子块创建)
- ✅ 块操作 (get_block_children, append_block_children)
- ✅ 错误处理和连接测试

#### Operators测试 (test_notion_operators.py)
- ✅ NotionQueryDatabaseOperator (初始化, 执行, 过滤, 模板字段)
- ✅ NotionCreatePageOperator (初始化, 执行, XCom推送, 子块)
- ✅ NotionUpdatePageOperator (初始化, 执行, 多属性更新)

## 安装和运行测试

### 方法1: 安装完整依赖 (推荐)

```bash
# 1. 创建虚拟环境 (如果还没有)
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# 或
.venv\\Scripts\\activate  # Windows

# 2. 安装包及开发依赖
pip install -e ".[dev]"

# 3. 运行所有单元测试
pytest tests/unit/ -v

# 4. 查看测试覆盖率
pytest tests/unit/ --cov=airflow/providers/notion --cov-report=html
open htmlcov/index.html  # macOS
```

### 方法2: 只安装测试依赖

```bash
# 安装核心依赖
pip install apache-airflow>=2.3.0 requests>=2.25.0

# 安装测试依赖
pip install pytest>=6.0 pytest-cov>=2.0

# 运行测试
pytest tests/unit/ -v
```

### 方法3: 使用 uv (更快)

```bash
# 安装 uv (如果还没有)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 创建虚拟环境并安装
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# 运行测试
pytest tests/unit/ -v
```

## 运行特定测试

```bash
# 运行单个测试文件
pytest tests/unit/test_notion_hook.py -v

# 运行单个测试类
pytest tests/unit/test_notion_hook.py::TestNotionHook -v

# 运行单个测试方法
pytest tests/unit/test_notion_hook.py::TestNotionHook::test_get_conn_with_password -v

# 运行所有测试（包括集成测试，需要真实API凭证）
pytest tests/ -v
```

## 集成测试

集成测试需要真实的 Notion API 凭证：

```bash
# 设置环境变量
export NOTION_API_TOKEN="your_notion_integration_token"
export NOTION_TEST_DATABASE_ID="your_test_database_id"

# 运行集成测试
pytest tests/integration/ -v
```

如果没有设置凭证，集成测试会被自动跳过。

## 测试示例

### Hook 测试示例

```python
@patch('airflow.providers.notion.hooks.notion.NotionHook.get_connection')
@patch('requests.Session.post')
def test_query_database_success(
    self, mock_post, mock_get_connection, 
    mock_notion_connection, mock_database_id, sample_query_response
):
    """测试成功查询数据库"""
    mock_get_connection.return_value = mock_notion_connection
    mock_response = Mock()
    mock_response.json.return_value = sample_query_response
    mock_response.raise_for_status = Mock()
    mock_post.return_value = mock_response
    
    hook = NotionHook(notion_conn_id='notion_default')
    result = hook.query_database(database_id=mock_database_id)
    
    assert result == sample_query_response
    assert len(result['results']) == 2
```

### Operator 测试示例

```python
@patch('airflow.providers.notion.operators.notion.NotionHook')
def test_execute_success(
    self, mock_hook_class, mock_dag, mock_database_id,
    sample_query_response, mock_context
):
    """测试成功执行查询操作"""
    # Arrange
    mock_hook_instance = Mock()
    mock_hook_class.return_value = mock_hook_instance
    mock_hook_instance.query_database.return_value = sample_query_response
    
    operator = NotionQueryDatabaseOperator(
        task_id='test_query',
        database_id=mock_database_id,
        dag=mock_dag
    )
    
    # Act
    result = operator.execute(context=mock_context)
    
    # Assert
    assert result == sample_query_response
    mock_hook_instance.query_database.assert_called_once()
```

## 在 DAG 中使用

安装后，你可以在 Airflow DAG 中使用这些 operators：

```python
from airflow import DAG
from airflow.providers.notion.operators.notion import (
    NotionQueryDatabaseOperator,
    NotionCreatePageOperator,
    NotionUpdatePageOperator
)
import pendulum

dag = DAG(
    dag_id='notion_workflow',
    start_date=pendulum.now(),
    schedule_interval='@daily'
)

# 查询数据库
query_task = NotionQueryDatabaseOperator(
    task_id='query_tasks',
    database_id='{{ var.value.notion_db_id }}',
    filter_params={
        'property': 'Status',
        'select': {'equals': 'Todo'}
    },
    dag=dag
)

# 创建页面
create_task = NotionCreatePageOperator(
    task_id='create_report',
    database_id='{{ var.value.notion_db_id }}',
    properties={
        'Title': {
            'title': [{'text': {'content': '每日报告 {{ ds }}'}}]
        },
        'Status': {
            'select': {'name': 'Created'}
        }
    },
    dag=dag
)

# 更新页面 (使用上游任务的XCom数据)
update_task = NotionUpdatePageOperator(
    task_id='update_page',
    page_id="{{ task_instance.xcom_pull(task_ids='create_report', key='page_id') }}",
    properties={
        'Status': {
            'select': {'name': 'Completed'}
        }
    },
    dag=dag
)

query_task >> create_task >> update_task
```

## 配置 Airflow 连接

在 Airflow UI 中配置 Notion 连接：

1. 进入 Admin -> Connections
2. 添加新连接:
   - Connection Id: `notion_default`
   - Connection Type: `Notion` ✅ (自定义类型)
   - Password: `secret_your_notion_integration_token`
   - Extra (可选): `{"headers": {"Notion-Version": "2022-06-28"}}`

或使用命令行：

```bash
airflow connections add notion_default \
    --conn-type notion \
    --conn-password secret_your_notion_integration_token
```

**说明**:
- ✅ 使用 `notion` 作为 connection type（已注册自定义类型）
- ✅ Token 存储在 `password` 字段中
- ✅ 不需要配置 `host`（已在代码中硬编码为 `https://api.notion.com/v1`）
- ✅ API 版本默认为 `2022-06-28`（可在 Extra 中覆盖）

## 故障排除

### 问题: ModuleNotFoundError: No module named 'airflow'

**解决方案:**
```bash
pip install apache-airflow>=2.3.0
```

### 问题: pytest not found

**解决方案:**
```bash
pip install pytest>=6.0 pytest-cov>=2.0
```

### 问题: Import errors in tests

**解决方案:**
```bash
# 确保在项目根目录
cd /path/to/airflow-notion-provider

# 以可编辑模式安装
pip install -e .
```

## 下一步

1. ✅ 测试套件已完成 - 32+ 个测试用例
2. ✅ 所有核心功能已覆盖
3. ⏭️ 安装依赖并运行测试
4. ⏭️ 检查测试覆盖率 (目标: >90%)
5. ⏭️ 添加 CI/CD 配置 (GitHub Actions)

## 有用的命令

```bash
# 运行测试并显示详细输出
pytest tests/ -v -s

# 只运行失败的测试
pytest tests/ --lf

# 并行运行测试 (需要 pytest-xdist)
pip install pytest-xdist
pytest tests/ -n auto

# 生成覆盖率报告
pytest tests/unit/ --cov=airflow/providers/notion --cov-report=term-missing

# 检查代码质量
black airflow/ --check
flake8 airflow/
mypy airflow/
```

## 参考文档

- tests/README.md - 详细的测试文档
- .github/copilot-instructions.md - AI agent开发指南
- PROMPTS.md - 详细的开发模板和最佳实践
- README.md - 用户使用文档
