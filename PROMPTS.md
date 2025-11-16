# Airflow Notion Provider 开发提示词文档

本文档为 airflows-notion-provider 项目提供全面的开发提示词和最佳实践指南。

## 📋 目录

1. [技术架构提示词](#技术架构提示词)
2. [开发指导提示词](#开发指导提示词)
3. [最佳实践提示词](#最佳实践提示词)
4. [扩展功能提示词](#扩展功能提示词)
5. [文档和示例提示词](#文档和示例提示词)

---

## 🏗️ 技术架构提示词

### 项目结构规范
```
当创建新的provider功能时，遵循以下结构：
airflow/providers/notion/
├── hooks/           # API连接和基础操作
├── operators/       # 具体操作实现
├── sensors/         # 监控和触发器
├── utils/          # 工具函数和数据转换
├── exceptions/     # 自定义异常
└── tests/          # 测试文件
    ├── unit/       # 单元测试
    ├── integration/ # 集成测试
    └── fixtures/   # 测试数据
```

### 依赖管理原则
```
核心依赖配置原则：
- apache-airflow>=2.3.0 (保证兼容性)
- requests>=2.25.0 (HTTP客户端)
- python>=3.8 (类型提示支持)

开发依赖选择标准：
- pytest>=6.0 (稳定版本)
- pytest-cov>=2.0 (覆盖率报告)
- black>=21.0 (代码格式化)
- flake8>=3.8 (代码检查)
- mypy>=0.900 (类型检查)
```

### 模块设计模式
```python
# Hook设计模式
def create_hook_method(self, param: str) -> Dict[str, Any]:
    """创建操作的基础方法模板。

    Args:
        param: 操作参数的详细说明

    Returns:
        Dict[str, Any]: 返回结果的结构说明

    Raises:
        NotionAPIError: API错误时的异常说明
    """
    try:
        # 1. 参数验证
        self._validate_params(param)

        # 2. 构建请求
        request_data = self._build_request_data(param)

        # 3. 执行请求
        response = self.get_conn().post(url, json=request_data)
        response.raise_for_status()

        # 4. 处理响应
        return self._process_response(response.json())

    except requests.exceptions.RequestException as e:
        self.log.error(f"API request failed: {e}")
        raise NotionAPIError(f"创建操作失败: {e}") from e
```

### 类型系统设计
```python
# 使用Type Hints的最佳实践
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

@dataclass
class NotionPage:
    """Notion页面数据结构"""
    id: str
    created_time: str
    last_edited_time: str
    properties: Dict[str, Any]
    url: str

class NotionPropertyType:
    """Notion属性类型枚举"""
    TITLE = "title"
    RICH_TEXT = "rich_text"
    SELECT = "select"
    MULTI_SELECT = "multi_select"
    DATE = "date"
    NUMBER = "number"
    CHECKBOX = "checkbox"
```

---

## 🔧 开发指导提示词

### 新Operator开发模板
```python
class NotionNewOperator(BaseOperator):
    """
    [一句话描述操作符的作用]

    [详细描述操作符的功能和使用场景]

    :param param1: 参数1的详细说明（包括类型、默认值、是否必填）
    :type param1: str
    :param param2: 参数2的详细说明（可选参数要说明默认值）
    :type param2: Optional[Dict[str, Any]]
    :param notion_conn_id: Notion连接ID（保持默认值notion_default）
    :type notion_conn_id: str
    """

    template_fields = ["param1", "param2"]  # 支持模板渲染的字段
    ui_color = "#3B7FB6"  # 操作符在UI中的颜色

    def __init__(
        self,
        param1: str,
        param2: Optional[Dict[str, Any]] = None,
        notion_conn_id: str = "notion_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.param1 = param1
        self.param2 = param2
        self.notion_conn_id = notion_conn_id

    def execute(self, context: Context) -> Any:
        """执行操作的主方法。"""
        # 1. 初始化hook
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        # 2. 参数预处理
        processed_params = self._process_params()

        # 3. 执行核心操作
        self.log.info(f"开始执行操作: {self.task_id}")
        result = hook.specific_method(**processed_params)

        # 4. 结果处理
        self.log.info(f"操作成功完成: {self.task_id}")

        # 5. XCom推送（如果需要）
        if result:
            context['task_instance'].xcom_push(key='result_key', value=result)

        return result
```

### Hook方法开发标准
```python
def new_api_method(self,
                   required_param: str,
                   optional_param: Optional[str] = None,
                   **kwargs) -> Dict[str, Any]:
    """
    [方法的功能描述]

    Args:
        required_param: 必填参数的详细说明
        optional_param: 可选参数的详细说明
        **kwargs: 额外参数的支持

    Returns:
        Dict[str, Any]: 返回数据的结构描述

    Raises:
        NotionAPIError: 各种API错误的分类说明
        ValueError: 参数验证错误的说明
    """
    # 1. 参数验证
    if not required_param:
        raise ValueError("required_param不能为空")

    # 2. 构建请求
    url = f"{self.base_url}/endpoint"
    data = self._build_request_body(required_param, optional_param, **kwargs)

    # 3. 执行请求
    response = self.get_conn().request_method(url, json=data)
    response.raise_for_status()

    # 4. 返回结果
    return response.json()
```

### 测试开发指导
```python
# 单元测试模板
class TestNotionNewOperator(unittest.TestCase):
    """测试NotionNewOperator的功能"""

    def setUp(self):
        """测试前的准备工作"""
        self.dag = DAG(dag_id='test_dag', start_date=pendulum.now())
        self.operator = NotionNewOperator(
            task_id='test_task',
            param1='test_value',
            dag=self.dag
        )

    @patch('airflow.providers.notion.hooks.notion.NotionHook')
    def test_execute_success(self, mock_hook):
        """测试成功执行的场景"""
        # Arrange
        mock_hook_instance = Mock()
        mock_hook.return_value = mock_hook_instance
        mock_hook_instance.specific_method.return_value = {'id': 'test_id'}

        # Act
        result = self.operator.execute(context={})

        # Assert
        mock_hook.assert_called_once_with(notion_conn_id='notion_default')
        mock_hook_instance.specific_method.assert_called_once()
        self.assertEqual(result, {'id': 'test_id'})

    def test_validation_failure(self):
        """测试参数验证失败的情况"""
        with self.assertRaises(ValueError):
            NotionNewOperator(
                task_id='test_task',
                param1='',  # 无效的参数值
                dag=self.dag
            )
```

### 错误处理最佳实践
```python
class NotionAPIError(Exception):
    """Notion API基础异常类"""
    def __init__(self, message: str, status_code: Optional[int] = None,
                 response_data: Optional[Dict] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_data = response_data

class NotionRateLimitError(NotionAPIError):
    """API速率限制异常"""
    pass

class NotionAuthError(NotionAPIError):
    """认证失败异常"""
    pass

class NotionNotFoundError(NotionAPIError):
    """资源不存在异常"""
    pass

# Hook中的错误处理
def api_method_with_error_handling(self, param: str) -> Dict[str, Any]:
    try:
        response = self.get_conn().post(url, json=data)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        error_data = e.response.json() if e.response.content else {}

        if status_code == 401:
            raise NotionAuthError("认证失败，请检查API token", status_code, error_data)
        elif status_code == 404:
            raise NotionNotFoundError("请求的资源不存在", status_code, error_data)
        elif status_code == 429:
            raise NotionRateLimitError("API速率限制，请稍后重试", status_code, error_data)
        else:
            raise NotionAPIError(f"API请求失败: {e}", status_code, error_data)
```

---

## ⚡ 最佳实践提示词

### API版本管理
```
当前使用的Notion API版本：2022-06-28
建议更新策略：
1. 定期检查Notion API版本更新
2. 在重大版本更新时创建迁移指南
3. 维护向后兼容性
4. 测试新版本功能
```

### 性能优化建议
```python
# 批量操作优化
def batch_create_pages(self, pages_data: List[Dict]) -> List[Dict]:
    """批量创建页面以提高性能"""
    results = []

    # 使用线程池或异步操作
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(self.create_page, **page_data)
            for page_data in pages_data
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                self.log.error(f"批量创建页面失败: {e}")

    return results

# 缓存机制
@lru_cache(maxsize=128)
def get_database_schema(self, database_id: str) -> Dict[str, Any]:
    """缓存数据库结构以减少API调用"""
    return self.get_database(database_id)
```

### 日志记录规范
```python
# 标准日志格式
def execute(self, context: Context):
    # 操作开始前记录
    self.log.info(f"开始执行 {self.task_id} - 参数: {self.safe_params}")

    try:
        # 主要逻辑
        result = self._execute_logic()

        # 成功完成记录
        self.log.info(f"成功完成 {self.task_id} - 结果: {self.safe_result(result)}")

    except NotionAPIError as e:
        # API错误记录
        self.log.error(f"API错误 in {self.task_id} - 类型: {type(e).__name__}, 详情: {e.message}")
        raise

    except Exception as e:
        # 意外错误记录
        self.log.exception(f"意外错误 in {self.task_id} - 错误: {str(e)}")
        raise
```

### 安全配置指南
```python
# 安全地处理API token
def get_conn(self) -> requests.Session:
    """安全地获取API连接"""
    if self.session is None:
        # 从Airflow连接中获取敏感信息
        conn = self.get_connection(self.notion_conn_id)

        # 使用环境变量或密码字段
        token = conn.password or os.getenv('NOTION_API_TOKEN')

        if not token:
            raise NotionAuthError("未找到API token配置")

        # 安全地配置请求头
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Notion-Version': self.api_version
        })

    return self.session
```

---

## 🔮 扩展功能提示词

### Sensors开发模板
```python
class NotionPagePropertySensor(BaseSensorOperator):
    """
    监控Notion页面属性变化的传感器

    :param page_id: 要监控的页面ID
    :param property_name: 要监控的属性名称
    :param expected_value: 期望的属性值
    :param poke_interval: 检查间隔（秒）
    :param timeout: 超时时间（秒）
    """

    def __init__(
        self,
        page_id: str,
        property_name: str,
        expected_value: Any,
        poke_interval: int = 60,
        timeout: int = 60 * 60 * 24,  # 24小时
        **kwargs
    ):
        super().__init__(poke_interval=poke_interval, timeout=timeout, **kwargs)
        self.page_id = page_id
        self.property_name = property_name
        self.expected_value = expected_value

    def poke(self, context: Context) -> bool:
        """检查属性值是否达到期望"""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        try:
            page = hook.get_page(self.page_id)
            current_value = self._extract_property_value(
                page['properties'][self.property_name]
            )

            self.log.info(f"当前值: {current_value}, 期望值: {self.expected_value}")

            return current_value == self.expected_value

        except Exception as e:
            self.log.warning(f"检查属性时出错: {e}")
            return False

class NotionDatabaseChangeSensor(BaseSensorOperator):
    """监控数据库变化的传感器"""

    def __init__(
        self,
        database_id: str,
        filter_criteria: Optional[Dict] = None,
        expected_count: Optional[int] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.database_id = database_id
        self.filter_criteria = filter_criteria
        self.expected_count = expected_count
        self.previous_count = None

    def poke(self, context: Context) -> bool:
        """检查数据库是否发生了变化"""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)

        current_results = hook.query_database(
            database_id=self.database_id,
            filter_params=self.filter_criteria
        )

        current_count = len(current_results.get('results', []))

        # 第一次记录基准值
        if self.previous_count is None:
            self.previous_count = current_count
            return False

        # 检查数量是否发生变化
        has_changed = current_count != self.previous_count

        if self.expected_count is not None:
            has_changed = has_changed and (current_count == self.expected_count)

        if has_changed:
            self.log.info(f"数据库发生变化 - 原数量: {self.previous_count}, 新数量: {current_count}")
            return True

        return False
```

### 高级Operators开发
```python
class NotionTemplatePageOperator(BaseOperator):
    """
    基于模板创建Notion页面的高级Operator

    支持使用Jinja2模板动态生成页面内容
    """

    template_fields = ["template_data", "properties_template"]

    def __init__(
        self,
        database_id: str,
        template_data: Dict[str, Any],
        properties_template: str,  # Jinja2模板字符串
        template_engine: str = "jinja2",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.database_id = database_id
        self.template_data = template_data
        self.properties_template = properties_template
        self.template_engine = template_engine

    def execute(self, context: Context) -> Dict[str, Any]:
        # 1. 渲染模板
        rendered_properties = self._render_template(
            self.properties_template,
            {**self.template_data, **context}
        )

        # 2. 创建页面
        hook = NotionHook(notion_conn_id=self.notion_conn_id)
        page = hook.create_page(
            database_id=self.database_id,
            properties=rendered_properties
        )

        # 3. 可选：创建子页面或添加内容块
        if self.template_data.get('children'):
            children = self._process_children_templates()
            hook.append_block_children(page['id'], children)

        return page

class NotionBatchOperator(BaseOperator):
    """批量处理Notion数据的高效Operator"""

    def __init__(
        self,
        operation: str,  # 'create', 'update', 'delete'
        data_list: List[Dict[str, Any]],
        batch_size: int = 10,
        max_workers: int = 5,
        continue_on_error: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.operation = operation
        self.data_list = data_list
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.continue_on_error = continue_on_error

    def execute(self, context: Context) -> Dict[str, Any]:
        """执行批量操作"""
        hook = NotionHook(notion_conn_id=self.notion_conn_id)
        results = []
        errors = []

        # 分批处理数据
        for i in range(0, len(self.data_list), self.batch_size):
            batch = self.data_list[i:i + self.batch_size]
            batch_results, batch_errors = self._process_batch(hook, batch)

            results.extend(batch_results)
            errors.extend(batch_errors)

            # 速率限制控制
            time.sleep(0.1)

        summary = {
            'total_processed': len(self.data_list),
            'successful': len(results),
            'failed': len(errors),
            'errors': errors
        }

        self.log.info(f"批量操作完成: {summary}")

        if errors and not self.continue_on_error:
            raise AirflowException(f"批量操作失败: {errors}")

        return summary
```

### 实用工具类
```python
# 数据类型转换器
class NotionDataTypeConverter:
    """Notion数据类型转换工具"""

    @staticmethod
    def python_to_notion(value: Any, prop_type: str) -> Dict[str, Any]:
        """将Python值转换为Notion属性格式"""
        converters = {
            'title': lambda v: {'title': [{'text': {'content': str(v)}}]},
            'rich_text': lambda v: {'rich_text': [{'text': {'content': str(v)}}]},
            'number': lambda v: {'number': float(v) if v else None},
            'checkbox': lambda v: {'checkbox': bool(v)},
            'date': lambda v: {'date': {'start': v.isoformat() if hasattr(v, 'isoformat') else str(v)}},
            'select': lambda v: {'select': {'name': str(v)}},
            'multi_select': lambda v: {'multi_select': [{'name': str(item)} for item in v]}
        }

        converter = converters.get(prop_type)
        if not converter:
            raise ValueError(f"不支持的数据类型: {prop_type}")

        return converter(value)

    @staticmethod
    def notion_to_python(notion_value: Dict[str, Any], prop_type: str) -> Any:
        """将Notion属性格式转换为Python值"""
        # 反向转换逻辑
        if prop_type == 'title':
            return ''.join(text['text']['content'] for text in notion_value.get('title', []))
        elif prop_type == 'rich_text':
            return ''.join(text['text']['content'] for text in notion_value.get('rich_text', []))
        elif prop_type == 'number':
            return notion_value.get('number')
        elif prop_type == 'checkbox':
            return notion_value.get('checkbox', False)
        # ... 其他类型转换

# 验证工具
class NotionPropertyValidator:
    """Notion属性验证器"""

    @staticmethod
    def validate_database_properties(database_properties: Dict[str, Any]) -> bool:
        """验证数据库属性结构"""
        required_fields = ['id', 'name', 'type']

        for prop_name, prop_config in database_properties.items():
            if not all(field in prop_config for field in required_fields):
                raise ValueError(f"属性 '{prop_name}' 缺少必需字段: {required_fields}")

        return True

    @staticmethod
    def validate_page_properties(properties: Dict[str, Any], database_schema: Dict[str, Any]) -> bool:
        """验证页面属性是否符合数据库结构"""
        # 验证属性名称和类型
        for prop_name, prop_value in properties.items():
            if prop_name not in database_schema:
                raise ValueError(f"属性 '{prop_name}' 不在数据库结构中")

            expected_type = database_schema[prop_name]['type']
            # 验证属性值类型
            # ... 类型验证逻辑

        return True
```

---

## 📚 文档和示例提示词

### API文档模板
```python
# Hook方法文档示例
def query_database(self, database_id: str, filter_params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    查询Notion数据库并返回结果

    支持复杂的过滤条件，包括属性过滤、排序和分页

    Args:
        database_id (str): 要查询的数据库ID
        filter_params (Optional[Dict]): 过滤参数，支持以下格式：
            - 简单过滤: {'property': 'Status', 'select': {'equals': 'Done'}}
            - 复合过滤: {'and': [{'property': 'Status', 'select': {'equals': 'Done'}},
                                 {'property': 'Priority', 'select': {'equals': 'High'}}]}
            - 排序: {'sorts': [{'property': 'Created', 'direction': 'descending'}]}
            - 分页: {'start_cursor': 'cursor_string', 'page_size': 100}

    Returns:
        Dict[str, Any]: 查询结果包含以下字段：
            - 'results': 页面列表
            - 'has_more': 是否还有更多页面
            - 'next_cursor': 下一页的cursor
            - 'object': 'list'

    Raises:
        NotionAPIError: 查询失败时抛出异常，包含错误详情
        ValueError: 参数无效时抛出异常

    Examples:
        >>> hook = NotionHook(notion_conn_id='notion_default')
        >>> # 简单过滤
        >>> results = hook.query_database('database_id', {
        ...     'filter': {'property': 'Status', 'select': {'equals': 'Done'}}
        ... })
        >>> # 复杂过滤和排序
        >>> results = hook.query_database('database_id', {
        ...     'filter': {
        ...         'and': [
        ...             {'property': 'Due Date', 'date': {'on_or_before': '2024-01-01'}},
        ...             {'property': 'Assignee', 'people': {'contains': 'user_id'}}
        ...         ]
        ...     },
        ...     'sorts': [{'property': 'Due Date', 'direction': 'ascending'}]
        ... })
    """
```

### 使用示例集合
```python
# 基础使用示例
"""
基础数据库查询示例
"""
from airflow import DAG
from airflow.providers.notion.operators import NotionQueryDatabaseOperator
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'notion_basic_example',
    default_args=default_args,
    description='基础Notion查询示例',
    schedule_interval=None,
)

# 查询今天到期的任务
task_query = NotionQueryDatabaseOperator(
    task_id='query_today_tasks',
    database_id='your_tasks_database_id',
    filter_params={
        'filter': {
            'and': [
                {
                    'property': 'Due Date',
                    'date': {'on_or_before': '{{ ds }}'}
                },
                {
                    'property': 'Due Date',
                    'date': {'on_or_after': '{{ ds }}'}
                },
                {
                    'property': 'Status',
                    'select': {'does_not_equal': 'Completed'}
                }
            ]
        }
    },
    dag=dag
)

# 使用XCom传递结果
create_notification = PythonOperator(
    task_id='create_notification',
    python_callable=lambda **context:
        process_tasks(context['task_instance'].xcom_pull(task_ids='query_today_tasks')),
    dag=dag
)
```

### 故障排除指南
```python
# 常见错误和解决方案
"""
1. 连接错误: "Connection to api.notion.com timed out"
   原因：网络连接问题或Notion API不可用
   解决方案：
   - 检查网络连接
   - 验证HTTP代理设置
   - 增加连接超时时间
   - 查看Notion服务状态

2. 认证错误: "Invalid token"
   原因：API token无效或过期
   解决方案：
   - 重新生成Notion API token
   - 检查Airflow连接配置
   - 验证token格式

3. 数据库不存在: "Database not found"
   原因：数据库ID错误或没有权限
   解决方案：
   - 验证数据库ID
   - 检查数据库共享权限
   - 确认token有访问权限

4. 属性不存在: "Property not found"
   原因：属性名称错误或已被删除
   解决方案：
   - 使用get_database获取最新结构
   - 验证属性名称拼写
   - 检查大小写敏感性
"""
```

### 贡献指南模板
```markdown
# 贡献指南

## 开发环境搭建
1. 创建虚拟环境：`uv venv`
2. 安装包：`uv pip install -e ".[dev]"`
3. 运行测试：`pytest tests/`
4. 代码检查：`flake8 airflow/`
5. 类型检查：`mypy airflow/`

## 代码提交规范
- 使用Angular提交消息格式
- 包含测试用例
- 更新相关文档
- 通过所有CI检查

## 添加新功能流程
1. 创建issue讨论需求
2. 创建功能分支
3. 实现核心功能
4. 添加单元测试
5. 更新文档
6. 提交PR并review

## 测试要求
- 单元测试覆盖率>90%
- 集成测试覆盖主要场景
- 异常处理测试
- 边界条件测试
```

---

## 📋 快速参考表

### Notion属性类型对照表
| Python类型 | Notion类型 | 示例 |
|-----------|-----------|------|
| str | title/rich_text | "Hello World" |
| int/float | number | 42, 3.14 |
| bool | checkbox | True, False |
| datetime | date | datetime.now() |
| list | multi_select | ["tag1", "tag2"] |
| str | select | "option" |
| str | url | "https://example.com" |
| str | email | "user@example.com" |

### 错误码对照表
| HTTP状态码 | 异常类型 | 说明 |
|-----------|---------|------|
| 400 | NotionBadRequestError | 请求参数错误 |
| 401 | NotionAuthError | 认证失败 |
| 404 | NotionNotFoundError | 资源不存在 |
| 429 | NotionRateLimitError | 速率限制 |
| 500 | NotionInternalError | 服务器错误 |

### 最佳实践速查
✅ **推荐做法**:
- 使用环境变量管理敏感信息
- 添加详细的错误处理
- 实现完整的日志记录
- 编写单元测试
- 使用类型提示
- 遵循PEP 8规范

❌ **避免做法**:
- 硬编码API token
- 忽略异常处理
- 缺少文档说明
- 过度复杂的函数
- 重复代码
- 忽略性能考虑

---

本文档持续更新，欢迎大家贡献更多的提示词和最佳实践！