# Airflow Notion Provider - AI Agent Guide

## Project Overview

Custom Apache Airflow provider enabling workflow automation with Notion API. Built following Airflow's provider package architecture pattern with hooks (low-level API clients), operators (task abstractions), and sensors (event monitoring).

**Entry point**: `get_provider_info()` in `airflow/providers/notion/get_provider_info.py` registers this package with Airflow's plugin system.

## Architecture

### Package Structure
```
airflow/providers/notion/
├── hooks/notion.py       # NotionHook: Session management, API methods
├── operators/notion.py   # 3 operators: Query, Create, Update
├── sensors/              # Empty - sensors not yet implemented
└── get_provider_info.py  # Airflow provider registry entry
```

### Component Patterns

**Hooks** (`NotionHook` extends `BaseHook`):
- Singleton session pattern: `self.session` cached in `get_conn()`
- Connection config from Airflow connections: token via `conn.password` or `conn.extra['headers']['Authorization']`
- Base URL: `https://api.notion.com/v1`, Notion-Version: `2022-06-28`
- All methods return raw `response.json()` - no error wrapping beyond `raise_for_status()`

**Operators** (extend `BaseOperator`):
- Standard pattern: `__init__` stores params → `execute()` instantiates hook → calls hook method → logs result
- Template fields: `["database_id", "filter_params"]` etc. - enable Jinja2 templating with Airflow macros
- XCom push: `NotionCreatePageOperator` pushes `page_id` to context for downstream tasks
- UI color: `#3B7FB6` (Notion blue)

### Data Flow

1. Operator receives task params (may include Jinja2 templates like `{{ ds }}`)
2. Airflow renders templates using `template_fields`
3. `execute(context)` creates hook with `notion_conn_id`
4. Hook lazily initializes session in `get_conn()` (reads Airflow connection)
5. API call via `requests.Session` with stored headers
6. Result logged and returned (operators return full API response dicts)

## Connection Configuration

Set up Airflow connection `notion_default` (or custom ID):
- **Type**: HTTP (or use custom "notion" type if registered)
- **Host**: `https://api.notion.com` (optional, defaults in code)
- **Password**: Notion integration token (preferred method)
- **Extra**: `{"headers": {"Notion-Version": "2022-06-28", "Authorization": "Bearer YOUR_TOKEN"}}`

Token from Notion integrations page: https://www.notion.so/my-integrations

## Development Commands

```bash
# Install editable with dev dependencies
pip install -e ".[dev]"

# Code quality (per pyproject.toml settings)
black airflow/                    # Line length 110, target py38
mypy airflow/                      # Strict typing enabled
flake8 airflow/

# Testing (pytest config needed)
pytest tests/                      # No tests/ dir currently exists
pytest --cov=airflow/providers/notion tests/
```

## Implementation Guidelines

### Adding New Operators

1. **Hook method first**: Add API method to `NotionHook` (e.g., `delete_page()`, `search()`)
2. **Operator wrapper**: Create operator class in `operators/notion.py`:
   - Inherit `BaseOperator`
   - Define `template_fields` list for any params needing Jinja2 rendering
   - Set `ui_color = "#3B7FB6"`
   - `execute(context)` pattern: instantiate hook → call method → log → return
3. **Register**: Add to `get_provider_info()` operators list
4. **Document**: Add usage example to README.md

### Notion API Specifics

**Property structure** (from README examples):
```python
# Title property
{'title': [{'text': {'content': 'Task Name'}}]}

# Select property  
{'select': {'name': 'In Progress'}}

# Multi-select
{'multi_select': [{'name': 'tag1'}, {'name': 'tag2'}]}
```

**Filter format** (`filter_params` in `query_database`):
```python
{
    'property': 'Status',
    'select': {'equals': 'Done'}
}
```

Hook passes this directly to `data['filter']` - no transformation applied.

### Common Patterns

**Template field usage**: Any param that might use Airflow context (execution date, run ID, etc.):
```python
template_fields = ["database_id", "page_id", "properties"]
# Enables: database_id="{{ var.value.db_id }}", properties with {{ ds }}
```

**XCom communication**:
```python
# Push in operator execute()
context['task_instance'].xcom_push(key='result_key', value=data)

# Pull in downstream task
prev_result = context['task_instance'].xcom_pull(task_ids='upstream_task', key='result_key')
```

**Error handling**: Currently minimal - only `response.raise_for_status()`. To improve:
- Add custom exceptions (e.g., `NotionAPIError`, `NotionRateLimitError`)
- Wrap requests in try/except with informative logging
- Handle Notion-specific errors (see PROMPTS.md for error code mappings)

## Testing Strategy

No tests currently exist. Recommended structure:
```
tests/
├── unit/
│   ├── test_notion_hook.py
│   └── test_notion_operators.py
└── integration/
    └── test_notion_integration.py
```

**Mock pattern** (from PROMPTS.md):
```python
@patch('airflow.providers.notion.hooks.notion.NotionHook')
def test_operator(self, mock_hook):
    mock_hook_instance = Mock()
    mock_hook.return_value = mock_hook_instance
    mock_hook_instance.query_database.return_value = {'results': [...]}
    # Test operator logic
```

## Known Gaps

- **No sensors**: `sensors/` directory empty - consider `NotionPagePropertySensor`, `NotionDatabaseChangeSensor`
- **No error classes**: Generic `requests` exceptions only
- **No pagination handling**: `query_database` doesn't auto-paginate (Notion limits to 100 results)
- **No type hints in README**: Examples show Python 3.8+ type hints in code but not in documentation
- **No retry logic**: Should add exponential backoff for rate limits (429 errors)

## References

- Notion API docs: https://developers.notion.com/reference
- Airflow provider docs: https://airflow.apache.org/docs/apache-airflow-providers/
- Package build: Uses `hatchling` backend, installs via pip, registered via `entry_points["apache_airflow_provider"]`
