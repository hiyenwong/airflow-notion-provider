# Airflow Notion Provider - AI Agent Guide

## Project Overview

Custom Apache Airflow provider enabling workflow automation with Notion API. Built following Airflow's provider package architecture pattern with hooks (low-level API clients), operators (task abstractions), and sensors (event monitoring).

**API Version**: Uses Notion API **2025-09-03** (released Sept 2025) supporting multi-source databases.

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
- Base URL: `https://api.notion.com/v1`, Notion-Version: `2025-09-03`
- All methods return raw `response.json()` - no error wrapping beyond `raise_for_status()`
- **Data Source Discovery**: `get_data_sources(database_id)` returns database info with `data_sources` array
- **Backward Compatibility**: Methods accept both `database_id` and `data_source_id`, auto-discovering first data source when only `database_id` provided

**Operators** (extend `BaseOperator`):
- Standard pattern: `__init__` stores params → `execute()` instantiates hook → calls hook method → logs result
- Template fields: `["database_id", "data_source_id", "filter_params", "sorts"]` etc. - enable Jinja2 templating with Airflow macros
- XCom push: `NotionCreatePageOperator` pushes `page_id` to context for downstream tasks
- UI color: `#3B7FB6` (Notion blue)
- **Migration Support**: Both `database_id` (legacy) and `data_source_id` (recommended) parameters supported

### Data Flow

1. Operator receives task params (may include Jinja2 templates like `{{ ds }}`)
2. Airflow renders templates using `template_fields`
3. `execute(context)` creates hook with `notion_conn_id`
4. Hook lazily initializes session in `get_conn()` (reads Airflow connection)
5. **Data Source Discovery** (if needed): Hook calls `get_data_sources(database_id)` to fetch first data source ID
6. API call via `requests.Session` with stored headers to `/v1/data_sources/{id}/*` endpoints
7. Result logged and returned (operators return full API response dicts)

## Connection Configuration

Set up Airflow connection `notion_default` (or custom ID):
- **Type**: `notion` (custom type registered in `get_provider_info()`)
- **Password**: Notion integration token (format: `ntn_xxxxx...` or legacy `secret_xxxxx...`)
- **Extra** (optional): `{"headers": {"Notion-Version": "2025-09-03"}}`

**Note**: `host` and base API version are hardcoded in `NotionHook`:
- Base URL: `https://api.notion.com/v1` (can override via `conn.host`)
- API Version: `2025-09-03` (can override in Extra)

Token from Notion integrations page: https://www.notion.so/my-integrations

**Hook Connection Attributes**:
- `conn_name_attr = 'notion_conn_id'`
- `default_conn_name = 'notion_default'`
- `conn_type = 'notion'`
- `hook_name = 'Notion'`

## Development Commands

```bash
# Install editable with dev dependencies
pip install -e ".[dev]"

# Code quality (per pyproject.toml settings)
black airflow/                    # Line length 110, target py38
mypy airflow/                      # Strict typing enabled
flake8 airflow/

# Testing (pytest config in pytest.ini)
pytest tests/                      # All tests
pytest tests/unit/                 # Unit tests only
pytest tests/integration/          # Integration tests (needs API token)
pytest --cov=airflow/providers/notion tests/  # With coverage
pytest -m unit                     # Run by marker

# Watch test output location
open htmlcov/index.html           # Coverage report (after running with --cov)
```

## Implementation Guidelines

### Notion API 2025-09-03 Migration

**Multi-Source Databases**: Notion now separates databases (containers) from data sources (tables). Key impacts:

1. **Endpoint Changes**:
   - Old: `/v1/databases/{database_id}/query`
   - New: `/v1/data_sources/{data_source_id}/query`

2. **Parent Type for Pages**:
   - Old: `{"parent": {"database_id": "..."}}`
   - New: `{"parent": {"data_source_id": "..."}}`

3. **Backward Compatibility Strategy**:
   - All methods accept both `database_id` and `data_source_id`
   - If only `database_id` provided, auto-discover first data source via `get_data_sources()`
   - Log discovery process for transparency
   - Raise clear errors if no data sources found

4. **Migration Pattern**:
   ```python
   # In hook methods
   if data_source_id is not None:
       return self.query_data_source(data_source_id, ...)
   elif database_id is not None:
       self.log.info(f"Auto-discovering data_source_id for database {database_id}")
       db_info = self.get_data_sources(database_id)
       data_sources = db_info.get('data_sources', [])
       if not data_sources:
           raise ValueError(f"No data sources found for database {database_id}")
       discovered_id = data_sources[0]['id']
       self.log.info(f"Using data_source_id: {discovered_id}")
       return self.query_data_source(discovered_id, ...)
   ```

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

**Pagination**: Use `start_cursor` and `page_size` for large result sets (max 100 per request).

### Common Patterns

**Template field usage**: Any param that might use Airflow context (execution date, run ID, etc.):
```python
template_fields = ["database_id", "data_source_id", "page_id", "properties"]
# Enables: data_source_id="{{ var.value.ds_id }}", properties with {{ ds }}
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

### Test Structure
```
tests/
├── conftest.py              # Shared fixtures (mock connections, responses)
├── pytest.ini              # Pytest config (markers, coverage)
├── unit/                   # Fast tests with mocks
│   ├── test_notion_hook.py
│   └── test_notion_operators.py
└── integration/            # Real API tests (requires token)
    └── test_notion_integration.py
```

**Mock pattern** (from conftest.py):
```python
@pytest.fixture
def mock_notion_connection():
    """Mock Airflow connection"""
    conn = Mock(spec=Connection)
    conn.password = "secret_test_token_12345"
    conn.extra = '{"headers": {"Notion-Version": "2025-09-03"}}'
    return conn

# Usage in tests
@patch('airflow.providers.notion.hooks.notion.NotionHook.get_connection')
@patch('requests.Session.post')
def test_operator(mock_post, mock_get_connection, mock_notion_connection):
    mock_get_connection.return_value = mock_notion_connection
    mock_response = Mock()
    mock_response.json.return_value = {'results': [...]}
    mock_post.return_value = mock_response
    # Test logic
```

**Test markers** (from pytest.ini):
- `@pytest.mark.unit` - Fast, no external dependencies
- `@pytest.mark.integration` - Requires real API credentials
- `@pytest.mark.slow` - Long-running tests

**Run integration tests**:
```bash
export NOTION_API_TOKEN="ntn_xxxxx..."
export NOTION_TEST_DATABASE_ID="db_xxxxx..."
pytest tests/integration/ -v
```

### Fixtures Available (conftest.py)
- `mock_notion_connection` - Mock Airflow connection
- `mock_database_id` - Test database ID string
- `mock_page_id` - Test page ID string
- `sample_database_response` - Full database API response
- `sample_page_response` - Full page API response
- `sample_query_response` - Query results with 2 pages
- `sample_properties` - Valid page properties dict
- `sample_filter_params` - Valid filter params
- `mock_dag` - Airflow DAG instance
- `mock_context` - Full context dict with task_instance

## Known Gaps

- **No sensors**: `sensors/` directory empty - consider `NotionPagePropertySensor`, `NotionDatabaseChangeSensor`
- **No error classes**: Generic `requests` exceptions only
- **No pagination handling in operators**: `query_database` doesn't auto-paginate (Notion limits to 100 results)
- **No type hints in README**: Examples show Python 3.8+ type hints in code but not in documentation
- **No retry logic**: Should add exponential backoff for rate limits (429 errors)
- **Multi-source database support**: Currently auto-discovers first data source only; doesn't support querying specific data source in multi-source databases

## Project-Specific Conventions

### Code Style
- **Line length**: 110 characters (black config in pyproject.toml)
- **Python version**: 3.8+ (uses type hints throughout)
- **Docstring format**: Google-style with `:param` and `:type` annotations
- **Import order**: Standard lib → third-party → airflow → local

### Naming Conventions
- Hook methods: `verb_noun` (e.g., `query_database`, `create_page`)
- Operators: `Notion{Action}{Resource}Operator` (e.g., `NotionQueryDatabaseOperator`)
- Test files: `test_{module}.py`
- Test methods: `test_{method}_{scenario}` (e.g., `test_query_database_success`)

### File Headers
All Python files include Apache License 2.0 header (see existing files for template).

## References

- Notion API docs: https://developers.notion.com/reference
- Airflow provider docs: https://airflow.apache.org/docs/apache-airflow-providers/
- Package build: Uses `hatchling` backend, installs via pip, registered via `entry_points["apache_airflow_provider"]`
- Migration guide: See `MIGRATION_2025-09-03.md` for detailed API migration info
- Development patterns: See `PROMPTS.md` for Chinese-language templates and best practices
