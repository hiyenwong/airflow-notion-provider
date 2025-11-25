# Airflow Notion Provider - AI Agent Guide

## Project Overview

Custom Apache Airflow provider for Notion API integration. **Uses Notion API 2025-09-03** (multi-source databases). Follows Airflow's provider architecture: hooks (API clients), operators (task wrappers), sensors (monitoring - not implemented).

**Key Files**:
- `airflow/providers/notion/hooks/notion.py` - NotionHook with all API methods
- `airflow/providers/notion/operators/notion.py` - Query/Create/Update operators
- `airflow/providers/notion/get_provider_info.py` - Provider registry (Airflow plugin system)

## Critical API Migration (2025-09-03)

**Database → Data Source paradigm shift**:
- Old: `POST /databases/{database_id}/query`
- New: `POST /data_sources/{data_source_id}/query`
- **Backward compatibility**: If only `database_id` provided, code auto-discovers first data source via `get_data_sources()`

**Implementation pattern** in all methods:
```python
# If data_source_id provided: use directly
if data_source_id is not None:
    return self.query_data_source(data_source_id, ...)

# If database_id provided: auto-discover first data source
if database_id is not None:
    db_info = self.get_data_sources(database_id)
    data_source_id = db_info['data_sources'][0]['id']
    return self.query_data_source(data_source_id, ...)
```

Used in: `query_database()`, `create_page()` hooks and corresponding operators.

## Architecture Patterns

### Hook Pattern (NotionHook)
- **Session caching**: `self.session` initialized once in `get_conn()`, reused across calls
- **Auth setup**: Token from `conn.password` → `Authorization: Bearer {token}` header
- **API version**: Hardcoded `Notion-Version: 2025-09-03` (overridable in `conn.extra`)
- **Empty string validation**: Convert empty `database_id`/`data_source_id` to None in operators' `__init__`:
  ```python
  self.database_id = database_id if database_id and database_id.strip() else None
  ```
  Prevents Jinja2 template rendering bugs where `{{ var.value.missing_key }}` returns empty string.

### Operator Pattern
- **Standard structure**:
  ```python
  def __init__(self, *, notion_conn_id='notion_default', database_id=None, **kwargs):
      super().__init__(**kwargs)
      self.database_id = database_id if database_id and database_id.strip() else None
      
  def execute(self, context: dict) -> dict:
      hook = NotionHook(self.notion_conn_id)
      result = hook.method_name(self.param1, self.param2)
      self.log.info(f"Operation completed: {result['id']}")
      return result
  ```
- **Template fields**: Enable Jinja2 rendering for dynamic values:
  ```python
  template_fields = ["database_id", "data_source_id", "filter_params", "properties"]
  # Allows: database_id="{{ var.value.db_id }}", properties with {{ ds }}
  ```
- **XCom pattern**: `NotionCreatePageOperator` pushes `page_id` for downstream tasks:
  ```python
  context['task_instance'].xcom_push(key='page_id', value=result['id'])
  ```

### Connection Configuration
```python
# Hook attributes (must match for Airflow discovery)
conn_name_attr = 'notion_conn_id'
default_conn_name = 'notion_default'
conn_type = 'notion'
hook_name = 'Notion'
```

**Airflow UI setup**:
- Type: `notion` (custom, registered in `get_provider_info()`)
- Password: `ntn_xxxxx...` or `secret_xxxxx...` (both formats supported)
- Extra (optional): `{"headers": {"Notion-Version": "2025-09-03"}}`

## Development Workflow

### Setup
```bash
pip install -e ".[dev]"  # Editable install with pytest, black, mypy, flake8
```

### Code Quality (from pyproject.toml)
```bash
black airflow/           # Line length 110, target py38
mypy airflow/            # Strict typing: disallow_untyped_defs=true
flake8 airflow/          # Style checks
```

### Testing (pytest.ini config)
```bash
# Unit tests (mocked, fast)
pytest tests/unit/ -v -m unit

# Integration tests (requires NOTION_API_TOKEN env var)
pytest tests/integration/ -v -m integration

# Coverage (target: >90%)
pytest tests/unit/ --cov=airflow/providers/notion --cov-report=html
# Opens htmlcov/index.html
```

**Mock pattern** (from `tests/conftest.py`):
```python
@patch('airflow.providers.notion.hooks.notion.NotionHook.get_connection')
def test_hook_method(mock_get_connection, mock_notion_connection):
    mock_get_connection.return_value = mock_notion_connection
    hook = NotionHook()
    # Test logic
```

**Fixtures available** (`tests/conftest.py`):
- `mock_notion_connection` - Airflow connection with token
- `mock_database_id`, `mock_page_id` - Test IDs
- `sample_query_response`, `sample_page_response` - API responses
- `mock_context`, `mock_task_instance` - Airflow execution context

## Adding New Features

### Adding Operator/Hook Method
1. **Hook method first** (`hooks/notion.py`):
   ```python
   def delete_page(self, page_id: str) -> Dict[str, Any]:
       url = f"{self.base_url}/pages/{page_id}"
       response = self.get_conn().delete(url)
       response.raise_for_status()
       return response.json()
   ```

2. **Operator wrapper** (`operators/notion.py`):
   ```python
   class NotionDeletePageOperator(BaseOperator):
       template_fields = ["page_id"]
       ui_color = "#3B7FB6"
       
       def __init__(self, *, page_id: str, notion_conn_id='notion_default', **kwargs):
           super().__init__(**kwargs)
           if not page_id or not page_id.strip():
               raise ValueError(f"page_id cannot be empty")
           self.page_id = page_id
           self.notion_conn_id = notion_conn_id
       
       def execute(self, context: dict) -> dict:
           hook = NotionHook(self.notion_conn_id)
           return hook.delete_page(self.page_id)
   ```

3. **Register** in `get_provider_info.py`:
   ```python
   "operators": [
       {"integration-name": "Notion",
        "python-modules": ["airflow.providers.notion.operators.notion"]}
   ]
   ```

4. **Add tests** (`tests/unit/test_notion_operators.py`):
   ```python
   @patch('airflow.providers.notion.hooks.notion.NotionHook')
   def test_delete_page_operator(mock_hook_class):
       mock_hook = Mock()
       mock_hook_class.return_value = mock_hook
       mock_hook.delete_page.return_value = {"object": "page", "archived": True}
       
       op = NotionDeletePageOperator(task_id='test', page_id='page_123')
       result = op.execute({})
       
       assert result["archived"] is True
       mock_hook.delete_page.assert_called_once_with('page_123')
   ```

### Notion Property Structures
**From working examples** (`examples/dags/example_notion_basic.py`):
```python
# Title (required for most databases)
{"Name": {"title": [{"text": {"content": "Task Name"}}]}}

# Rich text
{"Text": {"rich_text": [{"text": {"content": "Description"}}]}}

# Select (single choice)
{"Status": {"select": {"name": "In Progress"}}}

# Multi-select (multiple choices)
{"Tags": {"multi_select": [{"name": "urgent"}, {"name": "backend"}]}}

# Date
{"Due Date": {"date": {"start": "2025-01-15"}}}

# Filter format (for query_database)
filter_params = {"property": "Status", "status": {"equals": "In progress"}}
```

**Note**: `Created time` is read-only, cannot set in `create_page()`.

## Project-Specific Conventions

### Logging Pattern
```python
# Hook methods: verbose logging with masked tokens
self.log.info(f"Querying data source: {data_source_id}")
self.log.info(f"Request URL: {url}")
self.log.info(f"Request body: {json.dumps(data, indent=2)}")

# Error logging with full context
except requests.exceptions.HTTPError as e:
    self.log.error(f"HTTP Error: {e}")
    self.log.error(f"Status Code: {response.status_code}")
    self.log.error(f"Response Body: {response.text}")
    raise
```

### Error Handling Pattern
Currently minimal - only `response.raise_for_status()`. Requests exceptions bubble up.

**To add custom errors** (current gap):
```python
# Define in hooks/notion.py
class NotionAPIError(Exception):
    """Base Notion API error"""

class NotionRateLimitError(NotionAPIError):
    """Rate limit exceeded (429)"""

# Use in methods
if response.status_code == 429:
    raise NotionRateLimitError("Rate limit exceeded, retry after...")
```

### Package Metadata
- **Version**: `0.0.2.3` (from `pyproject.toml`)
- **Python**: `>=3.8` (supports 3.8-3.11)
- **Build**: `hatchling` backend, installs via `pip install -e .`
- **Entry point**: `apache_airflow_provider` registered in `[project.entry-points]`

## Known Limitations

1. **No sensors** - `sensors/` directory empty (consider `NotionPagePropertySensor`)
2. **No pagination** - `query_database()` doesn't auto-paginate (Notion limit: 100 results per page)
3. **No retry logic** - No exponential backoff for rate limits (429 errors)
4. **Basic error handling** - Only `raise_for_status()`, no Notion-specific error classes
5. **Multi-source ambiguity** - If database has multiple data sources, auto-discovery uses first one (may not be intended source)

## Testing Credentials

**Integration tests** require real Notion API:
```bash
export NOTION_API_TOKEN="ntn_xxxxx..."
export NOTION_TEST_DATABASE_ID="database-id"
pytest tests/integration/ -v -m integration
```

**Unit tests** use mocks (no credentials needed):
```bash
pytest tests/unit/ -v -m unit  # Fast, no API calls
```

## References

- Notion API docs (2025-09-03): https://developers.notion.com/reference
- Airflow provider guide: https://airflow.apache.org/docs/apache-airflow-providers/
- Example DAGs: `examples/dags/example_notion_*.py`
- Utility scripts: `scripts/list_databases.py`, `scripts/inspect_database_properties.py`
