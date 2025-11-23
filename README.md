# Airflow Notion Provider

This provider enables Apache Airflow to integrate with Notion API, allowing you to automate workflows involving Notion databases, pages, and other content.

**API Version**: This provider uses Notion API version **2025-09-03**, which supports multi-source databases. See [Migration Guide](#migration-from-legacy-api) for upgrading from older versions.

## Installation

```bash
pip install airflow-provider-notion
```

## Configuration

1. Get your Notion API token from [Notion Integrations](https://www.notion.so/my-integrations)
   - New tokens use `ntn_` prefix (after Sept 2024)
   - Legacy tokens with `secret_` prefix still work
2. Set the connection in Airflow:
   - Connection ID: `notion_default`
   - Connection Type: `notion` (custom type registered by this provider)
   - Password: `YOUR_NOTION_API_TOKEN` (format: `ntn_xxxxx...` or `secret_xxxxx...`)
   - Extra (optional): `{"headers": {"Notion-Version": "2025-09-03"}}`

### Configuration Methods

**Method 1: Airflow UI**
```
Admin → Connections → Add Connection
- Connection Id: notion_default
- Connection Type: notion
- Password: ntn_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**Method 2: Environment Variable**
```bash
export AIRFLOW_CONN_NOTION_DEFAULT='{"conn_type": "notion", "password": "ntn_YOUR_TOKEN"}'
```

**Method 3: Airflow CLI**
```bash
airflow connections add notion_default \
    --conn-type notion \
    --conn-password ntn_YOUR_NOTION_TOKEN
```

## Operators

### NotionQueryDatabaseOperator

Query a Notion database or data source and return the results.

**Note**: For API 2025-09-03+, prefer using `data_source_id`. If only `database_id` is provided, the operator will automatically discover the first data source.

```python
from airflow.providers.notion.operators import NotionQueryDatabaseOperator

# Recommended: Use data_source_id
query_database = NotionQueryDatabaseOperator(
    task_id='query_notion_datasource',
    data_source_id='your-data-source-id',  # Preferred
    filter_params={
        'property': 'Status',
        'select': {
            'equals': 'Done'
        }
    },
    sorts=[
        {
            'property': 'Created',
            'direction': 'descending'
        }
    ],
    page_size=50,
    dag=dag
)

# Legacy: Auto-discover from database_id (backward compatible)
query_database_legacy = NotionQueryDatabaseOperator(
    task_id='query_notion_database',
    database_id='your-database-id',  # Auto-discovers first data source
    filter_params={
        'property': 'Status',
        'select': {
            'equals': 'Done'
        }
    },
    dag=dag
)
```

### NotionCreatePageOperator

Create a new page in a Notion data source.

**Note**: For API 2025-09-03+, prefer using `data_source_id`. If only `database_id` is provided, the operator will automatically discover the first data source.

```python
from airflow.providers.notion.operators import NotionCreatePageOperator

# Recommended: Use data_source_id
create_page = NotionCreatePageOperator(
    task_id='create_notion_page',
    data_source_id='your-data-source-id',  # Preferred
    properties={
        'Title': {
            'title': [
                {
                    'text': {
                        'content': 'New Task'
                    }
                }
            ]
        },
        'Status': {
            'select': {
                'name': 'In Progress'
            }
        }
    },
    children=[  # Optional page content
        {
            'object': 'block',
            'type': 'paragraph',
            'paragraph': {
                'rich_text': [{'type': 'text', 'text': {'content': 'Page content here'}}]
            }
        }
    ],
    dag=dag
)

# Legacy: Auto-discover from database_id (backward compatible)
create_page_legacy = NotionCreatePageOperator(
    task_id='create_notion_page_legacy',
    database_id='your-database-id',  # Auto-discovers first data source
    properties={...},
    dag=dag
)
```

### NotionUpdatePageOperator

Update an existing Notion page.

```python
from airflow.providers.nion.operators import NotionUpdatePageOperator

update_page = NotionUpdatePageOperator(
    task_id='update_notion_page',
    page_id='your-page-id',
    properties={
        'Status': {
            'select': {
                'name': 'Completed'
            }
        }
    },
    dag=dag
)
```

## Hooks

### NotionHook

The base hook for interacting with Notion API (version 2025-09-03).

```python
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook(notion_conn_id='notion_default')

# Get data sources for a database
db_info = hook.get_data_sources('database-id')
data_sources = db_info.get('data_sources', [])
data_source_id = data_sources[0]['id']

# Query data source (recommended)
results = hook.query_data_source(
    data_source_id='data-source-id',
    filter_params={...},
    sorts=[...],
    page_size=50
)

# Query database (legacy, auto-discovers first data source)
results = hook.query_database(database_id='database-id', filter_params={...})

# Create page with data_source_id (recommended)
page = hook.create_page(
    data_source_id='data-source-id',
    properties={...}
)

# Create page with database_id (legacy, auto-discovers first data source)
page = hook.create_page(database_id='database-id', properties={...})

# Update page (unchanged)
page = hook.update_page(page_id='page-id', properties={...})

# Get page
page = hook.get_page(page_id='page-id')

# Block operations
children = hook.get_block_children(block_id='block-id')
hook.append_block_children(block_id='block-id', children=[...])
```

## Migration from Legacy API

This provider uses Notion API version **2025-09-03**, which introduces multi-source databases. Key changes:

### What Changed?

1. **Database → Data Source paradigm**:
   - Old: One database = one data table
   - New: One database can contain multiple data sources (tables)
   - Each data source has its own ID and schema

2. **API Endpoints**:
   - Old: `POST /v1/databases/{database_id}/query`
   - New: `POST /v1/data_sources/{data_source_id}/query`

3. **Parent Type for Pages**:
   - Old: `{"parent": {"database_id": "..."}}`
   - New: `{"parent": {"data_source_id": "..."}}`

### How to Migrate?

**Option 1: Automatic Migration (Recommended)**
- Keep using `database_id` parameter
- The provider automatically discovers the first data source
- Works for single-source databases (most common case)

```python
# No changes needed - backward compatible
query = NotionQueryDatabaseOperator(
    task_id='query',
    database_id='your-database-id',  # Auto-discovers data_source_id
    ...
)
```

**Option 2: Explicit Data Source IDs**
- Get data source ID from database
- Use `data_source_id` parameter explicitly
- Required for multi-source databases

```python
# Step 1: Get data source ID (one-time setup)
hook = NotionHook()
db_info = hook.get_data_sources('your-database-id')
data_source_id = db_info['data_sources'][0]['id']  # First data source

# Step 2: Use data_source_id in operators
query = NotionQueryDatabaseOperator(
    task_id='query',
    data_source_id=data_source_id,  # Explicit data source
    ...
)
```

**Finding Your Data Source ID**:
1. In Notion app: Database Settings → Manage data sources → Copy data source ID
2. Via API: `GET /v1/databases/{database_id}` returns `data_sources` array
3. Via Hook: `hook.get_data_sources(database_id)`

### Breaking Changes

If users add a second data source to a database in Notion, integrations using `database_id` will:
- Still work with automatic discovery (uses first data source)
- May not query the intended data source if multiple exist
- Should be updated to use explicit `data_source_id`

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest tests/
```

Format code:

```bash
black airflow/
```

Check types:

```bash
mypy airflow/
```

## License

Apache License 2.0