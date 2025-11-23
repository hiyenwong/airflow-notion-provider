# Migration Guide: Notion API 2025-09-03

## Summary

This document describes the migration of `airflow-notion-provider` from Notion API version **2022-06-28** to **2025-09-03**.

**Migration Date**: November 18, 2025  
**Breaking Changes**: Yes (multi-source databases)  
**Backward Compatibility**: Yes (automatic discovery)

## What Changed in Notion API 2025-09-03

Notion introduced **multi-source databases** - a fundamental change in their data model:

- **Before**: One database = one data table
- **After**: One database (container) can have multiple data sources (tables)

### Key API Changes

1. **Endpoints moved from `/v1/databases/*` to `/v1/data_sources/*`**:
   ```
   OLD: POST /v1/databases/{database_id}/query
   NEW: POST /v1/data_sources/{data_source_id}/query
   ```

2. **Parent type changed when creating pages**:
   ```python
   # Old
   {"parent": {"database_id": "..."}}
   
   # New
   {"parent": {"data_source_id": "..."}}
   ```

3. **Retrieve database now returns data sources**:
   ```python
   GET /v1/databases/{database_id}
   # Returns:
   {
     "object": "database",
     "id": "...",
     "data_sources": [
       {"id": "data-source-1", "name": "Main Table"},
       {"id": "data-source-2", "name": "Archive"}
     ]
   }
   ```

## Changes Made to This Provider

### 1. Updated API Version Header

**File**: `airflow/providers/notion/hooks/notion.py`

```python
# Changed from:
'Notion-Version': '2022-06-28'

# To:
'Notion-Version': '2025-09-03'
```

### 2. Added Data Source Discovery

**New Method**: `NotionHook.get_data_sources(database_id)`

Returns database info including `data_sources` array. Used for automatic discovery.

### 3. Added New Query Method

**New Method**: `NotionHook.query_data_source(data_source_id, ...)`

Direct API call to `/v1/data_sources/{id}/query` endpoint.

### 4. Updated Existing Methods

**Method**: `NotionHook.query_database()`

- **Before**: Only accepted `database_id`
- **After**: Accepts both `database_id` and `data_source_id`
- **Behavior**: 
  - If `data_source_id` provided → use it directly
  - If only `database_id` provided → auto-discover first data source
  - Logs discovery process for transparency

**Method**: `NotionHook.create_page()`

- **Before**: Only accepted `database_id` for parent
- **After**: Accepts both `database_id` and `data_source_id`
- **Behavior**: Same auto-discovery logic as `query_database()`

### 5. Updated All Operators

**Changes Applied To**:
- `NotionQueryDatabaseOperator`
- `NotionCreatePageOperator`

**New Parameters**:
```python
def __init__(
    self,
    database_id: Optional[str] = None,      # Legacy (still works)
    data_source_id: Optional[str] = None,   # Recommended
    ...
):
```

**New Template Fields**:
```python
template_fields = ["database_id", "data_source_id", "filter_params", "sorts"]
```

## Migration Path for Users

### Option 1: No Changes (Automatic - Recommended for Most Users)

If you have **single-source databases** (most common):

```python
# Your existing code continues to work
query = NotionQueryDatabaseOperator(
    task_id='query_task',
    database_id='your-database-id',  # No changes needed
    filter_params={...},
    dag=dag
)
```

**What happens**: The provider automatically discovers the first (and only) data source.

### Option 2: Explicit Data Source IDs (Required for Multi-Source Databases)

If you need to work with **specific data sources** in multi-source databases:

```python
# Step 1: Get your data source ID (one-time)
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()
db_info = hook.get_data_sources('your-database-id')
print(db_info['data_sources'])
# [{'id': 'ds-123', 'name': 'Main'}, {'id': 'ds-456', 'name': 'Archive'}]

# Step 2: Use explicit data_source_id
query = NotionQueryDatabaseOperator(
    task_id='query_main_table',
    data_source_id='ds-123',  # Explicit
    filter_params={...},
    dag=dag
)
```

### Finding Your Data Source ID

**Method 1: Via Notion App**
1. Open database in Notion
2. Click "..." → Database Settings
3. Manage data sources → Copy data source ID

**Method 2: Via API**
```python
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook(notion_conn_id='notion_default')
db_info = hook.get_data_sources('your-database-id')

for ds in db_info['data_sources']:
    print(f"Name: {ds['name']}, ID: {ds['id']}")
```

**Method 3: Via airflow CLI/Python**
```bash
airflow tasks test your_dag discover_ds 2025-01-01
```

```python
# DAG task
from airflow.decorators import task

@task
def discover_data_sources(**context):
    from airflow.providers.notion.hooks import NotionHook
    hook = NotionHook()
    db_info = hook.get_data_sources('your-database-id')
    return db_info['data_sources']
```

## Breaking Changes & Compatibility

### What Breaks?

If users add a **second data source** to an existing database in Notion, integrations using only `database_id` will:

✅ **Still work** (uses first data source automatically)  
⚠️ **May not query intended data** (if you wanted the second data source)

### Recommended Actions

1. **For existing DAGs**: No immediate action required
2. **For new DAGs**: Use `data_source_id` from the start
3. **For multi-source databases**: Update to use explicit `data_source_id`

## Testing the Migration

### 1. Verify API Version

```python
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()
session = hook.get_conn()
print(session.headers.get('Notion-Version'))
# Should print: 2025-09-03
```

### 2. Test Auto-Discovery

```python
hook = NotionHook()

# This should log discovery process
result = hook.query_database(database_id='your-database-id')
print(f"Found {len(result['results'])} results")
```

Expected logs:
```
INFO - Auto-discovering data_source_id for database abc-123
INFO - Using data_source_id: xyz-789
```

### 3. Test Explicit Data Source

```python
hook = NotionHook()

# Get data sources
db_info = hook.get_data_sources('your-database-id')
ds_id = db_info['data_sources'][0]['id']

# Query directly
result = hook.query_data_source(data_source_id=ds_id)
print(f"Found {len(result['results'])} results")
```

## Rollback Plan

If issues arise, you can temporarily rollback by:

1. **Override API version in connection**:
   ```json
   {
     "headers": {
       "Notion-Version": "2022-06-28"
     }
   }
   ```

2. **Note**: This only works for **single-source databases**. Multi-source databases require 2025-09-03.

## Additional Resources

- [Notion API Upgrade Guide](https://developers.notion.com/docs/upgrade-guide-2025-09-03)
- [Notion API Changelog](https://developers.notion.com/page/changelog)
- [Provider README](./README.md)
- [Copilot Instructions](./.github/copilot-instructions.md)

## Support

For issues or questions:
1. Check [README.md](./README.md) for examples
2. Review [TESTING.md](./TESTING.md) for test setup
3. Open an issue on GitHub with:
   - Your Airflow version
   - Error logs
   - Whether database is single or multi-source
