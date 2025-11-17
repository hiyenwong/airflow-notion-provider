# Airflow Notion Provider

This provider enables Apache Airflow to integrate with Notion API, allowing you to automate workflows involving Notion databases, pages, and other content.

## Installation

```bash
pip install airflow-provider-notion
```

## Configuration

1. Get your Notion API token from [Notion Integrations](https://www.notion.so/my-integrations)
2. Set the connection in Airflow:
   - Connection ID: `notion_default`
   - Connection Type: `Notion` (或 `HTTP`)
   - Password: `YOUR_NOTION_API_TOKEN` (格式: `secret_xxxxx...`)
   - Extra (可选): `{"headers": {"Notion-Version": "2022-06-28"}}`

### 配置方式

**方法 1: Airflow UI**
```
Admin → Connections → Add Connection
- Connection Id: notion_default
- Connection Type: Notion
- Password: secret_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**方法 2: 环境变量**
```bash
export AIRFLOW_CONN_NOTION_DEFAULT='{"conn_type": "notion", "password": "secret_YOUR_TOKEN"}'
```

**方法 3: Airflow CLI**
```bash
airflow connections add notion_default \
    --conn-type notion \
    --conn-password secret_YOUR_NOTION_TOKEN
```

## Operators

### NotionQueryDatabaseOperator

Query a Notion database and return the results.

```python
from airflow.providers.nion.operators import NotionQueryDatabaseOperator

query_database = NotionQueryDatabaseOperator(
    task_id='query_notion_database',
    database_id='your-database-id',
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

Create a new page in Notion.

```python
from airflow.providers.nion.operators import NotionCreatePageOperator

create_page = NotionCreatePageOperator(
    task_id='create_notion_page',
    database_id='your-database-id',
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

The base hook for interacting with Notion API.

```python
from airflow.providers.nion.hooks import NotionHook

hook = NotionHook(notion_conn_id='notion_default')
# Query database
database = hook.get_database('database-id')
# Create page
page = hook.create_page(database_id='database-id', properties={...})
# Update page
page = hook.update_page(page_id='page-id', properties={...})
```

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