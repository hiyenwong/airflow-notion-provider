# Notion Provider Examples

This directory contains comprehensive example DAGs demonstrating how to use the Airflow Notion Provider.

## 📋 Available Examples

### 1. `example_notion_basic.py` - Basic Operations
**Purpose**: Learn fundamental CRUD operations with Notion API

**Features demonstrated**:
- ✅ Query database with filters
- ✅ Get specific page details
- ✅ Create new pages with properties
- ✅ Update page properties
- ✅ Read page blocks (content)
- ✅ **Add comments using Notion Comments API** (new!)
- ✅ **List comments on a page** (new!)
- ✅ Add callout blocks for visual annotations

**When to use**: Start here if you're new to the provider

**Schedule**: Manual trigger only

**Note**: To use comment features, enable "Insert comments" and "Read comments" capabilities in your [Notion Integration settings](https://www.notion.so/my-integrations).

See: [Working with Comments](https://developers.notion.com/docs/working-with-comments)

---

### 2. `example_notion_workflow.py` - Daily Workflow
**Purpose**: Real-world daily task management automation

**Features demonstrated**:
- ✅ Query tasks due today
- ✅ Conditional branching (skip if no tasks)
- ✅ Create daily summary reports
- ✅ Automated task status updates
- ✅ XCom data passing between tasks
- ✅ Add processing comments

**When to use**: Building automated daily/weekly workflows

**Schedule**: Daily at 9 AM (`0 9 * * *`)

**Use case**: Automatically manage and report on tasks, update statuses, and create summaries

---

### 3. `example_notion_advanced.py` - Advanced Features
**Purpose**: Advanced patterns and best practices

**Features demonstrated**:
- ✅ Complex filters (AND/OR combinations)
- ✅ Pagination for large result sets
- ✅ Database schema introspection
- ✅ Nested block structures
- ✅ Multi-database coordination
- ✅ Error handling patterns

**When to use**: Building complex, production-ready workflows

**Schedule**: Manual trigger only

**Use case**: Enterprise-grade integrations with multiple databases and complex data flows

---

## 🚀 Quick Start

### Prerequisites

1. **Install the provider**:
   ```bash
   pip install airflow-provider-notion
   # OR for development:
   pip install -e .
   ```

2. **Get Notion API token**:
   - Go to https://www.notion.so/my-integrations
   - Create new integration
   - Copy the token (starts with `ntn_` or `secret_`)
   - Share your databases with the integration

3. **Find your database/data source IDs**:
   
   **Option A: From Notion URL**
   ```
   https://notion.so/workspace/abc123def456...
                              ^^^^^^^^^^^^^^^^ <- This is your database ID
   ```
   
   **Option B: Via API** (recommended for 2025-09-03):
   ```python
   from airflow.providers.notion.hooks import NotionHook
   
   hook = NotionHook()
   db_info = hook.get_data_sources('your-database-id')
   
   for ds in db_info['data_sources']:
       print(f"Data Source: {ds['name']}, ID: {ds['id']}")
   ```

### Setup Instructions

#### Step 1: Configure Airflow Connection

**Method 1: Airflow UI**
```
1. Go to Admin → Connections
2. Click "+" to add connection
3. Fill in:
   - Connection Id: notion_default
   - Connection Type: notion
   - Password: ntn_YOUR_TOKEN_HERE
4. Click Save
```

**Method 2: Environment Variable**
```bash
export AIRFLOW_CONN_NOTION_DEFAULT='{"conn_type": "notion", "password": "ntn_YOUR_TOKEN"}'
```

**Method 3: Airflow CLI**
```bash
airflow connections add notion_default \
    --conn-type notion \
    --conn-password ntn_YOUR_TOKEN
```

#### Step 2: Set Airflow Variables

Required variables:
```bash
# Option 1: Airflow UI (Admin → Variables)
notion_data_source_id = "your-data-source-id-here"  # Recommended for API 2025-09-03+
notion_database_id = "your-database-id-here"        # Legacy, still works

# Optional for advanced example:
notion_test_page_id = "existing-page-id"
notion_secondary_data_source_id = "secondary-ds-id"  # For multi-database sync
```

```bash
# Option 2: Airflow CLI
airflow variables set notion_data_source_id "your-data-source-id"
airflow variables set notion_database_id "your-database-id"
airflow variables set notion_test_page_id "your-page-id"
```

#### Step 3: Copy DAGs to Airflow

```bash
# Copy example DAGs to your Airflow DAGs folder
cp examples/dags/*.py ~/airflow/dags/

# OR create symlink
ln -s $(pwd)/examples/dags/*.py ~/airflow/dags/
```

#### Step 4: Run Examples

**Via Airflow UI**:
1. Go to DAGs page
2. Find `example_notion_basic`, `example_notion_workflow`, or `example_notion_advanced`
3. Toggle ON
4. Click "Trigger DAG" (play button)

**Via Airflow CLI**:
```bash
# Run basic example
airflow dags test example_notion_basic

# Run workflow
airflow dags test example_notion_daily_workflow

# Run advanced
airflow dags test example_notion_advanced

# Run specific task
airflow tasks test example_notion_basic query_database 2025-01-01
```

---

## 📖 Example Walkthroughs

### Example 1: Basic Operations Flow

```
Query Database → Get Page → Create Page → Update Page → Get Blocks → Add Comment → Add Rich Comment → List Comments → Add Callout
```

**What it does**:
1. Queries database for pages with Status = "In Progress"
2. Gets details of a specific test page
3. Creates a new page with title, status, dates, description
4. Updates the created page's status to "In Progress"
5. Reads all content blocks from the page
6. **Adds a comment using Notion Comments API** (appears in comment sidebar)
7. **Adds a rich text comment with formatting**
8. **Lists all comments on the page**
9. Adds a callout block for visual annotation (appears in page content)

**Expected output**:
- Console logs showing API responses
- New page created in your Notion database
- Page updated with new status
- Comments added to page (visible in Notion's comment sidebar)
- Callout block added to page content

### Example 2: Daily Workflow Flow

```
Query Due Today → Check Tasks → [No Tasks] → End
                             → [Has Tasks] → Generate Summary → Create Report → 
                                            Process Tasks → Update Report → End
```

**What it does**:
1. Queries tasks due today that aren't completed
2. Branches based on results:
   - If no tasks: skips processing
   - If has tasks: generates summary
3. Creates daily report page
4. Updates "To Do" tasks to "In Progress"
5. Adds automated comments to updated tasks
6. Updates summary with results

**Expected output**:
- Daily summary page in Notion
- Automated status updates on tasks
- Processing comments on each task

### Example 3: Advanced Features

```
Complex Query → Pagination → Schema Inspection → Nested Blocks → 
Multi-DB Sync → Error Handling
```

**What it does**:
1. Queries with complex AND/OR filters
2. Demonstrates pagination for large datasets
3. Inspects database schema and data sources
4. Creates page with rich nested content
5. Syncs completed tasks to archive database
6. Demonstrates error handling patterns

**Expected output**:
- Console logs showing all pages retrieved
- Database schema printed
- Page with complex nested blocks
- Tasks archived to secondary database

---

## 🔧 Customization Guide

### Adapting to Your Database Schema

The examples use generic property names. Update them to match your database:

```python
# Find your property names
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()
db_info = hook.get_data_sources('your-database-id')

for prop_name, prop_config in db_info['properties'].items():
    print(f"{prop_name}: {prop_config['type']}")
```

Then update the DAGs:
```python
# Change from generic:
properties={
    'Name': {...},
    'Status': {...}
}

# To your schema:
properties={
    'Task Name': {...},      # Your title property
    'Project Status': {...}   # Your select property
}
```

### Adding Custom Filters

```python
# Date range filter
filter_params={
    'and': [
        {
            'property': 'Due Date',
            'date': {
                'on_or_after': '{{ ds }}',                    # Today
                'on_or_before': '{{ macros.ds_add(ds, 7) }}'  # 7 days from now
            }
        }
    ]
}

# Text search
filter_params={
    'property': 'Title',
    'title': {
        'contains': 'urgent'
    }
}

# Number range
filter_params={
    'property': 'Priority',
    'number': {
        'greater_than_or_equal_to': 3
    }
}

# Multi-select contains
filter_params={
    'property': 'Tags',
    'multi_select': {
        'contains': 'Important'
    }
}
```

### Modifying Schedules

```python
# Daily at 9 AM
schedule_interval='0 9 * * *'

# Every hour
schedule_interval='0 * * * *'

# Weekly on Monday at 8 AM
schedule_interval='0 8 * * 1'

# Monthly on 1st at midnight
schedule_interval='0 0 1 * *'

# Manual only
schedule_interval=None
```

---

## 🐛 Troubleshooting

### Common Issues

**1. Connection Error: "Invalid token"**
```
Solution:
- Verify token in Airflow connection
- Check token hasn't expired
- Ensure token has correct permissions
- Try generating new token from Notion
```

**2. "Database not found" or "Object not found"**
```
Solution:
- Share database with your integration in Notion
- Verify database/data source ID is correct
- Check you're using data_source_id for API 2025-09-03+
```

**3. "Property not found"**
```
Solution:
- Check property names match your database exactly (case-sensitive)
- Use inspect_database_schema task to see available properties
- Update example code with your property names
```

**4. "No data sources found"**
```
Solution:
- For API 2025-09-03+, databases must have data sources
- Use hook.get_data_sources(database_id) to discover IDs
- Consider using database_id for auto-discovery
```

**5. Rate Limit Errors (429)**
```
Solution:
- Add delays between API calls: time.sleep(0.3)
- Use retry logic in task configuration
- Reduce page_size in queries
- Implement exponential backoff
```

### Debug Mode

Enable detailed logging:
```python
import logging
logging.getLogger('airflow.providers.notion').setLevel(logging.DEBUG)
```

Test connection:
```python
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()
status, message = hook.test_connection()
print(f"Status: {status}, Message: {message}")
```

---

## 📚 Additional Resources

- **Notion API Documentation**: https://developers.notion.com/reference
- **Provider README**: ../README.md
- **Migration Guide**: ../MIGRATION_2025-09-03.md
- **Development Guide**: ../.github/copilot-instructions.md
- **Chinese Documentation**: ../PROMPTS.md

---

## 💡 Best Practices

1. **Use data_source_id for new projects** (API 2025-09-03+)
2. **Implement error handling** for production workflows
3. **Add retries** to handle transient failures
4. **Use XCom** for passing data between tasks
5. **Test with small datasets** before scaling up
6. **Monitor rate limits** and add delays if needed
7. **Document your property names** and database schema
8. **Use Airflow Variables** for configuration
9. **Add logging** for debugging and monitoring
10. **Implement pagination** for large databases

---

## 🤝 Contributing

Found an issue or want to improve the examples?

1. Fork the repository
2. Make your changes
3. Add tests if applicable
4. Submit a pull request

---

## 📝 License

Apache License 2.0 - See LICENSE file for details
