# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.11.0.post8] - 2025-11-29

### Added
- **Search API Support**: Added complete Notion Search API functionality
  - New `NotionHook.search()` method for searching pages and databases
  - New `NotionSearchOperator` for easy integration in DAGs
  - Support for keyword search, object type filtering, sorting, and pagination
  - Comprehensive error handling and logging
  
- **Documentation**:
  - Added `SEARCH_FEATURE.md` with detailed feature documentation
  - Added `SEARCH_QUICKSTART.md` for quick start guide
  - Added `examples/dags/example_notion_search.py` with 7 example tasks
  - Added `scripts/test_search.py` for testing search functionality
  - Updated `README.md` with search API examples

- **Examples**:
  - Search all pages in workspace
  - Search with keyword filter
  - Search databases
  - Paginated search for large result sets
  - Process and filter search results

### Changed
- Updated `airflow/providers/notion/operators/__init__.py` to export `NotionSearchOperator`
- Enhanced documentation with search API usage examples

### Fixed
- Fixed typo in README.md import statement for `NotionUpdatePageOperator`

## [2.11.0.post7] - 2025-11-XX

### Previous releases
- Basic CRUD operations for Notion API
- Support for Notion API version 2025-09-03
- Data source and database query support
- Page creation, update, and retrieval
- Block operations (get children, append children)
- SSL configuration support
- Retry logic for network resilience

---

## Migration Guide

### From 2.11.0.post7 to 2.11.0.post8

No breaking changes. All existing code will continue to work.

**New features available**:

```python
# Use the new Search Operator
from airflow.providers.notion.operators import NotionSearchOperator

search = NotionSearchOperator(
    task_id="search_pages",
    filter_object_type="page",
    page_size=100
)

# Or use the Hook directly
from airflow.providers.notion.hooks import NotionHook

hook = NotionHook()
results = hook.search(
    filter_params={"property": "object", "value": "page"}
)
```

---

## Links

- [PyPI](https://pypi.org/project/airflow-provider-notion/)
- [GitHub](https://github.com/hiyenwong/airflow-notion-provider)
- [Documentation](https://github.com/hiyenwong/airflow-notion-provider)

