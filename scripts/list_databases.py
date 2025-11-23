#!/usr/bin/env python
"""
List all Notion databases accessible by your integration.

Usage:
    export NOTION_API_TOKEN="ntn_xxxxx..."
    python scripts/list_databases.py
"""

import os
import sys
import requests


def list_databases(api_token: str) -> None:
    """List all accessible Notion databases."""

    url = "https://api.notion.com/v1/search"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }

    # Search for data sources (API 2025-09-03 changed "database" to "data_source")
    data = {
        "filter": {"value": "data_source", "property": "object"},
        "sort": {"direction": "descending", "timestamp": "last_edited_time"},
    }

    print("🔍 Searching for Notion databases...\n")

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()

        databases = result.get("results", [])

        if not databases:
            print("❌ No databases found!")
            print("\nPossible reasons:")
            print("1. Your integration hasn't been added to any databases")
            print("2. The API token is invalid")
            print("\nTo add your integration to a database:")
            print("- Open the database in Notion")
            print("- Click the '...' menu in the top right")
            print("- Click 'Add connections'")
            print("- Select your integration")
            return

        print(f"✅ Found {len(databases)} data source(s):\n")
        print("=" * 80)

        for i, db in enumerate(databases, 1):
            # In API 2025-09-03, search returns data_source objects directly
            db_id = db["id"]
            db_type = db.get("type", "unknown")

            # Get title from parent_database if available
            parent_db = db.get("parent_database", {})
            parent_title = parent_db.get("title", "")

            # Get title from data source properties
            title_parts = db.get("title", [])
            if title_parts:
                title = "".join([t.get("plain_text", "") for t in title_parts])
            elif parent_title:
                title = parent_title
            else:
                title = "Untitled"

            # Get URL
            url = db.get("url", "N/A")

            print(f"\n{i}. {title}")
            print(f"   Data Source ID: {db_id}")
            print(f"   Type: {db_type}")
            print(f"   URL: {url}")

            # Get parent database info if available
            if parent_db:
                parent_id = parent_db.get("id", "N/A")
                print(f"   Parent Database ID: {parent_id}")

            print("   " + "-" * 76)

        print("\n" + "=" * 80)
        print("\n💡 Usage in Airflow:")
        print("   在 Airflow 中使用这些 ID：")
        print("")
        print("   方法 1 - 使用 Data Source ID (推荐):")
        print("      airflow variables set notion_data_source_id <DATA_SOURCE_ID>")
        print("")
        print("   方法 2 - 使用 Parent Database ID (如果有):")
        print("      airflow variables set notion_database_id <PARENT_DATABASE_ID>")
        print("")
        print("   在 DAG 中使用:")
        print("      data_source_id='{{ var.value.notion_data_source_id }}'")
        print("      # 或")
        print(
            "      database_id='{{ var.value.notion_database_id }}'  # 会自动发现 data source"
        )

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        print(f"   Status Code: {response.status_code}")
        print(f"   Response: {response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def get_database_details(api_token: str, database_id: str) -> None:
    """Get detailed information about a specific database."""

    url = f"https://api.notion.com/v1/databases/{database_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Notion-Version": "2025-09-03",
    }

    print(f"\n🔍 Getting details for database: {database_id}\n")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        db = response.json()

        # Get title
        title_parts = db.get("title", [])
        title = (
            "".join([t.get("plain_text", "") for t in title_parts])
            if title_parts
            else "Untitled"
        )

        print("=" * 80)
        print(f"Database: {title}")
        print(f"ID: {db['id']}")
        print(f"URL: {db.get('url', 'N/A')}")
        print(f"Created: {db.get('created_time', 'N/A')}")
        print(f"Last Edited: {db.get('last_edited_time', 'N/A')}")

        # Data sources
        data_sources = db.get("data_sources", [])
        print(f"\nData Sources: {len(data_sources)}")
        for i, ds in enumerate(data_sources, 1):
            print(f"  {i}. ID: {ds.get('id')}")
            print(f"     Type: {ds.get('type', 'unknown')}")

        # Properties
        properties = db.get("properties", {})
        print(f"\nProperties ({len(properties)}):")
        for prop_name, prop_data in properties.items():
            prop_type = prop_data.get("type", "unknown")
            print(f"  - {prop_name}: {prop_type}")

        print("=" * 80)

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        print(f"   Status Code: {response.status_code}")
        print(f"   Response: {response.text}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Get API token from environment
    api_token = os.environ.get("NOTION_API_TOKEN")

    if not api_token:
        print("❌ Error: NOTION_API_TOKEN environment variable not set")
        print("\nUsage:")
        print("   export NOTION_API_TOKEN='ntn_xxxxx...'")
        print("   python scripts/list_databases.py")
        print("\nOr provide a specific database ID to get details:")
        print("   python scripts/list_databases.py <database_id>")
        sys.exit(1)

    # If database ID provided, show details
    if len(sys.argv) > 1:
        database_id = sys.argv[1]
        get_database_details(api_token, database_id)
    else:
        # Otherwise, list all databases
        list_databases(api_token)
