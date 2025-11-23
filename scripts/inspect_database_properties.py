"""Inspect actual properties of a Notion database."""

import os
import requests

NOTION_API_TOKEN = os.environ.get("NOTION_API_TOKEN")
DATA_SOURCE_ID = "2afd1aa7-fe2f-80b0-af0e-000b1a23eb97"

headers = {
    "Authorization": f"Bearer {NOTION_API_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

# Get data source details
response = requests.get(
    f"https://api.notion.com/v1/data_sources/{DATA_SOURCE_ID}", headers=headers
)

if response.ok:
    data = response.json()
    properties = data.get("properties", {})

    print("=" * 60)
    print("数据库属性列表")
    print("=" * 60)
    print()

    for prop_name, prop_info in properties.items():
        prop_type = prop_info.get("type")
        print(f"属性名: {prop_name!r}")
        print(f"  类型: {prop_type}")

        # Show options for select/status types
        if prop_type == "select":
            options = prop_info.get("select", {}).get("options", [])
            if options:
                print(f"  选项: {[opt['name'] for opt in options]}")
        elif prop_type == "status":
            groups = prop_info.get("status", {}).get("groups", [])
            all_options = []
            for group in groups:
                all_options.extend([opt["name"] for opt in group.get("options", [])])
            print(f"  状态: {all_options}")
        elif prop_type == "multi_select":
            options = prop_info.get("multi_select", {}).get("options", [])
            if options:
                print(f"  选项: {[opt['name'] for opt in options]}")

        print()

    print("=" * 60)
    print("生成的 DAG properties 示例：")
    print("=" * 60)
    print()
    print("properties = {")

    # Generate sample properties based on actual schema
    for prop_name, prop_info in properties.items():
        prop_type = prop_info.get("type")

        if prop_type == "title":
            print(
                f"    {prop_name!r}: {{'title': [{{'text': {{'content': 'Your Title'}}}}]}},"
            )
        elif prop_type == "status":
            groups = prop_info.get("status", {}).get("groups", [])
            if groups and groups[0].get("options"):
                first_status = groups[0]["options"][0]["name"]
                print(f"    {prop_name!r}: {{'status': {{'name': {first_status!r}}}}},")
        elif prop_type == "select":
            options = prop_info.get("select", {}).get("options", [])
            if options:
                first_option = options[0]["name"]
                print(f"    {prop_name!r}: {{'select': {{'name': {first_option!r}}}}},")
        elif prop_type == "multi_select":
            options = prop_info.get("multi_select", {}).get("options", [])
            if options:
                first_option = options[0]["name"]
                print(
                    f"    {prop_name!r}: {{'multi_select': [{{'name': {first_option!r}}}]}},"
                )
        elif prop_type == "date":
            print(f"    {prop_name!r}: {{'date': {{'start': '2025-11-23'}}}},")
        elif prop_type == "checkbox":
            print(f"    {prop_name!r}: {{'checkbox': False}},")
        elif prop_type == "number":
            print(f"    {prop_name!r}: {{'number': 1}},")
        elif prop_type == "rich_text":
            print(
                f"    {prop_name!r}: {{'rich_text': [{{'text': {{'content': 'Text content'}}}}]}},"
            )
        elif prop_type == "url":
            print(f"    {prop_name!r}: {{'url': 'https://example.com'}},")
        elif prop_type == "email":
            print(f"    {prop_name!r}: {{'email': 'example@example.com'}},")
        elif prop_type == "phone_number":
            print(f"    {prop_name!r}: {{'phone_number': '+1234567890'}},")
        elif prop_type in [
            "people",
            "files",
            "relation",
            "rollup",
            "created_time",
            "created_by",
            "last_edited_time",
            "last_edited_by",
            "formula",
        ]:
            print(f"    # {prop_name!r}: {prop_type} (只读或需特殊处理)")

    print("}")

else:
    print(f"错误 {response.status_code}: {response.text}")
