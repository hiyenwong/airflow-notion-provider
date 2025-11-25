#!/usr/bin/env python3
"""
快速配置 Airflow Notion Connection 禁用 SSL 验证

使用方法:
    python scripts/disable_ssl_verification.py
"""

import sys
import json
import subprocess


def check_airflow_cli():
    """检查 Airflow CLI 是否可用"""
    try:
        result = subprocess.run(
            ["airflow", "version"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            print(f"✅ Airflow CLI 可用: {result.stdout.strip()}")
            return True
        else:
            print("❌ Airflow CLI 不可用")
            return False
    except Exception as e:
        print(f"❌ 无法执行 Airflow 命令: {e}")
        return False


def get_connection_info(conn_id="notion_default"):
    """获取现有连接信息"""
    try:
        result = subprocess.run(
            ["airflow", "connections", "get", conn_id, "--output", "json"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        return None
    except Exception as e:
        print(f"⚠️  无法获取连接信息: {e}")
        return None


def update_connection_disable_ssl(conn_id="notion_default"):
    """更新连接配置，禁用 SSL 验证"""
    print("\n" + "=" * 80)
    print(f"更新连接: {conn_id}")
    print("=" * 80)

    # 获取现有连接
    conn_info = get_connection_info(conn_id)

    if not conn_info:
        print(f"\n⚠️  连接 '{conn_id}' 不存在")
        print("\n请先创建连接:")
        print(f"  airflow connections add {conn_id} \\")
        print("    --conn-type notion \\")
        print("    --conn-password 'your_notion_token' \\")
        print("    --conn-extra '{\"verify_ssl\": false}'")
        return False

    # 解析现有 Extra
    extra = {}
    if conn_info.get("extra"):
        try:
            extra = json.loads(conn_info["extra"])
        except json.JSONDecodeError:
            print("⚠️  现有 Extra 字段格式错误，将创建新的")

    # 添加 verify_ssl: false
    extra["verify_ssl"] = False

    # 构建更新命令
    extra_json = json.dumps(extra)
    password = conn_info.get("password", "")

    if not password:
        print("\n❌ 连接没有配置 Password (Notion API Token)")
        print("请先设置 Token:")
        print(f"  airflow connections delete {conn_id}")
        print(f"  airflow connections add {conn_id} \\")
        print("    --conn-type notion \\")
        print("    --conn-password 'your_notion_token' \\")
        print(f"    --conn-extra '{extra_json}'")
        return False

    # 删除旧连接
    print("\n删除旧连接...")
    subprocess.run(["airflow", "connections", "delete", conn_id], capture_output=True)

    # 创建新连接
    print("创建新连接 (禁用 SSL 验证)...")
    cmd = [
        "airflow",
        "connections",
        "add",
        conn_id,
        "--conn-type",
        "notion",
        "--conn-password",
        password,
        "--conn-extra",
        extra_json,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✅ 成功! 连接 '{conn_id}' 已更新")
        print("\n当前配置:")
        print(f"  - Connection ID: {conn_id}")
        print("  - Type: notion")
        print(
            f"  - Password: {password[:10]}***{password[-4:] if len(password) > 14 else '***'}"
        )
        print(f"  - Extra: {json.dumps(extra, indent=2)}")
        print("\n⚠️  警告: SSL 证书验证已禁用! 这不安全，仅用于开发/测试环境")
        return True
    else:
        print(f"❌ 失败: {result.stderr}")
        return False


def test_connection(conn_id="notion_default"):
    """测试连接"""
    print("\n" + "=" * 80)
    print("测试连接")
    print("=" * 80)

    try:
        result = subprocess.run(
            ["airflow", "connections", "test", conn_id],
            capture_output=True,
            text=True,
            timeout=30,
        )

        print(result.stdout)
        if result.returncode == 0:
            print("✅ 连接测试成功!")
            return True
        else:
            print("❌ 连接测试失败")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False


def enable_ssl_verification(conn_id="notion_default"):
    """启用 SSL 验证"""
    print("\n" + "=" * 80)
    print(f"恢复 SSL 验证: {conn_id}")
    print("=" * 80)

    conn_info = get_connection_info(conn_id)

    if not conn_info:
        print(f"❌ 连接 '{conn_id}' 不存在")
        return False

    # 解析现有 Extra
    extra = {}
    if conn_info.get("extra"):
        try:
            extra = json.loads(conn_info["extra"])
        except json.JSONDecodeError:
            pass

    # 移除或设置 verify_ssl: true
    if "verify_ssl" in extra:
        del extra["verify_ssl"]

    # 更新连接
    extra_json = json.dumps(extra) if extra else "{}"
    password = conn_info.get("password", "")

    # 删除旧连接
    print("\n删除旧连接...")
    subprocess.run(["airflow", "connections", "delete", conn_id], capture_output=True)

    # 创建新连接
    print("创建新连接 (启用 SSL 验证)...")
    cmd = [
        "airflow",
        "connections",
        "add",
        conn_id,
        "--conn-type",
        "notion",
        "--conn-password",
        password,
    ]
    if extra:
        cmd.extend(["--conn-extra", extra_json])

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print("✅ 成功! SSL 验证已恢复")
        print("\n当前配置:")
        print(f"  - Connection ID: {conn_id}")
        print("  - SSL 验证: 启用 (默认)")
        return True
    else:
        print(f"❌ 失败: {result.stderr}")
        return False


def main():
    print("🔧 Airflow Notion Connection - SSL 配置工具\n")

    # 检查 Airflow CLI
    if not check_airflow_cli():
        print("\n请确保:")
        print("1. Airflow 已安装: pip install apache-airflow")
        print("2. Airflow 环境变量已设置")
        sys.exit(1)

    # 菜单
    print("\n" + "=" * 80)
    print("请选择操作:")
    print("=" * 80)
    print("1. 禁用 SSL 验证 (用于开发/测试)")
    print("2. 恢复 SSL 验证 (推荐)")
    print("3. 测试连接")
    print("4. 查看当前配置")
    print("0. 退出")
    print("=" * 80)

    choice = input("\n请输入选项 (0-4): ").strip()

    conn_id = input("Connection ID [notion_default]: ").strip() or "notion_default"

    if choice == "1":
        if update_connection_disable_ssl(conn_id):
            # 询问是否测试
            test = input("\n是否测试连接? (y/N): ").strip().lower()
            if test == "y":
                test_connection(conn_id)

    elif choice == "2":
        enable_ssl_verification(conn_id)

    elif choice == "3":
        test_connection(conn_id)

    elif choice == "4":
        conn_info = get_connection_info(conn_id)
        if conn_info:
            print("\n" + "=" * 80)
            print("当前配置")
            print("=" * 80)
            print(json.dumps(conn_info, indent=2))

            # 检查 SSL 状态
            extra = {}
            if conn_info.get("extra"):
                try:
                    extra = json.loads(conn_info["extra"])
                except:
                    pass

            verify_ssl = extra.get("verify_ssl", True)
            ssl_status = "启用" if verify_ssl else "禁用 (不安全)"
            print(f"\nSSL 验证: {ssl_status}")

    elif choice == "0":
        print("\n再见!")
        sys.exit(0)

    else:
        print("\n❌ 无效选项")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n操作已取消")
        sys.exit(0)
