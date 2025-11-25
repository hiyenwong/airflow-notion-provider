# SSL 证书验证配置指南

## 概述

从版本 0.0.2.4 开始，Airflow Notion Provider 支持禁用 SSL 证书验证。这对于以下场景很有用：

- 🧪 **开发/测试环境**：快速绕过 SSL 问题进行测试
- 🔒 **企业内网环境**：使用自签名证书或内部 CA
- 🐛 **SSL 故障排除**：临时禁用 SSL 验证来诊断网络问题

⚠️ **警告**: 禁用 SSL 验证会使连接不安全。**永远不要在生产环境中禁用 SSL 验证！**

## 配置方法

### 方法 1: 通过 Airflow UI 配置

1. 打开 Airflow Web UI
2. 进入 **Admin** → **Connections**
3. 找到或创建 `notion_default` 连接
4. 在 **Extra** 字段中添加以下 JSON：

```json
{
  "verify_ssl": false
}
```

完整配置示例：

```json
{
  "headers": {
    "Notion-Version": "2025-09-03"
  },
  "verify_ssl": false
}
```

5. 点击 **Save**

### 方法 2: 通过命令行配置

```bash
# 创建带有禁用 SSL 的连接
airflow connections add notion_default \
    --conn-type notion \
    --conn-password "ntn_your_token_here" \
    --conn-extra '{"verify_ssl": false}'

# 或更新现有连接
airflow connections export notion_default --file-format json > /tmp/conn.json
# 编辑 /tmp/conn.json，添加 "verify_ssl": false
airflow connections import /tmp/conn.json
```

### 方法 3: 通过环境变量配置

```bash
export AIRFLOW_CONN_NOTION_DEFAULT='{"conn_type": "notion", "password": "ntn_your_token", "extra": "{\"verify_ssl\": false}"}'
```

### 方法 4: 在代码中直接配置（不推荐）

```python
from airflow.providers.notion.hooks.notion import NotionHook

# 创建 Hook
hook = NotionHook(notion_conn_id='notion_default')

# 手动禁用 SSL 验证（需在调用 get_conn() 之前）
hook.verify_ssl = False

# 使用 Hook
database = hook.get_database('database-id')
```

## 验证配置

运行以下命令验证 SSL 配置：

```bash
cd /Users/hiyenwong/projects/funda_ai/airflow-notion-provider

# 设置 API Token
export NOTION_API_TOKEN="your_token_here"

# 运行诊断脚本
python scripts/test_ssl_connection.py
```

或在 Airflow 中测试连接：

```bash
# 测试连接
airflow connections test notion_default
```

## 日志输出

### SSL 验证启用时（默认）

```
[INFO] Using Notion API token: ntn_562817***C9Og
[INFO] Session headers configured: ['Content-Type', 'Notion-Version', 'Authorization']
[INFO] Configured retry strategy: 3 retries with exponential backoff
[INFO] SSL 验证: 启用
[INFO] Base URL: https://api.notion.com/v1
```

### SSL 验证禁用时

```
[WARNING] ⚠️  SSL 证书验证已禁用! 这不安全，仅用于开发/测试环境
[INFO] Using Notion API token: ntn_562817***C9Og
[INFO] Session headers configured: ['Content-Type', 'Notion-Version', 'Authorization']
[INFO] Configured retry strategy: 3 retries with exponential backoff
[INFO] Base URL: https://api.notion.com/v1
```

## 常见问题

### Q1: 为什么会出现 SSL 错误？

**A**: 常见原因包括：

1. **网络问题**：不稳定的网络连接
2. **VPN/代理**：公司 VPN 或代理干扰 SSL 握手
3. **SSL 库版本**：过时的 Python SSL 库
4. **防火墙**：防火墙阻止 HTTPS 连接
5. **自签名证书**：使用自签名证书的内网环境

### Q2: 如何判断是否应该禁用 SSL？

**A**: 仅在以下情况考虑禁用 SSL：

- ✅ 开发/测试环境
- ✅ 内网环境使用自签名证书
- ✅ 临时故障排除
- ❌ **生产环境（绝对不要！）**

### Q3: 禁用 SSL 后仍然有问题怎么办？

**A**: 尝试以下步骤：

1. **更新依赖包**：
   ```bash
   pip install --upgrade urllib3 requests certifi
   ```

2. **检查网络连接**：
   ```bash
   curl -v https://api.notion.com
   ```

3. **临时禁用 VPN**

4. **查看详细日志**：
   ```bash
   airflow dags test example_notion_basic --verbose
   ```

### Q4: 如何恢复 SSL 验证？

**A**: 

方法 1 - UI：删除 Extra 中的 `"verify_ssl": false`

方法 2 - 命令行：
```bash
airflow connections export notion_default --file-format json > /tmp/conn.json
# 编辑文件，删除 "verify_ssl": false
airflow connections import /tmp/conn.json
```

方法 3 - 直接更新：
```bash
airflow connections delete notion_default
airflow connections add notion_default \
    --conn-type notion \
    --conn-password "ntn_your_token_here"
```

## 安全最佳实践

### ✅ 推荐做法

1. **生产环境始终启用 SSL 验证**
2. **仅在隔离的开发/测试环境中禁用 SSL**
3. **禁用 SSL 时使用专门的测试 Token，不要用生产 Token**
4. **在代码审查时检查 SSL 配置**
5. **定期更新 SSL/TLS 相关库**：
   ```bash
   pip install --upgrade urllib3 requests certifi
   ```

### ❌ 不要做

1. ❌ 在生产环境禁用 SSL
2. ❌ 在公共网络上禁用 SSL
3. ❌ 长期禁用 SSL 而不解决根本问题
4. ❌ 在禁用 SSL 时使用敏感数据

## 技术细节

### 实现方式

```python
# 在 NotionHook 中
class NotionHook(BaseHook):
    def __init__(self, notion_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.verify_ssl = True  # 默认启用
    
    def get_conn(self) -> requests.Session:
        # 从 Connection Extra 读取配置
        if conn.extra:
            extra = json.loads(conn.extra)
            if "verify_ssl" in extra:
                self.verify_ssl = extra["verify_ssl"]
                if not self.verify_ssl:
                    # 禁用 urllib3 的 InsecureRequestWarning
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    def query_data_source(self, data_source_id: str, ...) -> Dict[str, Any]:
        # 所有 HTTP 请求都使用 verify 参数
        response = session.post(url, json=data, timeout=30, verify=self.verify_ssl)
```

### 影响的方法

所有 HTTP 请求方法都支持 SSL 配置：

- `test_connection()`
- `get_data_sources()`
- `query_data_source()`
- `query_database()`
- `get_database()`
- `create_page()`
- `update_page()`
- `get_page()`
- `get_block_children()`
- `append_block_children()`

## 相关文档

- [Notion API 文档](https://developers.notion.com/reference)
- [Requests SSL 验证文档](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification)
- [Python SSL 模块文档](https://docs.python.org/3/library/ssl.html)

## 获取帮助

如果遇到 SSL 相关问题：

1. 运行诊断脚本：`python scripts/test_ssl_connection.py`
2. 查看 [GitHub Issues](https://github.com/hiyenwong/airflow-notion-provider/issues)
3. 阅读 `.github/copilot-instructions.md` 中的故障排除指南
