# Redis WebSocket 服务器

这是一个基于Rust编写的WebSocket服务器，用于监听Redis频道消息并实时转发给WebSocket客户端。

## 功能特性

- 🔐 **身份验证**: 连接需要提供有效的认证令牌
- 📡 **实时转发**: 实时监听Redis频道并转发消息到WebSocket客户端
- 🌐 **Web界面**: 提供内置的测试页面
- 📊 **多频道支持**: 同时监听多个Redis频道
- 🔄 **自动重连**: Redis连接断开后自动重试

## 监听的频道

- `mempool:tx_analysis` - 交易分析数据
- `mempool:ctf_tx` - CTF代币交易数据

## 快速开始

### 1. 启动WebSocket服务器

```bash
# 使用默认配置启动
cargo run --bin redis_listener_example

# 或者指定环境变量
REDIS_URL=redis://127.0.0.1:6379 \
WS_AUTH_TOKEN=your_secret_token \
WS_PORT=8080 \
cargo run --bin redis_listener_example
```

### 2. 访问测试页面

打开浏览器访问: http://localhost:8080

### 3. WebSocket连接

WebSocket连接地址格式:
```
ws://localhost:8080/ws?token=your_secret_token
```

## 环境变量配置

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `REDIS_URL` | Redis连接地址 | `redis://127.0.0.1:6379` |
| `WS_AUTH_TOKEN` | WebSocket认证令牌 | `secret_token_123` |
| `WS_PORT` | WebSocket服务端口 | `8080` |

## 消息格式

WebSocket客户端接收到的消息格式为JSON:

```json
{
  "channel": "mempool:tx_analysis",
  "payload": "{\"tx_hash\":\"0x123...\",\"analysis\":{...}}",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## 测试工具

### Python WebSocket客户端

```bash
# 安装依赖
pip install websockets

# 运行测试客户端
python3 websocket_test_client.py
```

### Redis消息发布器

```bash
# 安装依赖
pip install redis

# 运行测试发布器
python3 redis_publisher_test.py
```

## 使用示例

### JavaScript/Node.js

```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080/ws?token=secret_token_123');

ws.on('open', function open() {
  console.log('WebSocket连接成功');
});

ws.on('message', function message(data) {
  const msg = JSON.parse(data);
  console.log('频道:', msg.channel);
  console.log('数据:', JSON.parse(msg.payload));
});

ws.on('error', function error(err) {
  console.error('WebSocket错误:', err);
});
```

### Python

```python
import asyncio
import websockets
import json

async def client():
    uri = "ws://localhost:8080/ws?token=secret_token_123"
    async with websockets.connect(uri) as websocket:
        async for message in websocket:
            data = json.loads(message)
            print(f"频道: {data['channel']}")
            print(f"数据: {data['payload']}")

asyncio.run(client())
```

## 安全注意事项

1. **认证令牌**: 在生产环境中请使用强随机令牌
2. **HTTPS**: 生产环境建议使用WSS（WebSocket over TLS）
3. **访问控制**: 建议配置防火墙限制访问

## 故障排除

### 常见问题

1. **连接被拒绝 (401 Unauthorized)**
   - 检查认证令牌是否正确
   - 确认环境变量 `WS_AUTH_TOKEN` 设置

2. **Redis连接失败**
   - 检查Redis服务是否运行
   - 验证Redis连接地址和端口
   - 检查防火墙设置

3. **WebSocket连接超时**
   - 确认服务器正在运行
   - 检查端口是否被占用
   - 验证防火墙设置

### 调试日志

服务器启动时会输出详细的日志信息，包括:
- Redis连接状态
- WebSocket服务器地址
- 客户端连接/断开事件
- 消息转发统计

## 性能优化

- 使用广播通道避免消息重复处理
- 异步处理Redis消息和WebSocket发送
- 自动清理断开的WebSocket连接
- 支持多个并发WebSocket客户端

## 扩展建议

1. **消息过滤**: 添加基于频道或内容的消息过滤
2. **消息缓存**: 为新连接的客户端提供历史消息
3. **监控指标**: 添加Prometheus指标导出
4. **负载均衡**: 支持多实例部署和负载均衡
5. **消息压缩**: 为大型消息添加压缩支持
