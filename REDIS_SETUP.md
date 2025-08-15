# Redis集成使用说明

## 概述
项目已成功集成Redis，替代了文件持久化存储，实现了更高效的内存池数据管理和实时事件通知。

## 主要变更

### 1. 移除的文件持久化
- ❌ `mempool.log` - 删除
- ❌ `peer_events.log` - 删除  
- ❌ `tx_analysis.jsonl` - 改为Redis存储
- ❌ `ctf_tx.jsonl` - 改为Redis存储
- ✅ `session_established.log` - 保留文件持久化

### 2. Redis存储结构

#### 交易分析数据 (tx_analysis)
- **前缀**: `tx_analysis:`
- **格式**: `tx_analysis:{timestamp}__{uuid}`
- **去重键**: `tx_analysis:hash:{tx_hash}`
- **TTL**: 15秒
- **频道**: `mempool:tx_analysis`

#### CTF交易数据 (ctf_tx)  
- **前缀**: `ctf_tx:`
- **格式**: `ctf_tx:{timestamp}__{uuid}`
- **去重键**: `ctf_tx:hash:{tx_hash}`
- **TTL**: 15秒
- **频道**: `mempool:ctf_tx`

### 3. 数据特性
- ✅ **自动过期**: 所有数据15秒后自动删除
- ✅ **哈希去重**: 同一交易哈希只存储一次
- ✅ **实时推送**: 每次存储都会发布到对应频道
- ✅ **高性能**: 使用Redis pipeline批量操作

## 使用方法

### 1. 启动Redis服务器
```bash
# 使用Docker启动Redis
docker run -d -p 6379:6379 redis:alpine

# 或者本地安装启动
redis-server
```

### 2. 配置Redis连接
```bash
# 设置Redis连接URL（可选，默认为localhost:6379）
export REDIS_URL="redis://127.0.0.1:6379"
```

### 3. 运行项目
```bash
cd reth-polygon-relay
cargo run
```

### 4. 监听Redis事件
```bash
# 使用Redis CLI监听频道
redis-cli SUBSCRIBE mempool:tx_analysis mempool:ctf_tx

# 或者使用提供的示例脚本
rustc --edition 2021 redis_listener_example.rs
./redis_listener_example
```

### 5. 查看存储的数据
```bash
# 查看所有交易分析数据键
redis-cli KEYS "tx_analysis:*"

# 查看所有CTF交易数据键  
redis-cli KEYS "ctf_tx:*"

# 获取具体数据
redis-cli GET "tx_analysis:1704067200000__some-uuid"
```

## Redis频道事件示例

### 交易分析事件 (mempool:tx_analysis)
```json
{
  "hash": "0x1234567890abcdef...",
  "tx_type": "Eip1559",
  "sender": "0xabcdef1234567890...",
  "receiver": "0x56C79347e95530c01A2FC76E732f9566dA16E113",
  "value": "1000000000000000000",
  "gas_limit": 21000,
  "gas_price_or_max_fee": 20000000000,
  "max_priority_fee": 1000000000,
  "input_len": 4,
  "method_id": "0xa9059cbb",
  "calldata_hex": "0xa9059cbb000000000000000000000000...",
  "ts_local": "2024-01-01 12:00:00.123456"
}
```

### CTF交易事件 (mempool:ctf_tx)
```json
{
  "hash": "0x1234567890abcdef...",
  "to": "0x56C79347e95530c01A2FC76E732f9566dA16E113",
  "value": "0",
  "gas_limit": 150000,
  "gas_price_or_max_fee": 30000000000,
  "max_priority_fee": 2000000000,
  "decoded": {
    "method": "transfer",
    "args": ["0xrecipient...", "1000"]
  },
  "ts_local": "2024-01-01 12:00:00.123456"
}
```

## WebSocket集成建议

基于Redis频道，你可以轻松构建WebSocket服务器：

```rust
// 伪代码示例
async fn websocket_server() {
    let mut pubsub = redis_client.get_async_connection().await.into_pubsub();
    pubsub.subscribe(&["mempool:tx_analysis", "mempool:ctf_tx"]).await;
    
    while let Some(msg) = pubsub.on_message().next().await {
        let channel = msg.get_channel_name();
        let data = msg.get_payload::<String>();
        
        // 转发到所有WebSocket客户端
        broadcast_to_websocket_clients(channel, data).await;
    }
}
```

## 性能优势

1. **内存效率**: 数据自动过期，避免内存泄漏
2. **高并发**: Redis原生支持高并发读写
3. **实时性**: 事件即时推送，无需轮询
4. **可扩展**: 支持多个消费者订阅同一频道
5. **去重机制**: 避免重复处理相同交易

## 故障排除

### Redis连接失败
- 检查Redis服务是否运行
- 验证REDIS_URL配置
- 确认网络连接和防火墙设置

### 数据丢失
- Redis数据有15秒TTL，这是正常行为
- 如需持久化特定数据，考虑额外存储方案

### 频道无消息
- 确认已正确订阅频道
- 检查是否有交易数据被处理
- 验证目标合约地址配置
