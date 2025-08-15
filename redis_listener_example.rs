// Redis WebSocket监听示例
// 编译: rustc --edition 2021 redis_listener_example.rs
// 或者: cargo run --bin redis_listener_example

use redis::{Client, Commands, PubSubCommands};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let client = Client::open(redis_url)?;
    
    println!("连接到Redis，开始监听mempool事件...");
    
    // 创建pubsub连接
    let mut conn = client.get_connection()?;
    let mut pubsub = conn.as_pubsub();
    
    // 订阅两个频道
    pubsub.subscribe("mempool:tx_analysis")?;
    pubsub.subscribe("mempool:ctf_tx")?;
    
    println!("已订阅频道: mempool:tx_analysis, mempool:ctf_tx");
    println!("等待消息...\n");
    
    loop {
        let msg = pubsub.get_message()?;
        let channel: String = msg.get_channel_name().to_string();
        let payload: String = msg.get_payload()?;
        
        println!("频道: {}", channel);
        println!("数据: {}", payload);
        println!("---");
        
        // 可以在这里添加WebSocket转发逻辑
        // 例如：将数据发送到WebSocket客户端
    }
}
