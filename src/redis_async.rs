use redis::{aio::MultiplexedConnection, Client, RedisResult};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, OnceCell};
use uuid::Uuid;
use crate::analysis::TxAnalysisResult;

// Redis键的前缀
const TX_ANALYSIS_PREFIX: &str = "tx_analysis:";
const CTF_TX_PREFIX: &str = "ctf_tx:";

// Redis频道名称
pub const TX_ANALYSIS_CHANNEL: &str = "mempool:tx_analysis";
pub const CTF_TX_CHANNEL: &str = "mempool:ctf_tx";

// TTL设置为15秒
const TTL_SECONDS: usize = 15;

// 批处理大小
const BATCH_SIZE: usize = 50;
const CHANNEL_BUFFER_SIZE: usize = 10000;

#[derive(Debug, Clone)]
pub enum RedisOperation {
    TxAnalysis(Vec<TxAnalysisResult>),
    CtfTx(String),
}

#[derive(Debug)]
struct BatchedOperations {
    tx_analyses: Vec<TxAnalysisResult>,
    ctf_txs: Vec<String>,
    processed_hashes: HashSet<String>,
}

static REDIS_CLIENT: OnceCell<Client> = OnceCell::const_new();
static REDIS_SENDER: OnceCell<mpsc::UnboundedSender<RedisOperation>> = OnceCell::const_new();

/// 初始化Redis异步处理器
pub async fn init_redis_async() -> RedisResult<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    let client = Client::open(redis_url)?;
    
    // 创建多路复用连接（真正的连接池）
    let conn = client.get_multiplexed_async_connection().await?;
    
    REDIS_CLIENT.set(client).map_err(|_| {
        redis::RedisError::from((redis::ErrorKind::ClientError, "Failed to set Redis client"))
    })?;
    
    // 创建异步处理器通道
    let (tx, rx) = mpsc::unbounded_channel();
    
    REDIS_SENDER.set(tx).map_err(|_| {
        redis::RedisError::from((redis::ErrorKind::ClientError, "Failed to set Redis sender"))
    })?;
    
    // 启动后台处理器
    tokio::spawn(redis_processor(conn, rx));
    
    println!("🚀 Redis异步处理器已启动");
    Ok(())
}

/// 后台Redis处理器 - 批量处理操作
async fn redis_processor(
    conn: MultiplexedConnection,
    mut rx: mpsc::UnboundedReceiver<RedisOperation>,
) {
    let conn = Arc::new(Mutex::new(conn));
    let mut batch = BatchedOperations {
        tx_analyses: Vec::with_capacity(BATCH_SIZE),
        ctf_txs: Vec::with_capacity(BATCH_SIZE),
        processed_hashes: HashSet::new(),
    };
    
    // 批处理定时器
    let mut batch_timer = tokio::time::interval(tokio::time::Duration::from_millis(100));
    
    loop {
        tokio::select! {
            // 接收新操作
            Some(operation) = rx.recv() => {
                match operation {
                    RedisOperation::TxAnalysis(analyses) => {
                        for analysis in analyses {
                            let hash = format!("{:x}", analysis.hash);
                            if !batch.processed_hashes.contains(&hash) {
                                batch.tx_analyses.push(analysis);
                                batch.processed_hashes.insert(hash);
                            }
                        }
                    }
                    RedisOperation::CtfTx(data) => {
                        // 从JSON中提取哈希
                        if let Ok(json) = serde_json::from_str::<JsonValue>(&data) {
                            if let Some(hash) = json.get("hash").and_then(|h| h.as_str()) {
                                if !batch.processed_hashes.contains(hash) {
                                    batch.ctf_txs.push(data);
                                    batch.processed_hashes.insert(hash.to_string());
                                }
                            }
                        }
                    }
                }
                
                // 如果批次已满，立即处理
                if batch.tx_analyses.len() >= BATCH_SIZE || batch.ctf_txs.len() >= BATCH_SIZE {
                    if let Err(e) = process_batch(&conn, &mut batch).await {
                        eprintln!("❌ 批处理失败: {}", e);
                    }
                }
            }
            
            // 定时处理批次（即使没满）
            _ = batch_timer.tick() => {
                if !batch.tx_analyses.is_empty() || !batch.ctf_txs.is_empty() {
                    if let Err(e) = process_batch(&conn, &mut batch).await {
                        eprintln!("❌ 定时批处理失败: {}", e);
                    }
                }
            }
        }
    }
}

/// 处理一个批次的操作
async fn process_batch(
    conn: &Arc<Mutex<MultiplexedConnection>>,
    batch: &mut BatchedOperations,
) -> RedisResult<()> {
    if batch.tx_analyses.is_empty() && batch.ctf_txs.is_empty() {
        return Ok(());
    }
    
    let start = std::time::Instant::now();
    let mut conn = conn.lock().await;
    
    // 构建批量pipeline
    let mut pipe = redis::pipe();
    let mut publish_commands = Vec::new();
    
    // 处理tx_analysis数据
    for analysis in &batch.tx_analyses {
        let storage_key = generate_key_with_timestamp(TX_ANALYSIS_PREFIX);
        
        let mut value = serde_json::to_value(analysis).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::ClientError, "Serialization failed", e.to_string()))
        })?;
        
        if let JsonValue::Object(ref mut obj) = value {
            obj.insert(
                "ts_local".to_string(),
                JsonValue::String(chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            );
        }
        
        let json_str = serde_json::to_string(&value).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::ClientError, "Serialization failed", e.to_string()))
        })?;
        
        // 添加到pipeline（不做去重检查，提高性能）
        pipe.cmd("SET").arg(&storage_key).arg(&json_str).arg("EX").arg(TTL_SECONDS);
        
        // 准备发布命令
        publish_commands.push((TX_ANALYSIS_CHANNEL, json_str));
    }
    
    // 处理ctf_tx数据
    for ctf_data in &batch.ctf_txs {
        let storage_key = generate_key_with_timestamp(CTF_TX_PREFIX);
        
        let mut json_value: JsonValue = serde_json::from_str(ctf_data).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::ClientError, "JSON parse failed", e.to_string()))
        })?;
        
        if let JsonValue::Object(ref mut obj) = json_value {
            obj.insert(
                "ts_local".to_string(),
                JsonValue::String(chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            );
        }
        
        let json_str = serde_json::to_string(&json_value).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::ClientError, "Serialization failed", e.to_string()))
        })?;
        
        // 添加到pipeline
        pipe.cmd("SET").arg(&storage_key).arg(&json_str).arg("EX").arg(TTL_SECONDS);
        
        // 准备发布命令
        publish_commands.push((CTF_TX_CHANNEL, json_str));
    }
    
    // 执行批量存储
    if !batch.tx_analyses.is_empty() || !batch.ctf_txs.is_empty() {
        pipe.query_async::<_, ()>(&mut *conn).await?;
        
        // 批量发布（使用单独的pipeline避免阻塞）
        let mut publish_pipe = redis::pipe();
        for (channel, data) in publish_commands {
            publish_pipe.cmd("PUBLISH").arg(channel).arg(data);
        }
        publish_pipe.query_async::<_, ()>(&mut *conn).await?;
    }
    
    let duration = start.elapsed();
    let total_items = batch.tx_analyses.len() + batch.ctf_txs.len();
    
    println!(
        "📦 批处理完成: {} tx_analysis + {} ctf_tx = {} items, 耗时: {:?}",
        batch.tx_analyses.len(),
        batch.ctf_txs.len(),
        total_items,
        duration
    );
    
    // 清空批次
    batch.tx_analyses.clear();
    batch.ctf_txs.clear();
    batch.processed_hashes.clear();
    
    Ok(())
}

/// 生成带时间戳的唯一键（优化版本）
fn generate_key_with_timestamp(prefix: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let uuid = Uuid::new_v4().simple(); // 使用simple格式，更短
    format!("{}{}__{}", prefix, timestamp, uuid)
}

/// 异步存储交易分析结果（非阻塞）
pub fn store_tx_analysis_async(results: Vec<TxAnalysisResult>) -> Result<(), &'static str> {
    if results.is_empty() {
        return Ok(());
    }
    
    if let Some(sender) = REDIS_SENDER.get() {
        if let Err(_) = sender.send(RedisOperation::TxAnalysis(results)) {
            return Err("Redis channel已关闭");
        }
        Ok(())
    } else {
        Err("Redis未初始化")
    }
}

/// 异步存储CTF交易（非阻塞）
pub fn store_ctf_tx_async(data: String) -> Result<(), &'static str> {
    if let Some(sender) = REDIS_SENDER.get() {
        if let Err(_) = sender.send(RedisOperation::CtfTx(data)) {
            return Err("Redis channel已关闭");
        }
        Ok(())
    } else {
        Err("Redis未初始化")
    }
}

/// 获取性能统计（可选实现）
pub async fn get_performance_stats() -> RedisResult<String> {
    // 可以添加性能监控统计
    Ok("性能统计功能待实现".to_string())
}
