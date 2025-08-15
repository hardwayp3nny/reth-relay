use redis::{aio::ConnectionManager, Client, RedisResult};
use serde_json::Value as JsonValue;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;
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

static REDIS_CLIENT: OnceCell<Client> = OnceCell::const_new();
static REDIS_CONN: OnceCell<ConnectionManager> = OnceCell::const_new();

/// 初始化Redis连接
pub async fn init_redis() -> RedisResult<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    let client = Client::open(redis_url)?;
    let conn = ConnectionManager::new(client.clone()).await?;
    
    REDIS_CLIENT.set(client).map_err(|_| {
        redis::RedisError::from((redis::ErrorKind::ClientError, "Failed to set Redis client"))
    })?;
    
    REDIS_CONN.set(conn).map_err(|_| {
        redis::RedisError::from((redis::ErrorKind::ClientError, "Failed to set Redis connection"))
    })?;
    
    Ok(())
}

/// 获取Redis连接
async fn get_connection() -> RedisResult<&'static ConnectionManager> {
    REDIS_CONN.get().ok_or_else(|| {
        redis::RedisError::from((redis::ErrorKind::ClientError, "Redis not initialized"))
    })
}

/// 生成带时间戳的唯一键
fn generate_key_with_timestamp(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let uuid = Uuid::new_v4();
    format!("{}{}__{}", prefix, timestamp, uuid)
}

/// 存储交易分析结果到Redis
pub async fn store_tx_analysis(results: &[TxAnalysisResult]) -> RedisResult<()> {
    if results.is_empty() {
        return Ok(());
    }

    let mut conn = get_connection().await?.clone();
    
    for result in results {
        // 使用交易哈希作为去重键
        let hash_key = format!("{}hash:{:x}", TX_ANALYSIS_PREFIX, result.hash);
        
        // 检查是否已存在（去重）
        let exists: bool = redis::cmd("EXISTS")
            .arg(&hash_key)
            .query_async(&mut conn)
            .await?;
        
        if !exists {
            // 生成唯一存储键
            let storage_key = generate_key_with_timestamp(TX_ANALYSIS_PREFIX);
            
            // 添加时间戳
            let mut value = serde_json::to_value(result).map_err(|e| {
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
            
            // 使用pipeline提高性能
            let mut pipe = redis::pipe();
            pipe.cmd("SET").arg(&storage_key).arg(&json_str).arg("EX").arg(TTL_SECONDS);
            pipe.cmd("SET").arg(&hash_key).arg("1").arg("EX").arg(TTL_SECONDS);
            pipe.query_async::<_, ()>(&mut conn).await?;
            
            // 发布到频道
            redis::cmd("PUBLISH")
                .arg(TX_ANALYSIS_CHANNEL)
                .arg(&json_str)
                .query_async::<_, ()>(&mut conn)
                .await?;
        }
    }
    
    Ok(())
}

/// 存储CTF交易到Redis
pub async fn store_ctf_tx(data: &str) -> RedisResult<()> {
    let mut conn = get_connection().await?.clone();
    
    // 解析JSON以获取哈希
    let mut json_value: JsonValue = serde_json::from_str(data).map_err(|e| {
        redis::RedisError::from((redis::ErrorKind::ClientError, "JSON parse failed", e.to_string()))
    })?;
    
    // 添加时间戳
    if let JsonValue::Object(ref mut obj) = json_value {
        obj.insert(
            "ts_local".to_string(),
            JsonValue::String(chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        );
    }
    
    // 获取哈希用于去重
    let hash = json_value.get("hash")
        .and_then(|h| h.as_str())
        .ok_or_else(|| {
            redis::RedisError::from((redis::ErrorKind::ClientError, "Missing hash field"))
        })?;
    
    let hash_key = format!("{}hash:{}", CTF_TX_PREFIX, hash);
    
    // 检查是否已存在（去重）
    let exists: bool = redis::cmd("EXISTS")
        .arg(&hash_key)
        .query_async(&mut conn)
        .await?;
    
    if !exists {
        // 生成唯一存储键
        let storage_key = generate_key_with_timestamp(CTF_TX_PREFIX);
        
        let json_str = serde_json::to_string(&json_value).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::ClientError, "Serialization failed", e.to_string()))
        })?;
        
        // 使用pipeline提高性能
        let mut pipe = redis::pipe();
        pipe.cmd("SET").arg(&storage_key).arg(&json_str).arg("EX").arg(TTL_SECONDS);
        pipe.cmd("SET").arg(&hash_key).arg("1").arg("EX").arg(TTL_SECONDS);
        pipe.query_async::<_, ()>(&mut conn).await?;
        
        // 发布到频道
        redis::cmd("PUBLISH")
            .arg(CTF_TX_CHANNEL)
            .arg(&json_str)
            .query_async::<_, ()>(&mut conn)
            .await?;
    }
    
    Ok(())
}

/// 获取所有交易分析数据（用于调试）
pub async fn get_all_tx_analysis() -> RedisResult<Vec<String>> {
    let mut conn = get_connection().await?.clone();
    
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*", TX_ANALYSIS_PREFIX))
        .query_async(&mut conn)
        .await?;
    
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    
    let values: Vec<String> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(&mut conn)
        .await?;
    
    Ok(values)
}

/// 获取所有CTF交易数据（用于调试）
pub async fn get_all_ctf_tx() -> RedisResult<Vec<String>> {
    let mut conn = get_connection().await?.clone();
    
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*", CTF_TX_PREFIX))
        .query_async(&mut conn)
        .await?;
    
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    
    let values: Vec<String> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(&mut conn)
        .await?;
    
    Ok(values)
}

/// 清理过期的键（Redis会自动处理，但可以手动清理用于测试）
pub async fn cleanup_expired_keys() -> RedisResult<()> {
    let mut conn = get_connection().await?.clone();
    
    // 删除所有过期的键（实际上Redis会自动处理）
    let tx_keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*", TX_ANALYSIS_PREFIX))
        .query_async(&mut conn)
        .await?;
    
    let ctf_keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*", CTF_TX_PREFIX))
        .query_async(&mut conn)
        .await?;
    
    if !tx_keys.is_empty() || !ctf_keys.is_empty() {
        let mut all_keys = tx_keys;
        all_keys.extend(ctf_keys);
        
        // 这里可以检查TTL并删除过期的键，但通常让Redis自动处理即可
        for key in &all_keys {
            let ttl: i64 = redis::cmd("TTL")
                .arg(key)
                .query_async(&mut conn)
                .await?;
            
            if ttl == -2 {  // 键已过期
                redis::cmd("DEL")
                    .arg(key)
                    .query_async::<_, ()>(&mut conn)
                    .await?;
            }
        }
    }
    
    Ok(())
}
