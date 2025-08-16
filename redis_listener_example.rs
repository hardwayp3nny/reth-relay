// Redis WebSocket服务器
// 编译: cargo run --bin redis_listener_example
// 
// 使用方法:
// 1. 启动服务器: cargo run --bin redis_listener_example
// 2. WebSocket连接: ws://localhost:8080/ws?token=your_secret_token
// 3. 环境变量:
//    - REDIS_URL: Redis连接地址 (默认: redis://127.0.0.1:6379)
//    - WS_AUTH_TOKEN: WebSocket验证令牌 (默认: secret_token_123)
//    - WS_PORT: WebSocket服务端口 (默认: 8080)

use axum::{
    extract::{Query, WebSocketUpgrade, State},
    response::{Html, Response, IntoResponse},
    routing::get,
    Router,
};
use axum::extract::ws::{WebSocket, Message};
use futures_util::{sink::SinkExt, stream::StreamExt};
use redis::{Client, PubSubCommands};
use serde::Deserialize;
use std::{
    env,
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{broadcast, Mutex};
use tracing::{info, warn, error};
use tracing_subscriber::fmt;

// WebSocket查询参数
#[derive(Deserialize)]
struct WsQuery {
    token: String,
}

// 应用状态
#[derive(Clone)]
struct AppState {
    auth_token: String,
    tx: broadcast::Sender<(String, String)>, // (channel, payload)
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    fmt::init();
    
    // 读取环境变量
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let auth_token = env::var("WS_AUTH_TOKEN").unwrap_or_else(|_| "secret_token_123".to_string());
    let port: u16 = env::var("WS_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap_or(8080);
    
    info!("正在连接到Redis: {}", redis_url);
    let redis_client = Client::open(redis_url)?;
    
    // 创建广播通道用于Redis消息分发
    let (tx, _rx) = broadcast::channel(1000);
    
    let app_state = AppState {
        auth_token,
        tx: tx.clone(),
    };
    
    // 启动Redis监听器
    let redis_tx = tx.clone();
    let redis_client_clone = redis_client.clone();
    tokio::spawn(async move {
        if let Err(e) = redis_listener(redis_client_clone, redis_tx).await {
            error!("Redis监听器错误: {}", e);
        }
    });
    
    // 创建路由
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(websocket_handler))
        .with_state(app_state);
    
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("WebSocket服务器启动在 {}", addr);
    info!("连接URL: ws://localhost:{}/ws?token={}", port, env::var("WS_AUTH_TOKEN").unwrap_or_else(|_| "secret_token_123".to_string()));
    info!("测试页面: http://localhost:{}", port);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

// 首页 - 提供WebSocket测试页面
async fn index() -> Html<&'static str> {
    Html(r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>Redis WebSocket 测试</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .container { max-width: 800px; }
            .log { background: #f5f5f5; padding: 20px; height: 400px; overflow-y: scroll; border: 1px solid #ddd; }
            .controls { margin: 20px 0; }
            input, button { padding: 8px; margin: 5px; }
            .status { font-weight: bold; }
            .connected { color: green; }
            .disconnected { color: red; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Redis WebSocket 监听器测试</h1>
            <div class="controls">
                <input type="text" id="token" placeholder="认证令牌" value="secret_token_123">
                <button onclick="connect()">连接</button>
                <button onclick="disconnect()">断开</button>
                <span id="status" class="status disconnected">未连接</span>
            </div>
            <div id="log" class="log"></div>
        </div>
        
        <script>
            let ws = null;
            const log = document.getElementById('log');
            const status = document.getElementById('status');
            
            function addLog(message) {
                const time = new Date().toLocaleTimeString();
                log.innerHTML += `[${time}] ${message}<br>`;
                log.scrollTop = log.scrollHeight;
            }
            
            function connect() {
                const token = document.getElementById('token').value;
                const wsUrl = `ws://localhost:8080/ws?token=${token}`;
                
                ws = new WebSocket(wsUrl);
                
                ws.onopen = function() {
                    status.textContent = '已连接';
                    status.className = 'status connected';
                    addLog('WebSocket 连接成功');
                };
                
                ws.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    addLog(`<strong>频道:</strong> ${data.channel}<br><strong>数据:</strong> ${data.payload}`);
                };
                
                ws.onclose = function() {
                    status.textContent = '未连接';
                    status.className = 'status disconnected';
                    addLog('WebSocket 连接关闭');
                };
                
                ws.onerror = function(error) {
                    addLog(`连接错误: ${error}`);
                };
            }
            
            function disconnect() {
                if (ws) {
                    ws.close();
                }
            }
        </script>
    </body>
    </html>
    "#)
}

// WebSocket处理器
async fn websocket_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsQuery>,
    State(state): State<AppState>,
) -> Response {
    // 验证令牌
    if params.token != state.auth_token {
        warn!("WebSocket认证失败: token = {}", params.token);
        return axum::http::StatusCode::UNAUTHORIZED.into_response();
    }
    
    info!("WebSocket认证成功，建立连接");
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

// 处理WebSocket连接
async fn handle_websocket(socket: WebSocket, state: AppState) {
    let (sender, mut receiver) = socket.split();
    let mut redis_rx = state.tx.subscribe();
    
    // 生成客户端ID
    let client_id = uuid::Uuid::new_v4().to_string();
    info!("新的WebSocket客户端连接: {}", client_id);
    
    // 使用Arc<Mutex>包装sender以便在多个任务中共享
    let sender = Arc::new(Mutex::new(sender));
    let sender_clone = sender.clone();
    
    // 启动接收Redis消息的任务
    let sender_task = tokio::spawn(async move {
        while let Ok((channel, payload)) = redis_rx.recv().await {
            let message = serde_json::json!({
                "channel": channel,
                "payload": payload,
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            if let Ok(text) = serde_json::to_string(&message) {
                let mut sender_guard = sender_clone.lock().await;
                if sender_guard.send(Message::Text(text)).await.is_err() {
                    break;
                }
            }
        }
    });
    
    // 处理客户端消息（主要是ping/pong）
    let receiver_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(_)) => {
                    // 可以处理客户端发送的命令
                }
                Ok(Message::Ping(ping)) => {
                    // 响应ping
                    let mut sender_guard = sender.lock().await;
                    if sender_guard.send(Message::Pong(ping)).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) => {
                    break;
                }
                _ => {}
            }
        }
    });
    
    // 等待任务完成
    tokio::select! {
        _ = sender_task => {},
        _ = receiver_task => {},
    }
    
    info!("WebSocket客户端断开连接: {}", client_id);
}

// Redis监听器
async fn redis_listener(
    client: Client,
    tx: broadcast::Sender<(String, String)>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("开始Redis监听器...");
    
    let mut conn = client.get_connection()?;
    let mut pubsub = conn.as_pubsub();
    
    // 订阅两个频道
    pubsub.subscribe("mempool:tx_analysis")?;
    pubsub.subscribe("mempool:ctf_tx")?;
    
    info!("已订阅Redis频道: mempool:tx_analysis, mempool:ctf_tx");
    
    loop {
        match pubsub.get_message() {
            Ok(msg) => {
                let channel: String = msg.get_channel_name().to_string();
                let payload: String = msg.get_payload().unwrap_or_default();
                
                info!("收到Redis消息 - 频道: {}, 大小: {} bytes", channel, payload.len());
                
                // 广播到所有WebSocket客户端
                if let Err(_) = tx.send((channel.clone(), payload.clone())) {
                    warn!("广播Redis消息失败，没有活跃的WebSocket客户端");
                }
            }
            Err(e) => {
                error!("Redis消息接收错误: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}
