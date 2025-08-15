//! Example for how hook into the polygon p2p network
//!
//! Run with
//!
//! ```sh
//! cargo run -p polygon-p2p
//! ```
//!
//! This launches a regular reth node overriding the engine api payload builder with our custom.
//!
//! Credits to: <https://merkle.io/blog/modifying-reth-to-build-the-fastest-transaction-network-on-bsc-and-polygon>

#![warn(unused_crate_dependencies)]

use chain_cfg::{boot_nodes, head, polygon_chain_spec};
use alloy_consensus::transaction::Transaction as _;
use reth_discv4::{Discv4ConfigBuilder, NatResolver};
use reth_ethereum::network::{
    api::events::SessionInfo, config::NetworkMode, NetworkConfig, NetworkEvent,
    NetworkEventListenerProvider, NetworkManager,
};
use reth_ethereum::network::transactions::NetworkTransactionEvent;
use reth_tracing::{
    tracing::info, tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer,
    Tracer,
};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use chrono::Local;
use reth_network::PeerRequest;
use tokio::sync::Semaphore;
use std::fs::OpenOptions;
use std::io::Write;
use secp256k1::{rand, SecretKey};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio_stream::StreamExt;
use reth_network_types::{PeersConfig, SessionsConfig};
use std::path::Path;

pub mod chain_cfg;
mod analysis;
mod redis_manager;

fn persist_session_established(info: &SessionInfo) {
    const FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/logs/session_established.log");
    if let Some(dir) = Path::new(FILE).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(FILE) {
        let ts = Local::now().format("%Y-%m-%d %H:%M:%S%.6f");
        let line = format!(
            "ts_local={} peer_id={} client={} chain={:?} version={:?}\n",
            ts, info.peer_id, info.client_version, info.status.chain, info.version
        );
        let _ = f.write_all(line.as_bytes());
    }
}

// 移除mempool文件持久化，改用Redis

// 移除tx_analysis文件持久化，改用Redis

// 移除ctf_tx文件持久化，改用Redis

fn build_method_map(abi_json_str: &str) -> HashMap<[u8;4], String> {
    use sha3::{Digest, Keccak256};
    use serde_json::Value;

    fn canonical_type(p: &Value) -> Option<String> {
        let ty = p.get("type")?.as_str()?;
        if let Some(rest) = ty.strip_prefix("tuple") {
            let comps = p.get("components")?.as_array()?;
            let inner: Vec<String> = comps.iter().map(|c| canonical_type(c)).collect::<Option<Vec<_>>>()?;
            Some(format!("({}){}", inner.join(","), rest))
        } else {
            Some(ty.to_string())
        }
    }

    let v: serde_json::Value = serde_json::from_str(abi_json_str).expect("abi json");
    let arr = v.as_array().expect("abi array");
    let mut map: HashMap<[u8;4], String> = HashMap::new();
    for item in arr {
        if item.get("type").and_then(|t| t.as_str()) == Some("function") {
            let name = match item.get("name").and_then(|n| n.as_str()) { Some(n) => n.to_string(), None => continue };
            let inputs = item.get("inputs").and_then(|i| i.as_array()).cloned().unwrap_or_default();
            let mut tys: Vec<String> = Vec::with_capacity(inputs.len());
            for inp in &inputs {
                if let Some(ct) = canonical_type(inp) { tys.push(ct); } else { tys.push(String::from("unknown")); }
            }
            let sig = format!("{}({})", name, tys.join(","));
            let mut hasher = Keccak256::new();
            hasher.update(sig.as_bytes());
            let out = hasher.finalize();
            map.insert([out[0], out[1], out[2], out[3]], name);
        }
    }
    map
}

static CTF_METHODS: Lazy<HashMap<[u8;4], String>> = Lazy::new(|| build_method_map(include_str!("../data/ctf_abi.json")));
static NEGRISK_METHODS: Lazy<HashMap<[u8;4], String>> = Lazy::new(|| build_method_map(include_str!("../data/negrisk_abi.json")));

fn decode_method_from_map(method_id: &[u8], map: &HashMap<[u8;4], String>) -> Option<String> {
    if method_id.len() < 4 { return None; }
    let key = [method_id[0], method_id[1], method_id[2], method_id[3]];
    map.get(&key).cloned()
}

fn decode_ctf_like_calldata(to: alloy_primitives::Address, calldata: &[u8]) -> Option<serde_json::Value> {
    // 仅支持两个目标合约，按 method selector 在各自 ABI 中查找，再利用 alloy-json-abi 的 Function decoder 做解码
    if calldata.len() < 4 { return None; }
    let sel = [calldata[0], calldata[1], calldata[2], calldata[3]];
    let (name_opt, func_opt) = if to == alloy_primitives::address!("0x56C79347e95530c01A2FC76E732f9566dA16E113") {
        let name = CTF_METHODS.get(&sel).cloned();
        let func = find_function_by_selector(include_str!("../data/ctf_abi.json"), &sel);
        (name, func)
    } else if to == alloy_primitives::address!("0x78769D50Be1763ed1CA0D5E878D93f05aabff29e") {
        let name = NEGRISK_METHODS.get(&sel).cloned();
        let func = find_function_by_selector(include_str!("../data/negrisk_abi.json"), &sel);
        (name, func)
    } else { return None };

    let (name, func) = match (name_opt, func_opt) { (Some(n), Some(f)) => (n, f), _ => return None };
    let args_bytes = &calldata[4..];
    // 用 ethers-core 的 ABI 解码器以 JSON ABI function 描述进行解码
    let abi_func: ethers_core::abi::Function = serde_json::from_value(serde_json::to_value(&func).ok()?).ok()?;
    let params = abi_func.decode_input(args_bytes).ok()?;
    Some(function_tokens_to_json_ethers(&name, &params))
}

fn find_function_by_selector(abi_str: &str, sel: &[u8;4]) -> Option<alloy_json_abi::Function> {
    let v: serde_json::Value = serde_json::from_str(abi_str).ok()?;
    let arr = v.as_array()?;
    for item in arr {
        if item.get("type").and_then(|t| t.as_str()) == Some("function") {
            // 重建 selector
            use sha3::{Digest, Keccak256};
            let name = item.get("name")?.as_str()?;
            let inputs = item.get("inputs")?.as_array()?.clone();
            fn canonical_type(p: &serde_json::Value) -> Option<String> {
                let ty = p.get("type")?.as_str()?;
                if let Some(rest) = ty.strip_prefix("tuple") {
                    let comps = p.get("components")?.as_array()?;
                    let inner: Vec<String> = comps.iter().map(|c| canonical_type(c)).collect::<Option<Vec<_>>>()?;
                    Some(format!("({}){}", inner.join(","), rest))
                } else { Some(ty.to_string()) }
            }
            let mut tys = Vec::with_capacity(inputs.len());
            for inp in &inputs { tys.push(canonical_type(inp)?); }
            let sig = format!("{}({})", name, tys.join(","));
            let mut h = Keccak256::new(); h.update(sig.as_bytes()); let out = h.finalize();
            if &out[..4] == sel {
                // 直接反序列化为 Function 以便 decode
                return serde_json::from_value(item.clone()).ok();
            }
        }
    }
    None
}

fn function_tokens_to_json_ethers(name: &str, tokens: &[ethers_core::abi::Token]) -> serde_json::Value {
    use ethers_core::abi::Token as T;
    fn tok_to_val(t: &T) -> serde_json::Value {
        match t {
            T::Address(a) => serde_json::json!(format!("0x{:x}", a)),
            T::Uint(u) => serde_json::json!(u.to_string()),
            T::Int(i) => serde_json::json!(i.to_string()),
            T::Bool(b) => serde_json::json!(b),
            T::Bytes(b) => serde_json::json!(format!("0x{}", hex::encode(b))),
            T::String(s) => serde_json::json!(s),
            T::FixedBytes(b) => serde_json::json!(format!("0x{}", hex::encode(b))),
            T::Array(v) => serde_json::Value::Array(v.iter().map(tok_to_val).collect()),
            T::FixedArray(v) => serde_json::Value::Array(v.iter().map(tok_to_val).collect()),
            T::Tuple(v) => serde_json::Value::Array(v.iter().map(tok_to_val).collect()),
        }
    }
    let params: Vec<serde_json::Value> = tokens.iter().map(tok_to_val).collect();
    serde_json::json!({ "method": name, "args": params })
}

// 移除peer_events文件持久化

#[tokio::main]
async fn main() {
    // 初始化Redis连接
    if let Err(e) = redis_manager::init_redis().await {
        eprintln!("Failed to initialize Redis: {}", e);
        std::process::exit(1);
    }
    info!("Redis initialized successfully");
    // The ECDSA private key used to create our enode identifier.
    let secret_key = SecretKey::new(&mut rand::thread_rng());

    let _ = RethTracer::new()
        .with_stdout(LayerInfo::new(
            LogFormat::Terminal,
            LevelFilter::INFO.to_string(),
            "".to_string(),
            Some("always".to_string()),
        ))
        .init();

    // The local address we want to bind to (use env P2P_PORT or 0 for ephemeral port)
    let port: u16 = std::env::var("P2P_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let local_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), port);

    // The network configuration with aggressive peers/session tuning
    let peers_cfg = PeersConfig::default()
        .with_max_outbound(200)
        .with_max_inbound(200)
        .with_max_concurrent_dials(200)
        .with_refill_slots_interval(Duration::from_millis(1500));

    let mut sessions_cfg = SessionsConfig::default()
        .with_session_event_buffer(8192)
        .with_upscaled_event_buffer(400);
    sessions_cfg.limits = sessions_cfg
        .limits
        .with_max_pending_outbound(400)
        .with_max_established_outbound(200)
        .with_max_established_inbound(200);

    let net_cfg = NetworkConfig::builder(secret_key)
        .set_head(head())
        .network_mode(NetworkMode::Work)
        .listener_addr(local_addr)
        .peer_config(peers_cfg)
        .sessions_config(sessions_cfg)
        .external_ip_resolver(NatResolver::default())
        .build_with_noop_provider(polygon_chain_spec());

    // 打印 forkid 以便核对链配置是否与主网一致
    let chainspec = polygon_chain_spec();
    let fork_id = chainspec.fork_id(&head());
    info!(?fork_id, "computed forkid for polygon mainnet");

    // Set Discv4 lookup more aggressive
    let mut discv4_cfg = Discv4ConfigBuilder::default();
    let interval = Duration::from_millis(250);
    let boot = boot_nodes();
    info!(count = boot.len(), "bootnodes loaded");
    for (i, n) in boot.iter().take(8).enumerate() {
        info!(idx = i, node = %n, "bootnode sample");
    }
    discv4_cfg.add_boot_nodes(boot).lookup_interval(interval);
    let net_cfg = net_cfg.set_discovery_v4(discv4_cfg.build());

    let mut net_manager = NetworkManager::eth(net_cfg).await.unwrap();

    // The network handle is our entrypoint into the network.
    let net_handle = net_manager.handle().clone();
    let mut events = net_handle.event_listener();
    let mut disc_stream = net_handle.discovery_listener();

    // 注册交易事件通道，接收 mempool 广播/请求
    let (tx_event_tx, mut tx_event_rx) = tokio::sync::mpsc::unbounded_channel::<NetworkTransactionEvent>();
    net_manager.set_transactions(tx_event_tx);

    // NetworkManager is a long running task, let's spawn it
    tokio::spawn(net_manager);
    info!("Looking for Polygon peers...");

    // 监听发现事件（仅记录日志，不再手工直连嗅探，避免不合规会话被远端清理）
    tokio::spawn(async move {
        use reth_ethereum::network::api::events::{DiscoveryEvent, DiscoveredEvent};
        while let Some(evt) = disc_stream.next().await {
            match evt {
                DiscoveryEvent::NewNode(DiscoveredEvent::EventQueued { peer_id, addr, fork_id }) => {
                    info!(%peer_id, addr=?addr, fork=?fork_id, "discovery: queued new node");
                }
                DiscoveryEvent::EnrForkId(peer_id, fork) => {
                    info!(%peer_id, fork=?fork, "discovery: enr forkid");
                }
            }
        }
    });

    // 交易事件处理（独立任务，必须在事件循环之前启动）
    // 重要：对每个广播请求并发处理，避免阻塞接收端导致积压，从而处理到“很久以前”的广播
    let handle_for_tx = net_handle.clone();
    let tx_req_semaphore = std::sync::Arc::new(Semaphore::new(128));
    tokio::spawn(async move {
        use reth_ethereum::network::eth_wire::{GetPooledTransactions, NewPooledTransactionHashes, PooledTransactions};
        use tokio::sync::oneshot;
        while let Some(event) = tx_event_rx.recv().await {
            match event {
                NetworkTransactionEvent::IncomingPooledTransactionHashes { peer_id, msg } => {
                    let hashes: Vec<_> = match msg {
                        NewPooledTransactionHashes::Eth66(h) => h.0,
                        NewPooledTransactionHashes::Eth68(h) => h.hashes,
                    };
                    if !hashes.is_empty() {
                        let permit = tx_req_semaphore.clone().acquire_owned().await;
                        let handle = handle_for_tx.clone();
                        tokio::spawn(async move {
                            let _permit = match permit { Ok(p) => p, Err(_) => return };
                            let request = GetPooledTransactions(hashes.clone());
                            let (resp_tx, resp_rx) = oneshot::channel();
                            let peer_req = PeerRequest::GetPooledTransactions { request, response: resp_tx };
                            handle.send_request(peer_id, peer_req);
                            match tokio::time::timeout(Duration::from_millis(2500), resp_rx).await {
                                Ok(Ok(Ok(PooledTransactions(txs)))) => {
                                    let sample: Vec<String> = hashes.iter().take(8).map(|h| format!("0x{:x}", h)).collect();
                                    info!(peer = %peer_id, hashes = hashes.len(), txs = txs.len(), sample = ?sample, "mempool: fetched txs from hashes broadcast");
                                }
                                Ok(Ok(Err(e))) => {
                                    info!(peer = %peer_id, error = %e, "mempool: peer request failed");
                                }
                                Ok(Err(e)) => {
                                    info!(peer = %peer_id, error = %e, "mempool: response channel closed");
                                }
                                Err(_) => {
                                    info!(peer = %peer_id, "mempool: request timeout (2.5s), drop stale response");
                                }
                            }
                        });
                    }
                }
                NetworkTransactionEvent::IncomingTransactions { peer_id, msg } => {
                    // 直接处理 full tx 的广播：解析并存储到Redis
                    let txs = msg.0;
                    tokio::spawn(async move {
                        // 只对特定 receiver 做 input 解析，其余不含 calldata 以节省 IO
                        let want1 = alloy_primitives::address!("0x56C79347e95530c01A2FC76E732f9566dA16E113");
                        let want2 = alloy_primitives::address!("0x78769D50Be1763ed1CA0D5E878D93f05aabff29e");
                        let analyses: Vec<_> = txs
                            .iter()
                            .map(|tx| {
                                let to = tx.to();
                                let include = matches!(to, Some(a) if a == want1 || a == want2);
                                analysis::analyze_transaction_filtered(tx, include)
                            })
                            .collect();
                        let count = analyses.len();
                        let sample: Vec<String> = analyses.iter().take(8).map(|a| format!("0x{:x}", a.hash)).collect();
                        
                        // 存储交易分析结果到Redis
                        if let Err(e) = redis_manager::store_tx_analysis(&analyses).await {
                            eprintln!("Failed to store tx analysis to Redis: {}", e);
                        }

                        // 额外输出 CTF 合约详情到Redis：包含方法名称（若能从 ABI 匹配）
                        for a in &analyses {
                            if let (Some(to), Some(calldata_hex)) = (a.receiver, a.calldata_hex.as_ref()) {
                                let is_ctf = to == want1;
                                let is_neg = to == want2;
                                if is_ctf || is_neg {
                                    let bytes = match calldata_hex.strip_prefix("0x") { Some(h) => h, None => calldata_hex };
                                    if let Ok(b) = hex::decode(bytes) {
                                        let decoded = decode_ctf_like_calldata(to, &b[..]);
                                        let line = serde_json::json!({
                                            "hash": format!("0x{:x}", a.hash),
                                            "to": format!("0x{:x}", to),
                                            "value": format!("{}", a.value),
                                            "gas_limit": a.gas_limit,
                                            "gas_price_or_max_fee": a.gas_price_or_max_fee,
                                            "max_priority_fee": a.max_priority_fee,
                                            "decoded": decoded,
                                        }).to_string();
                                        
                                        if let Err(e) = redis_manager::store_ctf_tx(&line).await {
                                            eprintln!("Failed to store CTF tx to Redis: {}", e);
                                        }
                                    }
                                }
                            }
                        }

                        info!(peer = %peer_id, count, sample = ?sample, "mempool: received full transactions broadcast (analyzed + stored to Redis)");
                    });
                }
                NetworkTransactionEvent::GetPooledTransactions { peer_id, request, response } => {
                    // 仍可最小应答，但为了减少重复请求积压，限制频率：过大批次直接返回空
                    if request.0.len() > 512 {
                        let _ = response.send(Ok(PooledTransactions::default()));
                    } else {
                        let _ = response.send(Ok(PooledTransactions::default()));
                    }
                    info!(peer = %peer_id, count = request.0.len(), "mempool: answered GetPooledTransactions (rate-limited)");
                }
                _ => {}
            }
        }
    });

    while let Some(evt) = events.next().await {
        match evt {
            NetworkEvent::ActivePeerSession { info, .. } => {
                let SessionInfo { ref status, ref client_version, .. } = info;
                let chain = status.chain;
                info!(?chain, ?client_version, "Session established with a new peer.");
                persist_session_established(&info);
                // 周期性轮询移除，改为：收到广播后用 PeerRequest 拉取，降低内部通道关闭导致的 panic 风险
            }
            NetworkEvent::Peer(peer_evt) => {
                match peer_evt {
                    reth_ethereum::network::api::events::PeerEvent::SessionClosed { peer_id, reason } => {
                        info!(%peer_id, ?reason, "Session closed");
                    }
                    reth_ethereum::network::api::events::PeerEvent::SessionEstablished(info) => {
                        info!(peer_id = %info.peer_id, client = %info.client_version, "Peer session established");
                        persist_session_established(&info);
                    }
                    reth_ethereum::network::api::events::PeerEvent::PeerAdded(peer_id) => {
                        info!(%peer_id, "Peer added to the pool");
                    }
                    reth_ethereum::network::api::events::PeerEvent::PeerRemoved(peer_id) => {
                        info!(%peer_id, "Peer removed from the pool");
                    }
                }
            }
        }
    }
    // We will be disconnected from peers since we are not able to answer to network requests
}
