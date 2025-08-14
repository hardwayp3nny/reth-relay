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
use reth_ecies::stream::ECIESStream;
use reth_ethereum::network::eth_wire::{EthMessage, EthStream, HelloMessage, P2PStream, UnauthedEthStream, UnauthedP2PStream, UnifiedStatus};
use reth_ethereum::network::EthNetworkPrimitives;
use reth_network::PeerRequest;
use reth_network_peers::{pk2id, NodeRecord, PeerId};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use secp256k1::{rand, SecretKey};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio_stream::StreamExt;
use reth_network_types::{PeersConfig, SessionsConfig};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod chain_cfg;

fn persist_session_established(info: &SessionInfo) {
    const FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/logs/session_established.log");
    if let Some(dir) = Path::new(FILE).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(FILE) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let line = format!(
            "{} peer_id={} client={} chain={:?} version={:?}\n",
            ts, info.peer_id, info.client_version, info.status.chain, info.version
        );
        let _ = f.write_all(line.as_bytes());
    }
}

fn persist_mempool_sample(peer: &str, hashes_sample: &[String], txs_count: usize) {
    const FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/logs/mempool.log");
    if let Some(dir) = Path::new(FILE).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(FILE) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let _ = f.write_all(
            format!("{} peer={} hashes={} txs={} sample=[{}]\n", ts, peer, hashes_sample.len(), txs_count, hashes_sample.join(","))
                .as_bytes(),
        );
    }
}

fn persist_peer_event(line: &str) {
    const FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/logs/peer_events.log");
    if let Some(dir) = Path::new(FILE).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(FILE) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let _ = f.write_all(format!("{} {}\n", ts, line).as_bytes());
    }
}

#[tokio::main]
async fn main() {
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
                    persist_peer_event(&format!("discovery_queued peer_id={} addr={:?} fork={:?}", peer_id, addr, fork_id));
                }
                DiscoveryEvent::EnrForkId(peer_id, fork) => {
                    info!(%peer_id, fork=?fork, "discovery: enr forkid");
                    persist_peer_event(&format!("enr_fork peer_id={} fork={:?}", peer_id, fork));
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
                            match resp_rx.await {
                                Ok(Ok(PooledTransactions(txs))) => {
                                    let sample: Vec<String> = hashes.iter().take(8).map(|h| format!("0x{:x}", h)).collect();
                                    persist_mempool_sample(&format!("{}", peer_id), &sample, txs.len());
                                    info!(peer = %peer_id, hashes = hashes.len(), txs = txs.len(), "mempool: fetched txs from hashes broadcast");
                                }
                                Ok(Err(e)) => {
                                    info!(peer = %peer_id, error = %e, "mempool: peer request failed");
                                }
                                Err(e) => {
                                    info!(peer = %peer_id, error = %e, "mempool: response channel closed");
                                }
                            }
                        });
                    }
                }
                NetworkTransactionEvent::IncomingTransactions { peer_id, msg } => {
                    // 直接处理 full tx 的广播：并发写盘/处理，避免阻塞接收端
                    let txs = msg.0;
                    tokio::spawn(async move {
                        let count = txs.len();
                        let sample: Vec<String> = txs.iter().take(8).map(|tx| format!("0x{:x}", tx.hash())).collect();
                        persist_mempool_sample(&format!("{}", peer_id), &sample, count);
                        info!(peer = %peer_id, count, "mempool: received full transactions broadcast");
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
                        persist_peer_event(&format!("session_closed peer_id={} reason={:?}", peer_id, reason));
                    }
                    reth_ethereum::network::api::events::PeerEvent::SessionEstablished(info) => {
                        info!(peer_id = %info.peer_id, client = %info.client_version, "Peer session established");
                        persist_session_established(&info);
                        persist_peer_event(&format!("session_established peer_id={} client={} version={:?}", info.peer_id, info.client_version, info.version));
                    }
                    reth_ethereum::network::api::events::PeerEvent::PeerAdded(peer_id) => {
                        info!(%peer_id, "Peer added to the pool");
                        persist_peer_event(&format!("peer_added peer_id={}", peer_id));
                    }
                    reth_ethereum::network::api::events::PeerEvent::PeerRemoved(peer_id) => {
                        info!(%peer_id, "Peer removed from the pool");
                        persist_peer_event(&format!("peer_removed peer_id={}", peer_id));
                    }
                }
            }
        }
    }
    // We will be disconnected from peers since we are not able to answer to network requests
}
