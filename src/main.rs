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
use reth_tracing::{
    tracing::info, tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer,
    Tracer,
};
use reth_ecies::stream::ECIESStream;
use reth_ethereum::network::eth_wire::{EthMessage, HelloMessage, UnauthedEthStream, UnauthedP2PStream, UnifiedStatus};
use reth_ethereum::network::EthNetworkPrimitives;
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
use std::collections::{HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use serde_json::json;
use alloy_consensus::transaction::Transaction as _;
use alloy_consensus::transaction::SignerRecoverable as _;
use alloy_primitives::{Address, B256, TxKind};

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

fn persist_tps_line(inst_tps: u64, avg60: f64) {
    const FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/logs/tps.log");
    if let Some(dir) = Path::new(FILE).parent() { let _ = std::fs::create_dir_all(dir); }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(FILE) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let _ = f.write_all(format!("{} tps={} avg60={:.2}\n", ts, inst_tps, avg60).as_bytes());
    }
}

fn persist_tx_json(tx_json: &serde_json::Value) {
    const FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/logs/txs.jsonl");
    if let Some(dir) = Path::new(FILE).parent() { let _ = std::fs::create_dir_all(dir); }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(FILE) {
        let _ = f.write_all(tx_json.to_string().as_bytes());
        let _ = f.write_all(b"\n");
    }
}

fn format_address(addr: &Address) -> String { format!("0x{:x}", addr) }
fn format_b256(h: &B256) -> String { format!("0x{:x}", h) }

fn tx_to_json(ts: u64, peer: &str, tx: &reth_ethereum_primitives::PooledTransactionVariant) -> serde_json::Value {
    let hash = tx.hash();
    let nonce = tx.nonce();
    let gas_limit = tx.gas_limit();
    let gas_price = tx.gas_price();
    let max_fee_per_gas = tx.max_fee_per_gas();
    let max_priority_fee_per_gas = tx.max_priority_fee_per_gas();
    let chain_id = tx.chain_id();
    let value = tx.value();
    let input_len = tx.input().len();
    let to = match tx.kind() { TxKind::Call(to) => Some(format_address(&to)), TxKind::Create => None };
    let from = tx.recover_signer().ok().map(|a| format_address(&a));
    // Infer tx type by presence of features
    let tx_type = if tx.authorization_list().map(|l| !l.is_empty()).unwrap_or(false) {
        "eip7702"
    } else if tx.blob_versioned_hashes().map(|l| !l.is_empty()).unwrap_or(false) {
        "eip4844"
    } else if tx.max_priority_fee_per_gas().is_some() {
        "eip1559"
    } else if tx.access_list().map(|l| !l.0.is_empty()).unwrap_or(false) {
        "eip2930"
    } else {
        "legacy"
    };

    json!({
        "ts": ts,
        "peer": peer,
        "hash": format_b256(&hash),
        "tx_type": tx_type,
        "gas_limit": gas_limit,
        "gas_price": gas_price.map(|v| v.to_string()),
        "max_fee_per_gas": max_fee_per_gas.to_string(),
        "max_priority_fee_per_gas": max_priority_fee_per_gas.map(|v| v.to_string()),
        "to": to,
        "from": from,
        "nonce": nonce,
        "value": value.to_string(),
        "chain_id": chain_id.map(|v| v.to_string()),
        "input_len": input_len,
    })
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

    // The local address we want to bind to
    let local_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 30303);

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

    let net_manager = NetworkManager::eth(net_cfg).await.unwrap();

    // The network handle is our entrypoint into the network.
    let net_handle = net_manager.handle().clone();
    let mut events = net_handle.event_listener();
    let mut disc_stream = net_handle.discovery_listener();

    // NetworkManager is a long running task, let's spawn it
    tokio::spawn(net_manager);
    info!("Looking for Polygon peers...");

    // === TPS Reporter ===
    let tps_counter = Arc::new(AtomicU64::new(0));
    let tps_counter_bg = tps_counter.clone();
    tokio::spawn(async move {
        let mut window: VecDeque<u64> = VecDeque::with_capacity(60);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let count = tps_counter_bg.swap(0, Ordering::Relaxed);
            if window.len() == 60 { window.pop_front(); }
            window.push_back(count);
            let sum: u64 = window.iter().copied().sum();
            let avg = if window.is_empty() { 0.0 } else { sum as f64 / window.len() as f64 };
            persist_tps_line(count, avg);
        }
    });

    // === Hash queue + Workers for GetPooledTransactions ===
    let (hash_tx, mut hash_rx) = tokio::sync::mpsc::channel::<(PeerId, Vec<B256>)>(10_000);
    let fetch_workers: usize = std::env::var("FETCH_WORKERS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let batch_size: usize = std::env::var("BATCH_SIZE").ok().and_then(|s| s.parse().ok()).unwrap_or(256);
    let fetch_sem = Arc::new(Semaphore::new(fetch_workers));
    {
        let handle = net_handle.clone();
        let fetch_sem = fetch_sem.clone();
        tokio::spawn(async move {
            while let Some((peer_id, hashes)) = hash_rx.recv().await {
                if hashes.is_empty() { continue; }
                let permit = match fetch_sem.clone().acquire_owned().await { Ok(p) => p, Err(_) => continue };
                let handle2 = handle.clone();
                let hashes2 = hashes.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Some(txh) = handle2.transactions_handle().await {
                        for chunk in hashes2.chunks(batch_size) {
                            match txh.get_pooled_transactions_from(peer_id, chunk.to_vec()).await {
                                Ok(Some(txs)) => {
                                    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                                    for tx in txs.iter() {
                                        let js = tx_to_json(ts, &format!("{}", peer_id), tx);
                                        persist_tx_json(&js);
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    info!(peer = %peer_id, error = %e, "worker: failed to get pooled txs");
                                }
                            }
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                });
            }
        });
    }

    // 监听发现事件（节点被发现/返回 ENR ForkId）
    let net_handle_for_disc = net_handle.clone();
    let hash_tx_for_disc = hash_tx.clone();
    // 高并发嗅探控制：可通过环境变量 SNOOP_MAX_CONCURRENCY 配置，默认 256
    let snoop_max: usize = std::env::var("SNOOP_MAX_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let snoop_sem = std::sync::Arc::new(Semaphore::new(snoop_max));
    // 已嗅探或正在嗅探的 peer 去重
    let seen = std::sync::Arc::new(Mutex::new(std::collections::HashSet::<PeerId>::new()));
    tokio::spawn(async move {
        use reth_ethereum::network::api::events::{DiscoveryEvent, DiscoveredEvent};
        while let Some(evt) = disc_stream.next().await {
            match evt {
                DiscoveryEvent::NewNode(DiscoveredEvent::EventQueued { peer_id, addr, fork_id }) => {
                    info!(%peer_id, addr=?addr, fork=?fork_id, "discovery: queued new node");
                    persist_peer_event(&format!("discovery_queued peer_id={} addr={:?} fork={:?}", peer_id, addr, fork_id));
                    // 对新发现节点尝试底层握手 + 监听 mempool 广播
                    let chainspec = chain_cfg::polygon_chain_spec();
                    let head = chain_cfg::head();
                    let fork_filter = chainspec.fork_filter(head);
                    let status = UnifiedStatus::spec_builder(&chainspec, &head);
                    let peer_enode: NodeRecord = NodeRecord::new(addr.tcp(), peer_id);
                    let sk = *net_handle_for_disc.secret_key();
                    let sem = snoop_sem.clone();
                    let seen_peers = seen.clone();
                    let tx_hash_sender = hash_tx_for_disc.clone();
                    let tps = tps_counter.clone();
                    tokio::spawn(async move {
                        // 去重：同一个 peer 只嗅探一次
                        {
                            let mut s = seen_peers.lock().await;
                            if !s.insert(peer_id) {
                                return;
                            }
                        }
                        // 并发限制
                        let permit = match sem.acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => return,
                        };
                        // 连接并握手 P2P
                        if let Ok(Ok(outgoing)) = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(peer_enode.tcp_addr())).await {
                            if let Ok(ecies) = ECIESStream::connect(outgoing, sk, peer_enode.id).await {
                                let our_peer_id = pk2id(&sk.public_key(secp256k1::SECP256K1));
                                let hello = HelloMessage::builder(our_peer_id).build();
                                if let Ok((p2p_stream, _their_hello)) = UnauthedP2PStream::new(ecies).handshake(hello).await {
                                    // ETH 握手
                                    let mut status = status.clone();
                                    if let Ok(ver) = p2p_stream.shared_capabilities().eth().map(|v| v.version()) {
                                        status.version = ver.try_into().unwrap_or(status.version);
                                    }
                                    if let Ok((mut eth_stream, _their_status)) = UnauthedEthStream::new(p2p_stream).handshake::<EthNetworkPrimitives>(status, fork_filter).await {
                                        // 监听广播
                                        while let Some(Ok(update)) = eth_stream.next().await {
                                            match update {
                                                EthMessage::NewPooledTransactionHashes66(txs) => {
                                                    let count = txs.0.len();
                                                    let sample: Vec<String> = txs.0.iter().take(8).map(|h| format!("0x{:x}", h)).collect();
                                                    persist_mempool_sample(&format!("{}", peer_enode.id), &sample, count);
                                                    tps.fetch_add(count as u64, Ordering::Relaxed);
                                                    let _ = tx_hash_sender.try_send((peer_enode.id, txs.0));
                                                }
                                                EthMessage::NewPooledTransactionHashes68(txs) => {
                                                    let count = txs.hashes.len();
                                                    let sample: Vec<String> = txs.hashes.iter().take(8).map(|h| format!("0x{:x}", h)).collect();
                                                    persist_mempool_sample(&format!("{}", peer_enode.id), &sample, count);
                                                    tps.fetch_add(count as u64, Ordering::Relaxed);
                                                    let _ = tx_hash_sender.try_send((peer_enode.id, txs.hashes));
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        drop(permit);
                    });
                }
                DiscoveryEvent::EnrForkId(peer_id, fork) => {
                    info!(%peer_id, fork=?fork, "discovery: enr forkid");
                    persist_peer_event(&format!("enr_fork peer_id={} fork={:?}", peer_id, fork));
                }
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
                // 主动向该 peer 请求 mempool 交易（仅将哈希推入队列，正文由 worker 拉取）
                let handle = net_handle.clone();
                let session_info = info.clone();
                let tx_hash_sender = hash_tx.clone();
                tokio::spawn(async move {
                    if let Some(txh) = handle.transactions_handle().await {
                        let peer_id = session_info.peer_id;
                        match txh.get_peer_transaction_hashes(peer_id).await {
                            Ok(set) => {
                                let mut hashes: Vec<_> = set.into_iter().collect();
                                let take_n = hashes.len().min(128);
                                hashes.truncate(take_n);
                                let sample: Vec<String> = hashes.iter().take(8).map(|h| format!("0x{:x}", h)).collect();
                                persist_mempool_sample(&format!("{}", peer_id), &sample, 0);
                                let _ = tx_hash_sender.send((peer_id, hashes));
                            }
                            Err(e) => {
                                info!(peer = %session_info.peer_id, error = %e, "mempool: failed to get peer tx hashes");
                            }
                        }
                    }
                });
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
