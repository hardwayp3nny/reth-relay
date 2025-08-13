use reth_discv4::Discv4ConfigBuilder;
use reth_ethereum::network::{
    api::events::SessionInfo, config::NetworkMode, NetworkConfig, NetworkEvent,
    NetworkEventListenerProvider, NetworkManager,
};
use reth_tracing::{tracing::{info, debug}, tracing_subscriber::filter::LevelFilter, LayerInfo, LogFormat, RethTracer, Tracer};
use secp256k1::SecretKey;
use rand::RngCore;
use std::{net::{Ipv4Addr, SocketAddr}, time::Duration};
use tokio_stream::StreamExt;

mod polygon_cfg;

#[tokio::main]
async fn main() {
    let _ = RethTracer::new()
        .with_stdout(LayerInfo::new(LogFormat::Terminal, LevelFilter::TRACE.to_string(), "".to_string(), Some("always".to_string())))
        .init();

    let secret_key = {
        // construct SecretKey from random 32 bytes to avoid version mismatch
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        SecretKey::from_slice(&bytes).expect("valid secret key")
    };
    let local_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 30303);

    let net_cfg = NetworkConfig::builder(secret_key)
        .set_head(polygon_cfg::head())
        .network_mode(NetworkMode::Work)
        .listener_addr(local_addr)
        .build_with_noop_provider(polygon_cfg::polygon_chain_spec());

    let mut discv4_cfg = Discv4ConfigBuilder::default();
    let interval = Duration::from_secs(1);
    let boot_nodes = polygon_cfg::boot_nodes();
    info!(count = boot_nodes.len(), "Adding bootnodes");
    for (i, n) in boot_nodes.iter().enumerate() { debug!(index = i, ?n, "bootnode"); }
    // Add fork id pair to improve compatibility in discv4 (EIP-868)
    use reth_chainspec::{ForkHash, ForkId};
    let fork_id = polygon_cfg::polygon_chain_spec().fork_id(&polygon_cfg::head());
    discv4_cfg
        .add_boot_nodes(boot_nodes)
        .lookup_interval(interval)
        .add_eip868_pair("eth", fork_id);
    let net_cfg = net_cfg.set_discovery_v4(discv4_cfg.build());

    let net_manager = NetworkManager::eth(net_cfg).await.unwrap();
    let net_handle = net_manager.handle();
    let mut events = net_handle.event_listener();

    tokio::spawn(net_manager);
    info!("Looking for Polygon peers (detailed TRACE logging enabled)...");

    while let Some(evt) = events.next().await {
        // Log all network events for detailed peer discovery diagnostics
        info!(target: "reth_network_events", event = ?evt);
        if let NetworkEvent::ActivePeerSession { info, .. } = evt {
            let SessionInfo { status, client_version, .. } = info;
            let chain = status.chain;
            info!(?chain, ?client_version, "Session established with a new peer.");
        }
    }
}
