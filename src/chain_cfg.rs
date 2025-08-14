use alloy_genesis::Genesis;
use alloy_primitives::BlockHash;
use reth_chainspec::{make_genesis_header, Chain, EthereumHardfork, ForkCondition, ChainHardforks, Hardfork};
use reth_primitives::SealedHeader;
use std::str::FromStr;
use reth_discv4::NodeRecord;
use reth_ethereum::chainspec::{ChainSpec, Head};
use reth_tracing::tracing::{debug, info, warn};

use std::sync::Arc;

// Polygon 主要分叉高度（来自本地 genesis.json 最新配置）
const ISTANBUL_BLOCK: u64 = 3_395_000;
const MUIR_GLACIER_BLOCK: u64 = 3_395_000;
const BERLIN_BLOCK: u64 = 14_750_000;
const LONDON_BLOCK: u64 = 23_850_000;
const SHANGHAI_BLOCK: u64 = 50_523_000;
const CANCUN_BLOCK: u64 = 54_876_000;
const PRAGUE_BLOCK: u64 = 73_440_256;

// Bor 分叉（供参考，当前实现未直接使用这些常量）
const JAIPUR_BLOCK: u64 = 23_850_000;
const DELHI_BLOCK: u64 = 38_189_056;
const INDORE_BLOCK: u64 = 44_934_656;
const AHMEDABAD_BLOCK: u64 = 62_278_656;
const BHILAI_BLOCK: u64 = 73_440_256;

// 限制 bootnodes 数量，避免列表过大
const MAX_BOOTNODES: usize = 512;

pub(crate) fn polygon_chain_spec() -> Arc<ChainSpec> {
    let genesis: Genesis =
        serde_json::from_str(include_str!("./genesis.json")).expect("deserialize genesis");
    debug!("已加载 Polygon genesis.json 并构建 ChainSpec");
    // 显式 hardforks（按块高触发）
    let hardforks = ChainHardforks::new(vec![
        (EthereumHardfork::Frontier.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Homestead.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Tangerine.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::SpuriousDragon.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Byzantium.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Constantinople.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Petersburg.boxed(), ForkCondition::Block(0)),
        (EthereumHardfork::Istanbul.boxed(), ForkCondition::Block(ISTANBUL_BLOCK)),
        (EthereumHardfork::MuirGlacier.boxed(), ForkCondition::Block(MUIR_GLACIER_BLOCK)),
        (EthereumHardfork::Berlin.boxed(), ForkCondition::Block(BERLIN_BLOCK)),
        (EthereumHardfork::London.boxed(), ForkCondition::Block(LONDON_BLOCK)),
        (EthereumHardfork::Shanghai.boxed(), ForkCondition::Block(SHANGHAI_BLOCK)),
        (EthereumHardfork::Cancun.boxed(), ForkCondition::Block(CANCUN_BLOCK)),
        (EthereumHardfork::Prague.boxed(), ForkCondition::Block(PRAGUE_BLOCK)),
    ]);

    let mut spec: ChainSpec = genesis.clone().into();
    spec.chain = Chain::from_id(137);
    spec.hardforks = hardforks;
    let header = make_genesis_header(&genesis, &spec.hardforks);
    let hash = BlockHash::from_str(
        "0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b",
    )
    .expect("valid polygon genesis hash");
    spec.genesis_header = SealedHeader::new(header, hash);
    Arc::new(spec)
}

/// Polygon mainnet boot nodes <https://github.com/maticnetwork/bor/blob/master/params/bootnodes.go#L79>
static BOOTNODES: [&str; 4] = [
    "enode://b8f1cc9c5d4403703fbf377116469667d2b1823c0daf16b7250aa576bacf399e42c3930ccfcb02c5df6879565a2b8931335565f0e8d3f8e72385ecf4a4bf160a@3.36.224.80:30303",
    "enode://8729e0c825f3d9cad382555f3e46dcff21af323e89025a0e6312df541f4a9e73abfa562d64906f5e59c51fe6f0501b3e61b07979606c56329c020ed739910759@54.194.245.5:30303",
    "enode://76316d1cb93c8ed407d3332d595233401250d48f8fbb1d9c65bd18c0495eca1b43ec38ee0ea1c257c0abb7d1f25d649d359cdfe5a805842159cfe36c5f66b7e8@52.78.36.216:30303",
    "enode://681ebac58d8dd2d8a6eef15329dfbad0ab960561524cf2dfde40ad646736fe5c244020f20b87e7c1520820bc625cfb487dd71d63a3a3bf0baea2dbb8ec7c79f1@34.240.245.39:30303",
];

pub(crate) fn head() -> Head {
    // 使用最新 Prague 高度作为默认 Head
    let h = Head { number: PRAGUE_BLOCK, ..Default::default() };
    debug!(target: "polygon", number = h.number, "返回默认 Head（Prague 高度）");
    h
}

pub(crate) fn boot_nodes() -> Vec<NodeRecord> {
    // 从 CSV 加载 Polygon (bor) 节点，并与内置默认节点合并去重
    let csv_path: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/export-nodestrackerlist.csv");

    let mut enode_strings: Vec<String> = Vec::new();

    if let Ok(contents) = std::fs::read_to_string(csv_path) {
        info!(target: "polygon", path = csv_path, "从 CSV 加载候选 bootnodes");
        let mut parsed_lines: usize = 0;
        for line in contents.lines().skip(1) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            // 需要至少: Node Id, Last Seen, Host, Port, Country, Client
            if parts.len() < 6 {
                warn!(target: "polygon", "CSV 行字段不足，已跳过: {line}");
                continue;
            }
            let client = parts[5].trim_matches('"').to_ascii_lowercase();
            // 仅筛选 Polygon 的 bor 客户端，避免混入其他链（例如 Ethereum、Ronin 等）
            if !client.contains("bor/") {
                continue;
            }
            let node_id = parts[0].trim_matches('"').trim();
            let host = parts[2].trim_matches('"').trim();
            let port = parts[3].trim_matches('"').trim();
            if node_id.is_empty() || host.is_empty() || port.is_empty() {
                warn!(target: "polygon", "CSV 行存在空字段，已跳过: {line}");
                continue;
            }
            enode_strings.push(format!("enode://{}@{}:{}", node_id, host, port));
            parsed_lines += 1;
        }
        info!(target: "polygon", count = parsed_lines, total = enode_strings.len(), "CSV 解析完成");
    } else {
        warn!(target: "polygon", path = csv_path, "未找到 CSV 或读取失败，将仅使用内置 bootnodes 兜底");
    }

    // 合并内置 bootnodes 作为兜底
    enode_strings.extend(BOOTNODES.iter().map(|s| s.to_string()));

    // 去重并限制数量，避免过大列表影响启动
    enode_strings.sort();
    enode_strings.dedup();
    let total_after_dedup = enode_strings.len();
    if total_after_dedup > MAX_BOOTNODES {
        warn!(target: "polygon", total_after_dedup, max = MAX_BOOTNODES, "bootnodes 数量过多，将截断");
    }
    enode_strings.truncate(MAX_BOOTNODES);

    let mut nodes: Vec<NodeRecord> = Vec::with_capacity(enode_strings.len());
    for s in enode_strings.into_iter() {
        match s.parse::<NodeRecord>() {
            Ok(n) => nodes.push(n),
            Err(e) => warn!(target: "polygon", enode = s, error = %e, "解析 NodeRecord 失败，已跳过"),
        }
    }
    info!(target: "polygon", total = nodes.len(), "最终可用 bootnodes 数量");
    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_polygon_fork_hashes() {
        let spec = polygon_chain_spec();

        // 选取关键分叉附近的高度（前一块、分叉块、本分叉后一块）
        let points: [(&str, u64); 15] = [
            ("genesis", 0),
            ("istanbul-1", ISTANBUL_BLOCK.saturating_sub(1)),
            ("istanbul", ISTANBUL_BLOCK),
            ("istanbul+1", ISTANBUL_BLOCK + 1),
            ("berlin", BERLIN_BLOCK),
            ("berlin+1", BERLIN_BLOCK + 1),
            ("london", LONDON_BLOCK),
            ("london+1", LONDON_BLOCK + 1),
            ("shanghai", SHANGHAI_BLOCK),
            ("shanghai+1", SHANGHAI_BLOCK + 1),
            ("cancun", CANCUN_BLOCK),
            ("cancun+1", CANCUN_BLOCK + 1),
            ("prague-1", PRAGUE_BLOCK.saturating_sub(1)),
            ("prague", PRAGUE_BLOCK),
            ("prague+1", PRAGUE_BLOCK + 1),
        ];

        for (label, number) in points {            
            let head = Head { number, ..Default::default() };
            let fid = spec.fork_id(&head);
            // 使用 print，建议以 -- --nocapture 运行查看
            println!("{:12} | block={} | fork_hash={:?} | next={}", label, number, fid.hash, fid.next);
        }
    }
}
