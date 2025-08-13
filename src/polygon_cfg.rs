use reth_discv4::NodeRecord;
use alloy_primitives::{b256, B256};
use reth_chainspec::{BaseFeeParams, BaseFeeParamsKind, ChainSpec, ChainSpecBuilder, Head};
use reth_ethereum_forks::{EthereumHardfork, ForkCondition};
use reth_primitives_traits::SealedHeader;
use std::sync::Arc;
use std::env;

const SHANGHAI_BLOCK: u64 = 50523000;
const POLYGON_CHAIN_ID: u64 = 137;
const POLYGON_GENESIS_HASH: B256 = b256!("a9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b");

pub(crate) fn polygon_chain_spec() -> Arc<ChainSpec> {
    // Build ChainSpec like the article: explicit genesis hash + hardfork schedule + base fee params
    // Use default header shape but override hash explicitly
    let sealed = SealedHeader::new(Default::default(), POLYGON_GENESIS_HASH);

    let mut builder = ChainSpecBuilder::default();
    builder = builder
        .chain(POLYGON_CHAIN_ID.into())
        // Load the real Polygon genesis JSON for completeness
        .genesis(serde_json::from_str(include_str!("./polygon_genesis.json")).expect("parse genesis"));

    // Configure Polygon hardforks per Merkle article schedule
    builder = builder
        .with_fork(EthereumHardfork::Petersburg, ForkCondition::Block(0))
        .with_fork(EthereumHardfork::Istanbul, ForkCondition::Block(3395000))
        .with_fork(EthereumHardfork::MuirGlacier, ForkCondition::Block(3395000))
        .with_fork(EthereumHardfork::Berlin, ForkCondition::Block(14750000))
        .with_fork(EthereumHardfork::London, ForkCondition::Block(23850000))
        .with_fork(EthereumHardfork::Shanghai, ForkCondition::Block(SHANGHAI_BLOCK));

    let mut spec = builder.build();
    // Override genesis header hash to the known Polygon genesis
    spec.genesis_header = sealed;
    spec.base_fee_params = polygon_base_fee_params();

    Arc::new(spec)
}

/// Polygon mainnet boot nodes
static BOOTNODES: [&str; 12] = [
    "enode://b8f1cc9c5d4403703fbf377116469667d2b1823c0daf16b7250aa576bacf399e42c3930ccfcb02c5df6879565a2b8931335565f0e8d3f8e72385ecf4a4bf160a@3.36.224.80:30303",
    "enode://8729e0c825f3d9cad382555f3e46dcff21af323e89025a0e6312df541f4a9e73abfa562d64906f5e59c51fe6f0501b3e61b07979606c56329c020ed739910759@54.194.245.5:30303",
    "enode://76316d1cb93c8ed407d3332d595233401250d48f8fbb1d9c65bd18c0495eca1b43ec38ee0ea1c257c0abb7d1f25d649d359cdfe5a805842159cfe36c5f66b7e8@52.78.36.216:30303",
    "enode://681ebac58d8dd2d8a6eef15329dfbad0ab960561524cf2dfde40ad646736fe5c244020f20b87e7c1520820bc625cfb487dd71d63a3a3bf0baea2dbb8ec7c79f1@34.240.245.39:30303",
    "enode://d860a01f9722d78051619d1e2351aba3f43f943f6f00718d1b9baa4101932a1f5011f16bb2b1bb35db20d6fe28fa0bf09636d26a87d31de9ec6203eeedb1f666@18.138.108.67:30303",   // bootnode-aws-ap-southeast-1-001
	"enode://22a8232c3abc76a16ae9d6c3b164f98775fe226f0917b0ca871128a74a8e9630b458460865bab457221f1d448dd9791d24c4e5d88786180ac185df813a68d4de@3.209.45.79:30303",     // bootnode-aws-us-east-1-001
	"enode://8499da03c47d637b20eee24eec3c356c9a2e6148d6fe25ca195c7949ab8ec2c03e3556126b0d7ed644675e78c4318b08691b7b57de10e5f0d40d05b09238fa0a@52.187.207.27:30303",   // bootnode-azure-australiaeast-001
	"enode://103858bdb88756c71f15e9b5e09b56dc1be52f0a5021d46301dbbfb7e130029cc9d0d6f73f693bc29b665770fff7da4d34f3c6379fe12721b5d7a0bcb5ca1fc1@191.234.162.198:30303", // bootnode-azure-brazilsouth-001
	"enode://715171f50508aba88aecd1250af392a45a330af91d7b90701c436b618c86aaa1589c9184561907bebbb56439b8f8787bc01f49a7c77276c58c1b09822d75e8e8@52.231.165.108:30303",  // bootnode-azure-koreasouth-001
	"enode://5d6d7cd20d6da4bb83a1d28cadb5d409b64edf314c0335df658c1a54e32c7c4a7ab7823d57c39b6a757556e68ff1df17c748b698544a55cb488b52479a92b60f@104.42.217.25:30303",   // bootnode-azure-westus-001
	"enode://2b252ab6a1d0f971d9722cb839a42cb81db019ba44c08754628ab4a823487071b5695317c8ccd085219c3a03af063495b2f1da8d18218da2d6a82981b45e6ffc@65.108.70.101:30303",   // bootnode-hetzner-hel
	"enode://4aeb4ab6c14b23e2c4cfdce879c04b0748a20d8e9b59e25ded2a08143e265c6c25936e74cbc8e641e3312ca288673d91f2f93f8e277de3cfa444ecdaaf982052@157.90.35.166:30303",   // bootnode-hetzner-fsn

];

pub(crate) fn head() -> Head {
    let number = env::var("POLY_HEAD_BLOCK")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(SHANGHAI_BLOCK);
    Head { number, ..Default::default() }
}

pub(crate) fn boot_nodes() -> Vec<NodeRecord> {
    BOOTNODES[..].iter().map(|s| s.parse().unwrap()).collect()
}


fn polygon_base_fee_params() -> BaseFeeParamsKind {
    // Align with intent of BaseFeeParams::polygon() from the article.
    // Keep constants we used previously to avoid behavior change.
    BaseFeeParamsKind::Constant(BaseFeeParams::new(70, 60))
}


#[cfg(test)]
mod tests {
    use super::{head, polygon_chain_spec};

    #[test]
    fn print_polygon_forkid() {
        let fork_id = polygon_chain_spec().fork_id(&head());
        let h = fork_id.hash.0;
        println!(
            "polygon forkid = 0x{:02x}{:02x}{:02x}{:02x}, next = {}",
            h[0], h[1], h[2], h[3], fork_id.next
        );
    }

    #[test]
    fn emit_polygon_forkids() {
        let spec = polygon_chain_spec();
        let sample_heads = [
            0u64,
            3394999,
            3395000,
            14749999,
            14750000,
            23849999,
            23850000,
            super::SHANGHAI_BLOCK,
        ];
        for n in sample_heads { 
            let h = reth_chainspec::Head { number: n, ..Default::default() };
            let fid = spec.fork_id(&h);
            let b = fid.hash.0;
            println!(
                "head={n:>8} forkid=0x{:02x}{:02x}{:02x}{:02x} next={}",
                b[0], b[1], b[2], b[3], fid.next
            );
        }
    }
}
