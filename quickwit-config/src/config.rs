// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Context};
use byte_unit::Byte;
use derivative::Derivative;
use json_comments::StripComments;
use once_cell::sync::OnceCell;
use quickwit_common::net::{find_private_ip, Host, HostAddr};
use quickwit_common::new_coolid;
use quickwit_common::uri::{Extension, Uri};
use quickwit_config_macros::{qw_default, qw_env_var};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config_value::{ConfigValue, ConfigValueBuilder, ConfigValueSource, NoQwEnvVar};
use crate::templating::render_config;
use crate::validate_identifier;

pub const DEFAULT_QW_CONFIG_PATH: &str = "./config/quickwit.yaml";

const DEFAULT_DATA_DIR_PATH: &str = "./qwdata";

const DEFAULT_CLUSTER_ID: &str = "quickwit-default-cluster";

type Cvb<T, Q> = ConfigValueBuilder<T, Q>;

#[qw_env_var]
struct QwDataDir;

fn default_data_dir_path() -> Cvb<PathBuf, QwDataDir> {
    ConfigValueBuilder {
        qw_default: Some(PathBuf::from(DEFAULT_DATA_DIR_PATH)),
        ..Default::default()
    }
}

// Surprisingly, the default metastore and the index root uri are the same (if you exclude the
// polling_interval parameter). Indeed, this is a convenient setting for testing with a file backed
// metastore and indexes splits stored locally too.
// For a given index `index-id`, it means that we have the metastore file
// in  `./qwdata/indexes/{index-id}/metastore.json` and splits in
// dir `./qwdata/indexes/{index-id}/splits`.
fn default_metastore_uri(data_dir_path: &Path) -> Uri {
    Uri::try_new(&data_dir_path.join("indexes#polling_interval=30s").to_string_lossy())
        .expect("Failed to create default metastore URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

// See comment above.
fn default_index_root_uri(data_dir_path: &Path) -> Uri {
    Uri::try_new(&data_dir_path.join("indexes").to_string_lossy())
        .expect("Failed to create default index_root URI. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
}

#[qw_env_var]
struct QwClusterId;

fn default_cluster_id() -> Cvb<String, QwClusterId> {
    ConfigValueBuilder {
        qw_default: Some(DEFAULT_CLUSTER_ID.to_string()),
        ..Default::default()
    }
}

#[qw_env_var]
struct QwNodeId;

fn default_node_id() -> Cvb<String, QwNodeId> {
    ConfigValueBuilder {
        qw_default: Some(new_coolid("node")),
        ..Default::default()
    }
}

#[qw_env_var]
struct QwListenAddress;

fn default_listen_address() -> Cvb<String, QwListenAddress> {
    ConfigValueBuilder {
        qw_default: Some(Host::default().to_string()),
        ..Default::default()
    }
}

#[qw_env_var]
struct QwRestListenPort;

fn default_rest_listen_port() -> Cvb<u16, QwRestListenPort> {
    ConfigValueBuilder {
        qw_default: Some(7280),
        ..Default::default()
    }
}

#[qw_env_var]
struct QwAdvertiseAddress;

#[qw_env_var]
struct QwGossipListenPort;

#[qw_env_var]
struct QwGrpcListenPort;

#[qw_env_var]
struct QwPeerSeeds;

#[qw_env_var]
struct QwMetastoreUri;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[serde(default = "IndexerConfig::default_split_store_max_num_bytes")]
    pub split_store_max_num_bytes: Byte,
    #[serde(default = "IndexerConfig::default_split_store_max_num_splits")]
    pub split_store_max_num_splits: usize,
}

impl IndexerConfig {
    fn default_split_store_max_num_bytes() -> Byte {
        Byte::from_bytes(100_000_000_000) // 100G
    }

    fn default_split_store_max_num_splits() -> usize {
        1_000
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> anyhow::Result<Self> {
        let indexer_config = IndexerConfig {
            split_store_max_num_bytes: Byte::from_bytes(1_000_000),
            split_store_max_num_splits: 3,
        };
        Ok(indexer_config)
    }
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            split_store_max_num_bytes: Self::default_split_store_max_num_bytes(),
            split_store_max_num_splits: Self::default_split_store_max_num_splits(),
        }
    }
}

pub static SEARCHER_CONFIG_INSTANCE: once_cell::sync::OnceCell<SearcherConfig> = OnceCell::new();

pub fn get_searcher_config_instance() -> &'static SearcherConfig {
    SEARCHER_CONFIG_INSTANCE.get_or_init(SearcherConfig::default)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearcherConfig {
    #[serde(default = "SearcherConfig::default_fast_field_cache_capacity")]
    pub fast_field_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_split_footer_cache_capacity")]
    pub split_footer_cache_capacity: Byte,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_searches")]
    pub max_num_concurrent_split_searches: usize,
    #[serde(default = "SearcherConfig::default_max_num_concurrent_split_streams")]
    pub max_num_concurrent_split_streams: usize,
}

impl SearcherConfig {
    fn default_fast_field_cache_capacity() -> Byte {
        Byte::from_bytes(1_000_000_000) // 1G
    }

    fn default_split_footer_cache_capacity() -> Byte {
        Byte::from_bytes(500_000_000) // 500M
    }

    fn default_max_num_concurrent_split_searches() -> usize {
        100
    }

    fn default_max_num_concurrent_split_streams() -> usize {
        100
    }
}

impl Default for SearcherConfig {
    fn default() -> Self {
        Self {
            fast_field_cache_capacity: Self::default_fast_field_cache_capacity(),
            split_footer_cache_capacity: Self::default_split_footer_cache_capacity(),
            max_num_concurrent_split_streams: Self::default_max_num_concurrent_split_streams(),
            max_num_concurrent_split_searches: Self::default_max_num_concurrent_split_searches(),
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct PeerSeeds(Vec<String>);

impl FromStr for PeerSeeds {
    type Err = anyhow::Error;

    fn from_str(peer_seeds_str: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            peer_seeds_str
                .split(',')
                .filter_map(|peer_seed| match peer_seed.trim() {
                    "" => None,
                    ps => Some(ps.to_string()),
                })
                .collect(),
        ))
    }
}

// #[ConfigBuilder]
#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct QuickwitConfigBuilder {
    version: Cvb<usize, NoQwEnvVar>,
    #[serde(default = "default_cluster_id")]
    cluster_id: Cvb<String, QwClusterId>,
    #[serde(default = "default_node_id")]
    node_id: Cvb<String, QwNodeId>,
    #[serde(default = "default_listen_address")]
    listen_address: Cvb<String, QwListenAddress>,
    advertise_address: Option<Cvb<String, QwAdvertiseAddress>>,
    #[serde(default = "default_rest_listen_port")]
    rest_listen_port: Cvb<u16, QwRestListenPort>,
    gossip_listen_port: Option<Cvb<u16, QwGossipListenPort>>,
    grpc_listen_port: Option<Cvb<u16, QwGrpcListenPort>>,
    #[serde(default)]
    peer_seeds: Cvb<PeerSeeds, QwPeerSeeds>,
    #[serde(default)]
    metastore_uri: Option<Cvb<String, QwMetastoreUri>>,
    #[serde(default)]
    default_index_root_uri: Option<Cvb<String, NoQwEnvVar>>,
    #[serde(default = "default_data_dir_path")]
    #[serde(rename = "data_dir")]
    data_dir_path: Cvb<PathBuf, QwDataDir>,
    #[serde(rename = "indexer")]
    #[serde(default)]
    indexer_config: IndexerConfig,
    #[serde(rename = "searcher")]
    #[serde(default)]
    searcher_config: SearcherConfig,
}

impl QuickwitConfigBuilder {
    async fn from_uri(uri: &Uri, config_content: &[u8]) -> anyhow::Result<Self> {
        let parser_fn = match uri.extension() {
            Some(Extension::Json) => Self::from_json,
            Some(Extension::Toml) => Self::from_toml,
            Some(Extension::Yaml) => Self::from_yaml,
            Some(Extension::Unknown(extension)) => bail!(
                "Failed to read quickwit config file `{}`: file extension `.{}` is not supported. \
                 Supported file formats and extensions are JSON (.json), TOML (.toml), and YAML \
                 (.yaml or .yml).",
                uri,
                extension
            ),
            None => bail!(
                "Failed to read config file `{}`: file extension is missing. Supported file \
                 formats and extensions are JSON (.json), TOML (.toml), and YAML (.yaml or .yml).",
                uri
            ),
        };
        let rendered_config = render_config(uri, config_content)?;
        parser_fn(rendered_config.as_bytes())
    }

    fn from_json(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_reader(StripComments::new(bytes))
            .context("Failed to parse JSON config file.")
    }

    fn from_toml(bytes: &[u8]) -> anyhow::Result<Self> {
        toml::from_slice(bytes).context("Failed to parse TOML config file.")
    }

    fn from_yaml(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_yaml::from_slice(bytes).context("Failed to parse YAML config file.")
    }

    pub async fn build(self) -> anyhow::Result<QuickwitConfig> {
        let env_vars: HashMap<String, String> = env::vars().collect();

        let listen_address = self.listen_address.build(&env_vars)?;
        let listen_address = ConfigValue {
            value: listen_address.parse::<Host>()?.resolve().await?,
            source: listen_address.source,
        };
        let advertise_address = listen_address.clone(); // FIXME

        let rest_listen_port = self.rest_listen_port.build(&env_vars)?;

        let gossip_listen_port = self
            .gossip_listen_port
            .map(|cvb| cvb.build(&env_vars))
            .transpose()?
            .unwrap_or_else(|| rest_listen_port.clone());

        let grpc_listen_port = self
            .grpc_listen_port
            .map(|cvb| cvb.build(&env_vars))
            .transpose()?
            .unwrap_or_else(|| rest_listen_port.clone().map(|port| port + 1));

        let peer_seeds = self
            .peer_seeds
            .build(&env_vars)?
            .map(|peer_seeds| peer_seeds.0);

        let data_dir_path = self.data_dir_path.build(&env_vars)?;

        let metastore_uri = if let Some(cvb) = self.metastore_uri {
            cvb.build(&env_vars)?.try_map(|uri| {
                Uri::try_new(&uri)
                    .with_context(|| format!("Failed to parse metastore URI `{uri}`."))
            })?
        } else {
            ConfigValue {
                value: default_metastore_uri(&*data_dir_path),
                source: ConfigValueSource::QuickwitDefault,
            }
        };

        let default_index_root_uri = if let Some(cvb) = self.default_index_root_uri {
            cvb.build(&env_vars)?.try_map(|uri| {
                Uri::try_new(&uri)
                    .with_context(|| format!("Failed to parse default index root URI `{uri}`."))
            })?
        } else {
            ConfigValue {
                value: default_index_root_uri(&*data_dir_path),
                source: ConfigValueSource::QuickwitDefault,
            }
        };

        Ok(QuickwitConfig {
            version: self.version.build(&env_vars)?,
            cluster_id: self.cluster_id.build(&env_vars)?,
            node_id: self.node_id.build(&env_vars)?,
            listen_address,
            advertise_address,
            rest_listen_port,
            gossip_listen_port,
            grpc_listen_port,
            peer_seeds,
            metastore_uri,
            default_index_root_uri,
            data_dir_path,
            indexer_config: self.indexer_config,
            searcher_config: self.searcher_config,
        })
    }
}

// fn redact_uri(
//     uri_opt: &Option<ConfigValueBuilder<String>>,
//     formatter: &mut std::fmt::Formatter,
// ) -> Result<(), std::fmt::Error> {
//     match uri_opt.as_ref().map(|cvb| Uri::try_new(&cvb.value)) {
//         Some(Ok(uri)) => {
//             formatter.write_str("Some(")?;
//             formatter.write_str(&uri.as_redacted_str())?;
//             formatter.write_str(")")?;
//         }
//         Some(Err(_)) => formatter.write_str("Some(***redacted***)")?,
//         None => formatter.write_str("None")?,
//     };
//     Ok(())
// }

#[derive(Clone, Debug, Serialize)]
pub struct QuickwitConfig {
    pub version: ConfigValue<usize>,
    pub cluster_id: ConfigValue<String>,
    pub node_id: ConfigValue<String>,
    pub listen_address: ConfigValue<IpAddr>,
    pub advertise_address: ConfigValue<IpAddr>,
    pub rest_listen_port: ConfigValue<u16>,
    pub gossip_listen_port: ConfigValue<u16>,
    pub grpc_listen_port: ConfigValue<u16>,
    pub peer_seeds: ConfigValue<Vec<String>>,
    pub metastore_uri: ConfigValue<Uri>,
    pub default_index_root_uri: ConfigValue<Uri>,
    pub data_dir_path: ConfigValue<PathBuf>,
    pub indexer_config: IndexerConfig,
    pub searcher_config: SearcherConfig,
}

impl QuickwitConfig {
    /// Parses and validates a [`QuickwitConfig`] from a given URI and config content.
    pub async fn load(
        uri: &Uri,
        config_content: &[u8],
        data_dir_path_opt: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        let mut config_builder = QuickwitConfigBuilder::from_uri(uri, config_content).await?;
        // if let Some(data_dir_path) = data_dir_path_opt {
        //     info!(
        //         data_dir_path = %data_dir_path.display(),
        //         "Setting data dir path from CLI args or environment variable",
        //     );
        //     config_builder.data_dir_path = data_dir_path;
        // }
        let config = config_builder.build().await?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        validate_identifier("Cluster ID", &self.cluster_id)?;
        validate_identifier("Node ID", &self.node_id)?;

        if self.cluster_id == DEFAULT_CLUSTER_ID {
            warn!(
                cluster_id=%DEFAULT_CLUSTER_ID,
                "Cluster ID is not set, falling back to default value."
            );
        }
        if self.peer_seeds.is_empty() {
            warn!("Peer seed list is empty.");
        }
        let data_dir_uri = Uri::try_new(&self.data_dir_path.to_string_lossy())?;

        if !data_dir_uri.protocol().is_file() {
            bail!(
                "Data dir must be located on local file system. Current location: \
                 `{data_dir_uri}`."
            )
        }
        if !self.data_dir_path.exists() {
            bail!(
                "Data dir `{}` does not exist.",
                self.data_dir_path.display()
            );
        }
        Ok(())
    }

    pub fn rest_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(*self.listen_address, *self.rest_listen_port)
    }

    pub fn gossip_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(*self.listen_address, *self.gossip_listen_port)
    }

    pub fn grpc_listen_addr(&self) -> SocketAddr {
        SocketAddr::new(*self.listen_address, *self.grpc_listen_port)
    }

    pub fn gossip_advertise_addr(&self) -> SocketAddr {
        SocketAddr::new(*self.advertise_address, *self.gossip_listen_port)
    }

    pub fn grpc_advertise_addr(&self) -> SocketAddr {
        SocketAddr::new(*self.advertise_address, *self.grpc_listen_port)
    }

    /// Returns the list of peer seed addresses. The addresses MUST NOT be resolved. Otherwise, the
    /// DNS-based discovery mechanism implemented in Chitchat will not work correctly.
    pub async fn peer_seed_addrs(&self) -> anyhow::Result<Vec<String>> {
        let mut peer_seed_addrs = Vec::new();

        // We want to pass non-resolved addresses to Chitchat but still want to resolve them for
        // validation purposes. Additionally, we need to append a default port if necessary and
        // finally return the addresses as strings, which is tricky for IPv6. We let the logic baked
        // in `HostAddr` handle this complexity.
        for peer_seed in &*self.peer_seeds {
            let peer_seed_addr =
                HostAddr::parse_with_default_port(peer_seed, *self.gossip_listen_port)?;
            if let Err(error) = peer_seed_addr.resolve().await {
                warn!(peer_seed = %peer_seed_addr, error = ?error, "Failed to resolve peer seed address.");
                continue;
            }
            peer_seed_addrs.push(peer_seed_addr.to_string())
        }
        if !self.peer_seeds.is_empty() && peer_seed_addrs.is_empty() {
            bail!(
                "Failed to resolve any of the peer seed addresses: `{}`",
                self.peer_seeds.join(", ")
            )
        }
        Ok(peer_seed_addrs)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test() -> Self {
        let version = ConfigValue {
            value: 0,
            source: ConfigValueSource::QuickwitDefault,
        };
        use std::net::Ipv4Addr;
        // let listen_address = cv_provided!(Host::default().to_ip_address());
        // let advertise_address = listen_address.clone();
        let listen_address = ConfigValue {
            value: IpAddr::V4(Ipv4Addr::LOCALHOST),
            source: ConfigValueSource::QuickwitDefault,
        };
        let advertise_address = ConfigValue {
            value: IpAddr::V4(Ipv4Addr::LOCALHOST),
            source: ConfigValueSource::QuickwitDefault,
        };
        // let rest_listen_port = cv_provided!(quickwit_common::net::find_available_tcp_port()
        //     .expect("The OS should almost always find an available port."));
        // let gossip_listen_port = rest_listen_port.clone();
        // let grpc_listen_port = cv_provided!(quickwit_common::net::find_available_tcp_port()
        //     .expect("The OS should almost always find an available port."));
        let rest_listen_port = ConfigValue {
            value: 12012,
            source: ConfigValueSource::Provided,
        };
        let gossip_listen_port = ConfigValue {
            value: 12012,
            source: ConfigValueSource::Provided,
        };
        let grpc_listen_port = ConfigValue {
            value: 12012,
            source: ConfigValueSource::Provided,
        };
        let peer_seeds = ConfigValue {
            value: Vec::new(),
            source: ConfigValueSource::QuickwitDefault,
        };
        let data_dir_path = default_data_dir_path().unwrap();
        let metastore_uri = ConfigValue {
            value: default_metastore_uri(&*data_dir_path),
            source: ConfigValueSource::QuickwitDefault,
        };
        let default_index_root_uri = ConfigValue {
            value: default_index_root_uri(&*data_dir_path),
            source: ConfigValueSource::QuickwitDefault,
        };
        Self {
            version,
            cluster_id: default_cluster_id().unwrap(),
            node_id: default_node_id().unwrap(),
            listen_address,
            advertise_address,
            rest_listen_port,
            gossip_listen_port,
            grpc_listen_port,
            peer_seeds,
            metastore_uri,
            default_index_root_uri,
            data_dir_path,
            indexer_config: IndexerConfig::default(),
            searcher_config: SearcherConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    macro_rules! cvb_for_test {
        ($e:expr) => {
            crate::config_value::ConfigValueBuilder {
                provided: Some($e),
                ..Default::default()
            }
        };
    }

    impl Default for QuickwitConfigBuilder {
        fn default() -> Self {
            let version = ConfigValueBuilder {
                qw_default: Some(0),
                ..Default::default()
            };
            let peer_seeds = ConfigValueBuilder {
                qw_default: Some(PeerSeeds::default()),
                ..Default::default()
            };
            Self {
                version,
                cluster_id: default_cluster_id(),
                node_id: default_node_id(),
                listen_address: default_listen_address(),
                advertise_address: None,
                rest_listen_port: default_rest_listen_port(),
                gossip_listen_port: None,
                grpc_listen_port: None,
                peer_seeds,
                metastore_uri: None,
                default_index_root_uri: None,
                data_dir_path: default_data_dir_path(),
                indexer_config: IndexerConfig::default(),
                searcher_config: SearcherConfig::default(),
            }
        }
    }

    fn get_config_filepath(config_filename: &str) -> String {
        format!(
            "{}/resources/tests/config/{}",
            env!("CARGO_MANIFEST_DIR"),
            config_filename
        )
    }

    macro_rules! test_parser {
        ($test_function_name:ident, $file_extension:expr) => {
            #[tokio::test]
            async fn $test_function_name() -> anyhow::Result<()> {
                let config_filepath =
                    get_config_filepath(&format!("quickwit.{}", stringify!($file_extension)));
                let config_uri = Uri::try_new(&config_filepath)?;
                let file = std::fs::read_to_string(&config_filepath).unwrap();
                let config = QuickwitConfigBuilder::from_uri(&config_uri, file.as_bytes())
                    .await?
                    .build()
                    .await?;
                assert_eq!(config.version, 0);
                assert_eq!(config.cluster_id, "quickwit-cluster");
                assert_eq!(
                    config.rest_listen_addr(),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1111)
                );
                assert_eq!(
                    config.peer_seeds,
                    vec![
                        "quickwit-searcher-0.local".to_string(),
                        "quickwit-searcher-1.local".to_string()
                    ]
                );
                assert_eq!(
                    config.metastore_uri,
                    "postgres://username:password@host:port/db"
                );

                assert_eq!(
                    config.indexer_config,
                    IndexerConfig {
                        split_store_max_num_bytes: Byte::from_str("1T").unwrap(),
                        split_store_max_num_splits: 10_000,
                    }
                );

                assert_eq!(
                    config.searcher_config,
                    SearcherConfig {
                        fast_field_cache_capacity: Byte::from_str("10G").unwrap(),
                        split_footer_cache_capacity: Byte::from_str("1G").unwrap(),
                        max_num_concurrent_split_searches: 150,
                        max_num_concurrent_split_streams: 120,
                    }
                );

                Ok(())
            }
        };
    }

    test_parser!(test_config_from_json, json);
    test_parser!(test_config_from_toml, toml);
    test_parser!(test_config_from_yaml, yaml);

    #[tokio::test]
    async fn test_config_contains_wrong_values() {
        let config_filepath = get_config_filepath("quickwit.wrongkey.yaml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let config_str = std::fs::read_to_string(&config_filepath).unwrap();
        // let parsing_error = QuickwitConfigBuilder::from_uri(&config_uri, config_str.as_bytes())
        //     .await
        //     .unwrap_err();
        // assert!(format!("{parsing_error:?}")
        //     .contains("unknown field `max_num_concurrent_split_searchs`"));
    }

    #[test]
    fn test_indexer_config_default_values() {
        let indexer_config = serde_yaml::from_str::<IndexerConfig>("{}").unwrap();
        assert_eq!(indexer_config, IndexerConfig::default());
    }

    #[test]
    fn test_searcher_config_default_values() {
        let searcher_config = serde_yaml::from_str::<SearcherConfig>("{}").unwrap();
        assert_eq!(searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_default_values_minimal() {
        let config_yaml = "version: 0";
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build().await.unwrap();
        assert_eq!(config.version, 0);
        assert!(config.node_id.starts_with("node-"));
        assert_eq!(
            config.metastore_uri,
            format!(
                "file://{}/qwdata/indexes#polling_interval=30s",
                env::current_dir().unwrap().display()
            )
        );
        assert_eq!(config.data_dir_path.to_string_lossy(), "./qwdata");
    }

    #[tokio::test]
    async fn test_quickwwit_config_default_values_storage() {
        let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build().await.unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(config.cluster_id, DEFAULT_CLUSTER_ID);
        assert_eq!(config.node_id, "1");
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
    }

    #[tokio::test]
    async fn test_quickwit_config_config_default_values_default_indexer_searcher_config() {
        let config_yaml = r#"
            version: 0
            metastore_uri: postgres://username:password@host:port/db
            data_dir: /opt/quickwit/data
        "#;
        let config_builder = serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
        let config = config_builder.build().await.unwrap();
        assert_eq!(config.version, 0);
        assert_eq!(
            config.metastore_uri,
            "postgres://username:password@host:port/db"
        );
        assert_eq!(config.indexer_config, IndexerConfig::default());
        assert_eq!(config.searcher_config, SearcherConfig::default());
    }

    #[tokio::test]
    async fn test_quickwit_config_validate() {
        let config_filepath = get_config_filepath("quickwit.toml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let file_content = std::fs::read_to_string(&config_filepath).unwrap();
        let data_dir_path = env::current_dir().unwrap();
        let config =
            QuickwitConfig::load(&config_uri, file_content.as_bytes(), Some(data_dir_path))
                .await
                .unwrap();
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_peer_socket_addrs() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: cvb_for_test!(1789),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.unwrap().is_empty());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: cvb_for_test!(1789),
                peer_seeds: cvb_for_test!(PeerSeeds(vec!["unresolvable-host".to_string()])),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert!(quickwit_config.peer_seed_addrs().await.is_err());
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                rest_listen_port: cvb_for_test!(1789),
                peer_seeds: cvb_for_test!(PeerSeeds(vec![
                    "unresolvable-host".to_string(),
                    "localhost".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1".to_string(),
                    "127.0.0.1:1337".to_string(),
                ])),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.peer_seed_addrs().await.unwrap(),
                vec![
                    "localhost:1789".to_string(),
                    "localhost:1337".to_string(),
                    "127.0.0.1:1789".to_string(),
                    "127.0.0.1:1337".to_string()
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_socket_addr_ports() {
        {
            let quickwit_config = QuickwitConfigBuilder {
                listen_address: default_listen_address(),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr().to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr().to_string(),
                "127.0.0.1:7280"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr().to_string(),
                "127.0.0.1:7281"
            );
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                listen_address: default_listen_address(),
                rest_listen_port: cvb_for_test!(1789),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr().to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr().to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr().to_string(),
                "127.0.0.1:1790"
            );
        }
        {
            let quickwit_config = QuickwitConfigBuilder {
                listen_address: default_listen_address(),
                rest_listen_port: cvb_for_test!(1789),
                gossip_listen_port: Some(cvb_for_test!(1889)),
                grpc_listen_port: Some(cvb_for_test!(1989)),
                ..Default::default()
            }
            .build()
            .await
            .unwrap();
            assert_eq!(
                quickwit_config.rest_listen_addr().to_string(),
                "127.0.0.1:1789"
            );
            assert_eq!(
                quickwit_config.gossip_listen_addr().to_string(),
                "127.0.0.1:1889"
            );
            assert_eq!(
                quickwit_config.grpc_listen_addr().to_string(),
                "127.0.0.1:1989"
            );
        }
    }

    #[tokio::test]
    async fn test_load_config_with_validation_error() {
        let config_filepath = get_config_filepath("quickwit.yaml");
        let config_uri = Uri::try_new(&config_filepath).unwrap();
        let file = std::fs::read_to_string(&config_filepath).unwrap();
        let config = QuickwitConfig::load(&config_uri, file.as_bytes(), None)
            .await
            .unwrap_err();
        assert!(config.to_string().contains("Data dir"));
    }

    #[tokio::test]
    async fn test_config_validates_uris() {
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: ''
        "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            config_builder.build().await.unwrap_err();
        }
        {
            let config_yaml = r#"
            version: 0
            node_id: 1
            metastore_uri: postgres://username:password@host:port/db
            default_index_root_uri: ''
        "#;
            let config_builder =
                serde_yaml::from_str::<QuickwitConfigBuilder>(config_yaml).unwrap();
            config_builder.build().await.unwrap_err();
        }
    }
}
