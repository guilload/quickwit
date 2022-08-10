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

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::{any, fmt};

use anyhow::{anyhow, bail};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigValueSource {
    EnvVar(String),
    EnvVarDefault(String),
    Provided,
    QuickwitDefault,
    Default,
}

#[derive(Clone, Debug)]
pub struct ConfigValue<T> {
    pub value: T,
    pub source: ConfigValueSource,
}

impl<T> ConfigValue<T> {
    pub fn provided(value: T) -> Self {
        Self {
            value,
            source: ConfigValueSource::Provided,
        }
    }
}

impl<T> fmt::Display for ConfigValue<T>
where T: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<T> Default for ConfigValue<T>
where T: Default
{
    fn default() -> Self {
        Self {
            value: T::default(),
            source: ConfigValueSource::Default,
        }
    }
}

impl<T> Deref for ConfigValue<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, U> PartialEq<U> for ConfigValue<T>
where T: PartialEq<U>
{
    fn eq(&self, other: &U) -> bool {
        self.value.eq(other)
    }
}

impl<T> Serialize for ConfigValue<T>
where T: Serialize
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.value.serialize(serializer)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigValueOverride<T> {
    Maybe(String),
    No(T),
}

#[derive(Debug, PartialEq)]
pub(crate) struct ConfigValueBuilder<T> {
    env_var_key: Option<String>,
    env_var_default: Option<String>,
    provided: Option<T>,
    quickwit_default: Option<T>,
    defaultify: bool,
}

impl<T> ConfigValueBuilder<T>
where
    T: Default + FromStr,
    <T as FromStr>::Err: fmt::Debug,
{
    pub fn build(self, env_vars: &HashMap<String, String>) -> anyhow::Result<ConfigValue<T>> {
        if let Some(env_var_key) = self.env_var_key {
            if let Some(env_var_value) = env_vars.get(&env_var_key) {
                let value = env_var_value.parse::<T>().map_err(|error| {
                    anyhow!(
                        "Failed to convert value `{}` of environment variable `{}` to type `{}`: \
                         {:?}",
                        env_var_value,
                        env_var_key,
                        any::type_name::<T>(),
                        error
                    )
                })?;
                return Ok(ConfigValue {
                    value,
                    source: ConfigValueSource::EnvVar(env_var_key),
                });
            } else if let Some(env_var_default) = self.env_var_default {
                let value = env_var_default.parse::<T>().map_err(|error| {
                    anyhow!(
                        "Failed to convert default value `{}` of environment variable `{}` to \
                         type `{}`: {:?}",
                        env_var_default,
                        env_var_key,
                        any::type_name::<T>(),
                        error
                    )
                })?;
                return Ok(ConfigValue {
                    value,
                    source: ConfigValueSource::EnvVarDefault(env_var_key),
                });
            }
        }
        if let Some(value) = self.provided {
            return Ok(ConfigValue {
                value,
                source: ConfigValueSource::Provided,
            });
        }
        if let Some(value) = self.quickwit_default {
            return Ok(ConfigValue {
                value,
                source: ConfigValueSource::QuickwitDefault,
            });
        }
        if self.defaultify {
            let value = T::default();
            return Ok(ConfigValue {
                value,
                source: ConfigValueSource::Default,
            });
        }
        bail!("Failed to build config value. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues.")
    }

    pub fn quickwit_default(value: T) -> Self {
        Self {
            quickwit_default: Some(value),
            defaultify: false,
            ..Default::default()
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(self) -> ConfigValue<T> {
        let env_vars = HashMap::new();
        self.build(&env_vars).expect("")
    }
}

impl<T> Default for ConfigValueBuilder<T> {
    fn default() -> Self {
        Self {
            env_var_key: None,
            env_var_default: None,
            provided: None,
            quickwit_default: None,
            defaultify: true,
        }
    }
}

impl<'de, T> Deserialize<'de> for ConfigValueBuilder<T>
where
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<ConfigValueBuilder<T>, D::Error>
    where D: Deserializer<'de> {
        let maybe_override = match ConfigValueOverride::deserialize(deserializer)? {
            ConfigValueOverride::Maybe(maybe_override) => maybe_override,
            ConfigValueOverride::No(value) => {
                return Ok(ConfigValueBuilder {
                    provided: Some(value),
                    defaultify: false,
                    ..Default::default()
                })
            }
        };
        if let Some((env_var_key, env_var_default)) = parse_env_var_override(&maybe_override) {
            return Ok(ConfigValueBuilder {
                env_var_key: Some(env_var_key),
                env_var_default,
                defaultify: false,
                ..Default::default()
            });
        }
        // Cast the `String` back into a `T`...
        let value = maybe_override.parse::<T>().map_err(D::Error::custom)?;
        Ok(ConfigValueBuilder {
            provided: Some(value),
            defaultify: false,
            ..Default::default()
        })
    }
}

fn parse_env_var_override(maybe_override: &str) -> Option<(String, Option<String>)> {
    let maybe_trimmed_override = maybe_override.trim();
    if !maybe_trimmed_override.starts_with("${") || !maybe_trimmed_override.ends_with("}") {
        return None;
    }
    let env_var_override = &maybe_trimmed_override[2..maybe_trimmed_override.len() - 1];

    if let Some((env_var_key, env_var_default)) = env_var_override.split_once(":-") {
        Some((
            env_var_key.trim().to_string(),
            Some(env_var_default.trim().to_string()),
        ))
    } else {
        Some((env_var_override.trim().to_string(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_value_display() {
        let config_value: ConfigValue<usize> = ConfigValue::default();
        assert_eq!(format!("{config_value}"), "0");
    }

    #[test]
    fn test_config_value_default() {
        let config_value: ConfigValue<usize> = ConfigValue::default();
        assert_eq!(config_value.value, 0);
        assert_eq!(config_value.source, ConfigValueSource::Default);
    }

    #[test]
    fn test_config_value_builder_build() {
        let mut env_vars = HashMap::new();
        env_vars.insert("FOO".to_string(), "0".to_string());
        env_vars.insert("BAR".to_string(), "a".to_string());
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder {
                env_var_key: Some("FOO".to_string()),
                defaultify: true,
                ..Default::default()
            };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, 0);
            assert_eq!(
                config_value.source,
                ConfigValueSource::EnvVar("FOO".to_string())
            );
        }
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder {
                env_var_key: Some("BAR".to_string()),
                ..Default::default()
            };
            config_value_builder.build(&env_vars).unwrap_err();
        }
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder {
                env_var_key: Some("QUX".to_string()),
                env_var_default: Some("0".to_string()),
                ..Default::default()
            };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, 0);
            assert_eq!(
                config_value.source,
                ConfigValueSource::EnvVarDefault("QUX".to_string())
            );
        }
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder {
                env_var_key: Some("QUX".to_string()),
                env_var_default: Some("a".to_string()),
                ..Default::default()
            };
            config_value_builder.build(&env_vars).unwrap_err();
        }
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder {
                provided: Some(0),
                ..Default::default()
            };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, 0);
            assert_eq!(config_value.source, ConfigValueSource::Provided);
        }
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder {
                quickwit_default: Some(0),
                ..Default::default()
            };
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, 0);
            assert_eq!(config_value.source, ConfigValueSource::QuickwitDefault);
        }
        {
            let config_value_builder: ConfigValueBuilder<usize> = ConfigValueBuilder::default();
            let config_value = config_value_builder.build(&env_vars).unwrap();
            assert_eq!(config_value.value, 0);
            assert_eq!(config_value.source, ConfigValueSource::Default);
        }
    }

    #[test]
    fn test_config_value_builder_deser() {
        #[derive(Debug, Deserialize)]
        struct MyConfigBuilder {
            #[serde(default)]
            version: ConfigValueBuilder<usize>,
            #[serde(default = "my_cluster_id")]
            cluster_id: ConfigValueBuilder<String>,
            node_id: ConfigValueBuilder<String>,
            listen_address: ConfigValueBuilder<String>,
            listen_port: ConfigValueBuilder<usize>,
        }

        fn my_cluster_id() -> ConfigValueBuilder<String> {
            ConfigValueBuilder::quickwit_default("my-cluster".to_string())
        }

        let config_yaml = r#"
            node_id: my-node
            listen_address: ${QW_LISTEN_ADDRESS}
            listen_port: ${QW_LISTEN_PORT:-7280}
        "#;
        let config_builder = serde_yaml::from_str::<MyConfigBuilder>(config_yaml).unwrap();
        assert_eq!(
            config_builder.version,
            ConfigValueBuilder {
                defaultify: true,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.cluster_id,
            ConfigValueBuilder {
                quickwit_default: Some("my-cluster".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.node_id,
            ConfigValueBuilder {
                provided: Some("my-node".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.listen_address,
            ConfigValueBuilder {
                env_var_key: Some("QW_LISTEN_ADDRESS".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
        assert_eq!(
            config_builder.listen_port,
            ConfigValueBuilder {
                env_var_key: Some("QW_LISTEN_PORT".to_string()),
                env_var_default: Some("7280".to_string()),
                defaultify: false,
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_parse_env_var_override() {}
}
