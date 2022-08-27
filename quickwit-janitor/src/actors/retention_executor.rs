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

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{
    quickwit_metastore_uri_resolver, Metastore, MetastoreError, Split, SplitState,
};
use time::OffsetDateTime;
use tracing::info;

#[derive(Debug, Clone, Default)]
pub struct RetentionExecutorCounters {}

#[derive(Debug)]
struct Loop;

#[derive(Debug)]
struct ApplyRetentionPolicy {
    index_id: String,
}

pub struct RetentionEnforcer {
    metastore: Arc<dyn Metastore>,
    tokens: HashMap<String, u64>,
    counters: RetentionExecutorCounter,
}

impl RetentionEnforcer {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self {
            metastore,
            tokens: HashMap::new(),
            counters: RetentionExecutorCounters::default(),
        }
    }
}

#[async_trait]
impl Actor for RetentionExecutor {
    type ObservableState = RetentionExecutorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        "RetentionExecutor".to_string()
    }

    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        // self.handle(Loop, ctx).await
        Ok(())
    }
}

// macro_rules! ignore_index_not_found {
//     ($metastore_res: expr) => {
//         match $metastore_res {
//             Ok(something) => something,
//             Err(MetastoreError::IndexDoesNotExist { .. }) => return Ok(()),
//             Err(error) => return Err(ActorExitStatus::Failure(Arc::new(anyhow!(error)))),
//         }
//     };
// }

// #[async_trait]
// impl Handler<ApplyRetentionPolicy> for RetentionExecutor {
//     type Reply = ();

//     async fn handle(
//         &mut self,
//         message: ApplyRetentionPolicy,
//         ctx: &ActorContext<Self>,
//     ) -> Result<(), ActorExitStatus> {
// let ApplyRetentionPolicy { index_id } = message;

// let index_metadata =
//     ignore_index_not_found!(self.metastore.index_metadata(&index_id).await);

// let retention_policy = match index_metadata.retention_policy {
//     Some(retention_policy) => retention_policy,
//     None => return Ok(()),
// };
// let now_timestamp = OffsetDateTime::now_utc().unix_timestamp();
// let cutoff_timestamp = now_timestamp - retention_policy.period.as_secs() as i64;
// let cutoff_time_range = if index_metadata.is_timeseries() {
//     Some(i64::MIN..cutoff_timestamp)
// } else {
//     None
// };
// let splits = ignore_index_not_found!(
//     self.metastore
//         .list_splits(&index_id, SplitState::Published, cutoff_time_range, None)
//         .await
// );
// let target_split_ids = splits
//     .iter()
//     .filter(|split| {
//         let split_ts = split
//             .metadata
//             .time_range
//             .map(|time_range| time_range.end())
//             .unwrap_or(&split.update_timestamp); // TODO: Use publish_timestamp
//         split_ts <= &cutoff_timestamp
//     })
//     .map(|split| split.split_id())
//     .collect::<Vec<_>>();

// ignore_index_not_found!(
//     self.metastore
//         .mark_splits_for_deletion(&index_id, &target_split_ids)
//         .await
// );
// // TODO: compute with schedule
// let hourly = Duration::from_secs(3600);
// ctx.schedule_self_msg(hourly, ApplyRetentionPolicy { index_id, schedule })
//     .await;
// Ok(())
//     }
// }

// #[async_trait]
// impl Handler<Loop> for RetentionExecutor {
//     type Reply = ();

//     async fn handle(&mut self, _: Loop, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus>
// {         for index_metadata in self.metastore.list_indexes().await? {
//             if let Some(retention_policy) = index_metadata.retention_policy {
//                 let fingerprint = retention_policy.fingerprint();
//                 let entry = self.tokens.entry(index_metadata.clone());
//                 entry.
//                 let entry = self.tokens.
//                 let token = retention_policy.hash(&mut state);

//                 self.active_policies.insert(index_metadata.index_id.clone());
//                 // TODO:
//                 let hourly = Duration::from_secs(3600);
//                 let message = ApplyRetentionPolicy {
//                     index_id: index_metadata.index_id,
//                 };
//                 ctx.schedule_self_msg(hourly, message).await;
//             }
//         }
//         ctx.schedule_self_msg(Duration::from_secs(10 * 60), Loop).await;
//         Ok(())
//     }
// }

#[cfg(test)]
mod tests {}
