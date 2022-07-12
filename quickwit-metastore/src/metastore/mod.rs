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

pub mod file_backed_metastore;
mod index_metadata;
#[cfg(feature = "postgres")]
pub mod postgresql_metastore;
#[cfg(feature = "postgres")]
mod postgresql_model;

use std::collections::HashSet;
use std::ops::Range;

use async_trait::async_trait;
pub use index_metadata::IndexMetadata;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;

use crate::checkpoint::CheckpointDelta;
use crate::{MetastoreResult, Split, SplitMetadata, SplitState};

/// Metastore meant to manage Quickwit's indexes and their splits.
///
/// Quickwit needs a way to ensure that we can cleanup unused files,
/// and this process needs to be resilient to any fail-stop failures.
/// We rely on atomically transitioning the status of splits.
///
/// The split state goes through the following life cycle:
/// 1. `New`
///   - Create new split and start indexing.
/// 2. `Staged`
///   - Start uploading the split files.
/// 3. `Published`
///   - Uploading the split files is complete and the split is searchable.
/// 4. `MarkedForDeletion`
///   - Mark the split for deletion.
///
/// If a split has a file in the storage, it MUST be registered in the metastore,
/// and its state can be as follows:
/// - `Staged`: The split is almost ready. Some of its files may have been uploaded in the storage.
/// - `Published`: The split is ready and published.
/// - `MarkedForDeletion`: The split is marked for deletion.
///
/// Before creating any file, we need to stage the split. If there is a failure, upon recovery, we
/// schedule for deletion all the staged splits. A client may not necessarily remove files from
/// storage right after marking it for deletion. A CLI client may delete files right away, but a
/// more serious deployment should probably only delete those files after a grace period so that the
/// running search queries can complete.
#[derive(Clone, Debug)]
pub enum Expr<T> {
    Eq(T),
    Gt(T),
    Gte(T),
    Lt(T),
    Lte(T),
}

#[derive(Clone, Debug)]
pub struct SplitFilters {
    split_states: Option<HashSet<SplitState>>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    create_timestamp: Option<Expr<i64>>,
    update_timestamp: Option<Expr<i64>>,
    publish_timestamp: Option<Expr<i64>>,
    tags: Option<TagFilterAst>,
}

impl SplitFilters {
    pub fn builder() -> SplitsFiltersBuilder {
        SplitsFiltersBuilder::new()
    }

    pub fn by_state(&self, split: &Split) -> bool {
        self.split_states
            .as_ref()
            .map(|states| states.contains(&split.split_state))
            .unwrap_or(true)
    }

    pub fn by_tags(&self, split: &Split) -> bool {
        self.tags
            .as_ref()
            .map(|ast| ast.evaluate(&split.split_metadata.tags))
            .unwrap_or(true)
    }

    pub fn by_publish_timestamp(&self, split: &Split) -> bool {
        self.publish_timestamp
            .as_ref()
            .map(|expr| match expr {
                Expr::Eq(ts) => &split.update_timestamp == ts,
                Expr::Gt(ts) => &split.update_timestamp > ts,
                Expr::Gte(ts) => &split.update_timestamp >= ts,
                Expr::Lt(ts) => &split.update_timestamp < ts,
                Expr::Lte(ts) => &split.update_timestamp <= ts,
            })
            .unwrap_or(true)
    }
}

pub struct SplitsFiltersBuilder {
    split_states: HashSet<SplitState>,
    start_timestamp: Option<i64>,
    end_timestamp: Option<i64>,
    create_timestamp: Option<Expr<i64>>,
    update_timestamp: Option<Expr<i64>>,
    publish_timestamp: Option<Expr<i64>>,
    tags: Option<TagFilterAst>,
}

impl SplitsFiltersBuilder {
    pub fn new() -> Self {
        Self {
            split_states: HashSet::new(),
            start_timestamp: None,
            end_timestamp: None,
            create_timestamp: None,
            update_timestamp: None,
            publish_timestamp: None,
            tags: None,
        }
    }

    pub fn build(self) -> SplitFilters {
        let split_states = if !self.split_states.is_empty() {
            Some(self.split_states)
        } else {
            None
        };
        SplitFilters {
            split_states,
            start_timestamp: self.start_timestamp,
            end_timestamp: self.end_timestamp,
            create_timestamp: self.create_timestamp,
            update_timestamp: self.update_timestamp,
            publish_timestamp: self.publish_timestamp,
            tags: self.tags,
        }
    }

    pub fn split_state(mut self, state: SplitState) -> Self {
        self.split_states.insert(state);
        self
    }

    pub fn start_timestamp(mut self, start_timestamp: i64) -> Self {
        self.start_timestamp = Some(start_timestamp);
        self
    }

    pub fn end_timestamp(mut self, end_timestamp: i64) -> Self {
        self.end_timestamp = Some(end_timestamp);
        self
    }

    pub fn created_timestamp(mut self, create_timestamp_expr: Expr<i64>) -> Self {
        self.create_timestamp = Some(create_timestamp_expr);
        self
    }

    pub fn update_timestamp(mut self, update_timestamp_expr: Expr<i64>) -> Self {
        self.update_timestamp = Some(update_timestamp_expr);
        self
    }

    pub fn publish_timestamp(mut self, publish_timestamp_expr: Expr<i64>) -> Self {
        self.publish_timestamp = Some(publish_timestamp_expr);
        self
    }

    pub fn tags(mut self, tags: TagFilterAst) -> Self {
        self.tags = Some(tags);
        self
    }
}

#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Metastore: Send + Sync + 'static {
    /// Checks whether the metastore is available.
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Checks whether the given index is in this metastore.
    async fn check_index_available(&self, index_id: &str) -> anyhow::Result<()> {
        self.index_metadata(index_id).await?;
        Ok(())
    }

    /// Creates an index.
    ///
    /// This API creates a new index in the metastore.
    /// An error will occur if an index that already exists in the storage is specified.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()>;

    /// List indexes.
    ///
    /// This API lists the indexes stored in the metastore and returns a collection of
    /// [`IndexMetadata`].
    async fn list_indexes(&self) -> MetastoreResult<Vec<IndexMetadata>>;

    /// Returns the [`IndexMetadata`] for a given index.
    /// TODO consider merging with list_splits to remove one round-trip
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata>;

    /// Deletes an index.
    ///
    /// This API removes the specified  from the metastore, but does not remove the index from the
    /// storage. An error will occur if an index that does not exist in the storage is
    /// specified.
    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()>;

    /// Stages a split.
    ///
    /// A split needs to be staged before uploading any of its files to the storage.
    /// An error will occur if an index that does not exist in the storage is specified, or if you
    /// specify a split that already exists.
    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()>;

    /// Publishes a list of splits.
    ///
    /// This API only updates the state of the split from [`SplitState::Staged`] to
    /// [`SplitState::Published`]. At this point, the split files are assumed to have already
    /// been uploaded. If the split is already published, this API call returns a success.
    /// An error will occur if you specify an index or split that does not exist in the storage.
    ///
    /// This method can be used to advance the checkpoint, by supplying an empty array for
    /// `split_ids`.
    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        source_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()>;

    /// Replaces a list of splits with another list.
    ///
    /// This API is useful during merge and demux operations.
    /// The new splits should be staged, and the replaced splits should exist.
    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Lists the splits.
    ///
    /// Returns a list of splits that intersects the given `time_range`, `split_state`, and `tag`.
    /// Regardless of the time range filter, if a split has no timestamp it is always returned.
    /// An error will occur if an index that does not exist in the storage is specified.
    async fn list_splits(
        &self,
        index_id: &str,
        split_filters: &SplitFilters,
    ) -> MetastoreResult<Vec<Split>>;

    /// Marks a list of splits for deletion.
    ///
    /// This API will change the state to [`SplitState::MarkedForDeletion`] so that it is not
    /// referenced by the client anymore. It actually does not remove the split from storage. An
    /// error will occur if you specify an index or split that does not exist in the storage.
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()>;

    /// Deletes a list of splits.
    ///
    /// This API only accepts splits that are in [`SplitState::Staged`] or
    /// [`SplitState::MarkedForDeletion`] state. This removes the split metadata from the
    /// metastore, but does not remove the split from storage. An error will occur if you
    /// specify an index or split that does not exist in the storage.
    async fn delete_splits<'a>(&self, index_id: &str, split_ids: &[&'a str])
        -> MetastoreResult<()>;

    /// Adds a new source. Fails with
    /// [`SourceAlreadyExists`](crate::MetastoreError::SourceAlreadyExists) if a source with the
    /// same ID is already defined for the index.
    ///
    /// If a checkpoint is already registered for the source, it is kept.
    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()>;

    /// Deletes a source. Fails with
    /// [`SourceDoesNotExist`](crate::MetastoreError::SourceDoesNotExist) if the specified source
    /// does not exist.
    ///
    /// The checkpoint associated to the source is deleted as well.
    /// If the checkpoint is missing, this does not trigger an error.
    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()>;

    /// Returns the metastore uri.
    fn uri(&self) -> String;
}
