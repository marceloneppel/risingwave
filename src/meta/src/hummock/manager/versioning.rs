// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};

use function_name::named;
use itertools::Itertools;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    build_initial_compaction_group_levels, get_compaction_group_ids, BranchedSstInfo,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::table_stats::add_prost_table_stats_map;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockSstableObjectId, HummockVersionId, FIRST_VERSION_ID,
};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::write_limits::WriteLimit;
use risingwave_pb::hummock::{
    CompactionConfig, HummockPinnedSnapshot, HummockPinnedVersion, HummockVersionStats,
    SstableInfo, TableStats,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};

use crate::hummock::error::Result;
use crate::hummock::manager::checkpoint::HummockVersionCheckpoint;
use crate::hummock::manager::worker::{HummockManagerEvent, HummockManagerEventSender};
use crate::hummock::manager::{commit_multi_var, create_trx_wrapper, read_lock, write_lock};
use crate::hummock::metrics_utils::{
    trigger_safepoint_stat, trigger_write_stop_stats, LocalTableMetrics,
};
use crate::hummock::model::CompactionGroup;
use crate::hummock::HummockManager;
use crate::model::{VarTransaction, VarTransactionWrapper};
use crate::storage::MetaStore;
use crate::MetaResult;

/// `HummockVersionSafePoint` prevents hummock versions GE than it from being GC.
/// It's used by meta node itself to temporarily pin versions.
pub struct HummockVersionSafePoint {
    pub id: HummockVersionId,
    event_sender: HummockManagerEventSender,
}

impl Drop for HummockVersionSafePoint {
    fn drop(&mut self) {
        if self
            .event_sender
            .send(HummockManagerEvent::DropSafePoint(self.id))
            .is_err()
        {
            tracing::debug!("failed to drop hummock version safe point {}", self.id);
        }
    }
}

#[derive(Default)]
pub struct Versioning {
    // Volatile states below
    /// Avoide commit epoch epochs
    /// Don't persist compaction version delta to meta store
    pub disable_commit_epochs: bool,
    /// Latest hummock version
    pub current_version: HummockVersion,
    /// Objects that waits to be deleted from object store. It comes from either compaction, or
    /// full GC (listing object store).
    pub objects_to_delete: HashSet<HummockSstableObjectId>,
    /// SST whose `object_id` != `sst_id`
    pub branched_ssts: BTreeMap<
        // SST object id
        HummockSstableObjectId,
        BranchedSstInfo,
    >,
    /// `version_safe_points` is similar to `pinned_versions` expect for being a transient state.
    pub version_safe_points: Vec<HummockVersionId>,
    /// Tables that write limit is trigger for.
    pub write_limit: HashMap<CompactionGroupId, WriteLimit>,

    // Persistent states below
    pub hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta>,
    pub pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
    pub pinned_snapshots: BTreeMap<HummockContextId, HummockPinnedSnapshot>,
    /// Stats for latest hummock version.
    pub version_stats: HummockVersionStats,
    pub checkpoint: HummockVersionCheckpoint,
    pub local_metrics: HashMap<u32, LocalTableMetrics>,
}

impl Versioning {
    pub fn min_pinned_version_id(&self) -> HummockVersionId {
        let mut min_pinned_version_id = HummockVersionId::MAX;
        for id in self
            .pinned_versions
            .values()
            .map(|v| v.min_pinned_id)
            .chain(self.version_safe_points.iter().cloned())
        {
            min_pinned_version_id = cmp::min(id, min_pinned_version_id);
        }
        min_pinned_version_id
    }

    /// Marks all objects <= `min_pinned_version_id` for deletion.
    pub(super) fn mark_objects_for_deletion(&mut self) {
        let min_pinned_version_id = self.min_pinned_version_id();
        self.objects_to_delete.extend(
            self.checkpoint
                .stale_objects
                .iter()
                .filter(|(version_id, _)| **version_id <= min_pinned_version_id)
                .flat_map(|(_, stale_objects)| stale_objects.id.clone()),
        );
    }

    /// If there is some sst in the target group which is just split but we have not compact it, we
    ///  can not split or move state-table to those group, because it may cause data overlap.
    pub fn check_branched_sst_in_target_group(
        &self,
        table_ids: &[StateTableId],
        source_group_id: &CompactionGroupId,
        target_group_id: &CompactionGroupId,
    ) -> bool {
        for groups in self.branched_ssts.values() {
            if groups.contains_key(target_group_id) && groups.contains_key(source_group_id) {
                return false;
            }
        }
        let mut found_sstable_repeated = false;
        let moving_table_ids: HashSet<&u32> = HashSet::from_iter(table_ids);
        if let Some(group) = self.current_version.levels.get(target_group_id) {
            let target_member_table_ids: HashSet<u32> =
                HashSet::from_iter(group.member_table_ids.clone());
            self.current_version.level_iter(*source_group_id, |level| {
                for sst in &level.table_infos {
                    if sst
                        .table_ids
                        .iter()
                        .all(|table_id| !moving_table_ids.contains(table_id))
                    {
                        continue;
                    }
                    for table_id in &sst.table_ids {
                        if target_member_table_ids.contains(table_id) {
                            found_sstable_repeated = true;
                            return false;
                        }
                    }
                }
                true
            });
        }
        !found_sstable_repeated
    }
}

impl HummockManager {
    #[named]
    pub async fn list_pinned_version(&self) -> Vec<HummockPinnedVersion> {
        read_lock!(self, versioning)
            .await
            .pinned_versions
            .values()
            .cloned()
            .collect_vec()
    }

    #[named]
    pub async fn list_pinned_snapshot(&self) -> Vec<HummockPinnedSnapshot> {
        read_lock!(self, versioning)
            .await
            .pinned_snapshots
            .values()
            .cloned()
            .collect_vec()
    }

    pub async fn list_workers(
        &self,
        context_ids: &[HummockContextId],
    ) -> MetaResult<HashMap<HummockContextId, WorkerNode>> {
        let mut workers = HashMap::new();
        for context_id in context_ids {
            if let Some(worker_node) = self
                .metadata_manager()
                .get_worker_by_id(*context_id)
                .await?
            {
                workers.insert(*context_id, worker_node);
            }
        }
        Ok(workers)
    }

    #[named]
    pub async fn get_version_stats(&self) -> HummockVersionStats {
        read_lock!(self, versioning).await.version_stats.clone()
    }

    #[named]
    pub async fn register_safe_point(&self) -> HummockVersionSafePoint {
        let mut wl = write_lock!(self, versioning).await;
        let safe_point = HummockVersionSafePoint {
            id: wl.current_version.id,
            event_sender: self.event_sender.clone(),
        };
        wl.version_safe_points.push(safe_point.id);
        trigger_safepoint_stat(&self.metrics, &wl.version_safe_points);
        safe_point
    }

    #[named]
    pub async fn unregister_safe_point(&self, safe_point: HummockVersionId) {
        let mut wl = write_lock!(self, versioning).await;
        let version_safe_points = &mut wl.version_safe_points;
        if let Some(pos) = version_safe_points.iter().position(|sp| *sp == safe_point) {
            version_safe_points.remove(pos);
        }
        trigger_safepoint_stat(&self.metrics, &wl.version_safe_points);
    }

    /// Updates write limits for `target_groups` and sends notification.
    /// Returns true if `write_limit` has been modified.
    /// The implementation acquires `versioning` lock and `compaction_group_manager` lock.
    #[named]
    pub(super) async fn try_update_write_limits(
        &self,
        target_group_ids: &[CompactionGroupId],
    ) -> bool {
        let mut guard = write_lock!(self, versioning).await;
        let config_mgr = self.compaction_group_manager.read().await;
        let target_group_configs = target_group_ids
            .iter()
            .filter_map(|id| {
                config_mgr
                    .try_get_compaction_group_config(*id)
                    .map(|config| (*id, config))
            })
            .collect();
        let mut new_write_limits = calc_new_write_limits(
            target_group_configs,
            guard.write_limit.clone(),
            &guard.current_version,
        );
        let all_group_ids: HashSet<_> =
            HashSet::from_iter(get_compaction_group_ids(&guard.current_version));
        new_write_limits.retain(|group_id, _| all_group_ids.contains(group_id));
        if new_write_limits == guard.write_limit {
            return false;
        }
        tracing::debug!("Hummock stopped write is updated: {:#?}", new_write_limits);
        trigger_write_stop_stats(&self.metrics, &new_write_limits);
        guard.write_limit = new_write_limits;
        self.env
            .notification_manager()
            .notify_hummock_without_version(
                Operation::Add,
                Info::HummockWriteLimits(risingwave_pb::hummock::WriteLimits {
                    write_limits: guard.write_limit.clone(),
                }),
            );
        true
    }

    /// Gets write limits.
    /// The implementation acquires `versioning` lock.
    #[named]
    pub async fn write_limits(&self) -> HashMap<CompactionGroupId, WriteLimit> {
        let guard = read_lock!(self, versioning).await;
        guard.write_limit.clone()
    }

    #[named]
    pub async fn list_branched_objects(&self) -> BTreeMap<HummockSstableObjectId, BranchedSstInfo> {
        let guard = read_lock!(self, versioning).await;
        guard.branched_ssts.clone()
    }

    #[named]
    pub async fn rebuild_table_stats(&self) -> Result<()> {
        use crate::model::ValTransaction;
        let mut versioning = write_lock!(self, versioning).await;
        let new_stats = rebuild_table_stats(&versioning.current_version);
        let mut version_stats = create_trx_wrapper!(
            self.sql_meta_store(),
            VarTransactionWrapper,
            VarTransaction::new(&mut versioning.version_stats)
        );
        // version_stats.hummock_version_id is always 0 in meta store.
        version_stats.table_stats = new_stats.table_stats;
        commit_multi_var!(self.env.meta_store(), self.sql_meta_store(), version_stats)?;
        Ok(())
    }
}

/// Calculates write limits for `target_groups`.
/// Returns a new complete write limits snapshot based on `origin_snapshot` and `version`.
pub(super) fn calc_new_write_limits(
    target_groups: HashMap<CompactionGroupId, CompactionGroup>,
    origin_snapshot: HashMap<CompactionGroupId, WriteLimit>,
    version: &HummockVersion,
) -> HashMap<CompactionGroupId, WriteLimit> {
    let mut new_write_limits = origin_snapshot;
    for (id, config) in &target_groups {
        let levels = match version.levels.get(id) {
            None => {
                new_write_limits.remove(id);
                continue;
            }
            Some(levels) => levels,
        };
        // Add write limit conditions here.
        let threshold = config
            .compaction_config
            .level0_stop_write_threshold_sub_level_number as usize;
        let l0_sub_level_number = levels.l0.as_ref().unwrap().sub_levels.len();
        if threshold < l0_sub_level_number {
            new_write_limits.insert(
                *id,
                WriteLimit {
                    table_ids: levels.member_table_ids.clone(),
                    reason: format!(
                        "too many L0 sub levels: {} > {}",
                        l0_sub_level_number, threshold
                    ),
                },
            );
            continue;
        }
        // No condition is met.
        new_write_limits.remove(id);
    }
    new_write_limits
}

pub(super) fn create_init_version(default_compaction_config: CompactionConfig) -> HummockVersion {
    let mut init_version = HummockVersion {
        id: FIRST_VERSION_ID,
        levels: Default::default(),
        max_committed_epoch: INVALID_EPOCH,
        safe_epoch: INVALID_EPOCH,
        table_watermarks: HashMap::new(),
    };
    for group_id in [
        StaticCompactionGroupId::StateDefault as CompactionGroupId,
        StaticCompactionGroupId::MaterializedView as CompactionGroupId,
    ] {
        init_version.levels.insert(
            group_id,
            build_initial_compaction_group_levels(group_id, &default_compaction_config),
        );
    }
    init_version
}

/// Rebuilds table stats from the given version.
/// Note that the result is approximate value. See `estimate_table_stats`.
fn rebuild_table_stats(version: &HummockVersion) -> HummockVersionStats {
    let mut stats = HummockVersionStats {
        hummock_version_id: version.id,
        table_stats: Default::default(),
    };
    for level in version.get_combined_levels() {
        for sst in &level.table_infos {
            let changes = estimate_table_stats(sst);
            add_prost_table_stats_map(&mut stats.table_stats, &changes);
        }
    }
    stats
}

/// Estimates table stats change from the given file.
/// - The file stats is evenly distributed among multiple tables within the file.
/// - The total key size and total value size are estimated based on key range and file size.
/// - Branched files may lead to an overestimation.
fn estimate_table_stats(sst: &SstableInfo) -> HashMap<u32, TableStats> {
    let mut changes: HashMap<u32, TableStats> = HashMap::default();
    let weighted_value =
        |value: i64| -> i64 { (value as f64 / sst.table_ids.len() as f64).ceil() as i64 };
    let key_range = sst.key_range.as_ref().unwrap();
    let estimated_key_size: u64 = (key_range.left.len() + key_range.right.len()) as u64 / 2;
    let mut estimated_total_key_size = estimated_key_size * sst.total_key_count;
    if estimated_total_key_size > sst.uncompressed_file_size {
        estimated_total_key_size = sst.uncompressed_file_size / 2;
        tracing::warn!(sst.sst_id, "Calculated estimated_total_key_size {} > uncompressed_file_size {}. Use uncompressed_file_size/2 as estimated_total_key_size instead.", estimated_total_key_size, sst.uncompressed_file_size);
    }
    let estimated_total_value_size = sst.uncompressed_file_size - estimated_total_key_size;
    for table_id in &sst.table_ids {
        let e = changes.entry(*table_id).or_default();
        e.total_key_count += weighted_value(sst.total_key_count as i64);
        e.total_key_size += weighted_value(estimated_total_key_size as i64);
        e.total_value_size += weighted_value(estimated_total_value_size as i64);
    }
    changes
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{CompactionGroupId, HummockVersionId};
    use risingwave_pb::hummock::hummock_version::Levels;
    use risingwave_pb::hummock::write_limits::WriteLimit;
    use risingwave_pb::hummock::{
        HummockPinnedVersion, HummockVersionStats, KeyRange, Level, OverlappingLevel, SstableInfo,
    };

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::manager::versioning::{
        calc_new_write_limits, estimate_table_stats, rebuild_table_stats, Versioning,
    };
    use crate::hummock::model::CompactionGroup;

    #[test]
    fn test_min_pinned_version_id() {
        let mut versioning = Versioning::default();
        assert_eq!(versioning.min_pinned_version_id(), HummockVersionId::MAX);
        versioning.pinned_versions.insert(
            1,
            HummockPinnedVersion {
                context_id: 1,
                min_pinned_id: 10,
            },
        );
        assert_eq!(versioning.min_pinned_version_id(), 10);
        versioning.version_safe_points.push(5);
        assert_eq!(versioning.min_pinned_version_id(), 5);
        versioning.version_safe_points.clear();
        assert_eq!(versioning.min_pinned_version_id(), 10);
        versioning.pinned_versions.clear();
        assert_eq!(versioning.min_pinned_version_id(), HummockVersionId::MAX);
    }

    #[test]
    fn test_calc_new_write_limits() {
        let add_level_to_l0 = |levels: &mut Levels| {
            levels
                .l0
                .as_mut()
                .unwrap()
                .sub_levels
                .push(Level::default());
        };
        let set_sub_level_number_threshold_for_group_1 =
            |target_groups: &mut HashMap<CompactionGroupId, CompactionGroup>,
             sub_level_number_threshold: u64| {
                target_groups.insert(
                    1,
                    CompactionGroup {
                        group_id: 1,
                        compaction_config: Arc::new(
                            CompactionConfigBuilder::new()
                                .level0_stop_write_threshold_sub_level_number(
                                    sub_level_number_threshold,
                                )
                                .build(),
                        ),
                    },
                );
            };

        let mut target_groups: HashMap<CompactionGroupId, CompactionGroup> = Default::default();
        set_sub_level_number_threshold_for_group_1(&mut target_groups, 10);
        let origin_snapshot: HashMap<CompactionGroupId, WriteLimit> = [(
            2,
            WriteLimit {
                table_ids: vec![1, 2, 3],
                reason: "for test".to_string(),
            },
        )]
        .into_iter()
        .collect();
        let mut version: HummockVersion = Default::default();
        for group_id in 1..=3 {
            version.levels.insert(
                group_id,
                Levels {
                    l0: Some(OverlappingLevel::default()),
                    ..Default::default()
                },
            );
        }
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );
        assert_eq!(new_write_limits.len(), 1);
        for _ in 1..=10 {
            add_level_to_l0(version.levels.get_mut(&1).unwrap());
            let new_write_limits =
                calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
            assert_eq!(
                new_write_limits, origin_snapshot,
                "write limit should not be triggered for group 1"
            );
        }
        add_level_to_l0(version.levels.get_mut(&1).unwrap());
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_ne!(
            new_write_limits, origin_snapshot,
            "write limit should be triggered for group 1"
        );
        assert_eq!(
            new_write_limits.get(&1).as_ref().unwrap().reason,
            "too many L0 sub levels: 11 > 10"
        );
        assert_eq!(new_write_limits.len(), 2);

        set_sub_level_number_threshold_for_group_1(&mut target_groups, 100);
        let new_write_limits =
            calc_new_write_limits(target_groups.clone(), origin_snapshot.clone(), &version);
        assert_eq!(
            new_write_limits, origin_snapshot,
            "write limit should not be triggered for group 1"
        );

        set_sub_level_number_threshold_for_group_1(&mut target_groups, 5);
        let new_write_limits =
            calc_new_write_limits(target_groups, origin_snapshot.clone(), &version);
        assert_ne!(
            new_write_limits, origin_snapshot,
            "write limit should be triggered for group 1"
        );
        assert_eq!(
            new_write_limits.get(&1).as_ref().unwrap().reason,
            "too many L0 sub levels: 11 > 5"
        );
    }

    #[test]
    fn test_estimate_table_stats() {
        let sst = SstableInfo {
            key_range: Some(KeyRange {
                left: vec![1; 10],
                right: vec![1; 20],
                ..Default::default()
            }),
            table_ids: vec![1, 2, 3],
            total_key_count: 6000,
            uncompressed_file_size: 6_000_000,
            ..Default::default()
        };
        let changes = estimate_table_stats(&sst);
        assert_eq!(changes.len(), 3);
        for stats in changes.values() {
            assert_eq!(stats.total_key_count, 6000 / 3);
            assert_eq!(stats.total_key_size, (10 + 20) / 2 * 6000 / 3);
            assert_eq!(
                stats.total_value_size,
                (6_000_000 - (10 + 20) / 2 * 6000) / 3
            );
        }

        let mut version = HummockVersion {
            id: 123,
            levels: Default::default(),
            max_committed_epoch: 0,
            safe_epoch: 0,
            table_watermarks: HashMap::new(),
        };
        for cg in 1..3 {
            version.levels.insert(
                cg,
                Levels {
                    levels: vec![Level {
                        table_infos: vec![sst.clone()],
                        ..Default::default()
                    }],
                    l0: Some(Default::default()),
                    ..Default::default()
                },
            );
        }
        let HummockVersionStats {
            hummock_version_id,
            table_stats,
        } = rebuild_table_stats(&version);
        assert_eq!(hummock_version_id, version.id);
        assert_eq!(table_stats.len(), 3);
        for (tid, stats) in table_stats {
            assert_eq!(
                stats.total_key_count,
                changes.get(&tid).unwrap().total_key_count * 2
            );
            assert_eq!(
                stats.total_key_size,
                changes.get(&tid).unwrap().total_key_size * 2
            );
            assert_eq!(
                stats.total_value_size,
                changes.get(&tid).unwrap().total_value_size * 2
            );
        }
    }

    #[test]
    fn test_estimate_table_stats_large_key_range() {
        let sst = SstableInfo {
            key_range: Some(KeyRange {
                left: vec![1; 1000],
                right: vec![1; 2000],
                ..Default::default()
            }),
            table_ids: vec![1, 2, 3],
            total_key_count: 6000,
            uncompressed_file_size: 60_000,
            ..Default::default()
        };
        let changes = estimate_table_stats(&sst);
        assert_eq!(changes.len(), 3);
        for t in &sst.table_ids {
            let stats = changes.get(t).unwrap();
            assert_eq!(stats.total_key_count, 6000 / 3);
            assert_eq!(stats.total_key_size, 60_000 / 2 / 3);
            assert_eq!(stats.total_value_size, (60_000 - 60_000 / 2) / 3);
        }
    }
}
