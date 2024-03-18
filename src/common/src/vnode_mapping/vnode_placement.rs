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

use std::collections::{HashMap, HashSet, LinkedList, VecDeque};

use itertools::Itertools;
use num_integer::Integer;
use risingwave_pb::common::WorkerNode;

use crate::buffer::{Bitmap, BitmapBuilder};
use crate::hash::{ParallelUnitId, ParallelUnitMapping, VirtualNode};

/// Calculate a new vnode mapping, keeping locality and balance on a best effort basis.
/// The strategy is similar to `rebalance_actor_vnode` used in meta node, but is modified to
/// consider `max_parallelism` too.
pub fn place_vnode(
    hint_pu_mapping: Option<&ParallelUnitMapping>,
    new_workers: &[WorkerNode],
    max_parallelism: Option<usize>,
) -> Option<ParallelUnitMapping> {
    // Get all serving parallel units from all available workers, grouped by worker id and ordered
    // by parallel unit id in each group.
    let mut new_pus: LinkedList<_> = new_workers
        .iter()
        .filter(|w| w.property.as_ref().map_or(false, |p| p.is_serving))
        .sorted_by_key(|w| w.id)
        .map(|w| w.parallel_units.clone().into_iter().sorted_by_key(|p| p.id))
        .collect();

    // Set serving parallelism to the minimum of total number of parallel units, specified
    // `max_parallelism` and total number of virtual nodes.
    let serving_parallelism = std::cmp::min(
        new_pus.iter().map(|pus| pus.len()).sum(),
        std::cmp::min(max_parallelism.unwrap_or(usize::MAX), VirtualNode::COUNT),
    );

    // Select `serving_parallelism` parallel units in a round-robin fashion, to distribute workload
    // evenly among workers.
    let mut selected_pu_ids = Vec::new();
    while !new_pus.is_empty() {
        new_pus
            .extract_if(|ps| {
                if let Some(p) = ps.next() {
                    selected_pu_ids.push(p.id);
                    false
                } else {
                    true
                }
            })
            .for_each(drop);
    }
    selected_pu_ids.drain(serving_parallelism..);
    let selected_pu_id_set: HashSet<ParallelUnitId> = selected_pu_ids.iter().cloned().collect();
    if selected_pu_id_set.is_empty() {
        return None;
    }

    // Calculate balance for each selected parallel unit. Initially, each parallel unit is assigned
    // no vnodes. Thus its negative balance means that many vnodes should be assigned to it later.
    // `is_temp` is a mark for a special temporary parallel unit, only to simplify implementation.
    #[derive(Debug)]
    struct Balance {
        pu_id: ParallelUnitId,
        balance: i32,
        builder: BitmapBuilder,
        is_temp: bool,
    }
    let (expected, mut remain) = VirtualNode::COUNT.div_rem(&selected_pu_ids.len());
    let mut balances: HashMap<ParallelUnitId, Balance> = HashMap::default();
    for pu_id in &selected_pu_ids {
        let mut balance = Balance {
            pu_id: *pu_id,
            balance: -(expected as i32),
            builder: BitmapBuilder::zeroed(VirtualNode::COUNT),
            is_temp: false,
        };
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
        balances.insert(*pu_id, balance);
    }

    // Now to maintain affinity, if a hint has been provided via `hint_pu_mapping`, follow
    // that mapping to adjust balances.
    let mut temp_pu = Balance {
        pu_id: 0, // This id doesn't matter for `temp_pu`. It's distinguishable via `is_temp`.
        balance: 0,
        builder: BitmapBuilder::zeroed(VirtualNode::COUNT),
        is_temp: true,
    };
    match hint_pu_mapping {
        Some(hint_pu_mapping) => {
            for (vnode, pu_id) in hint_pu_mapping.iter_with_vnode() {
                let b = if selected_pu_id_set.contains(&pu_id) {
                    // Assign vnode to the same parallel unit as hint.
                    balances.get_mut(&pu_id).unwrap()
                } else {
                    // Assign vnode that doesn't belong to any parallel unit to `temp_pu`
                    // temporarily. They will be reassigned later.
                    &mut temp_pu
                };
                b.balance += 1;
                b.builder.set(vnode.to_index(), true);
            }
        }
        None => {
            // No hint is provided, assign all vnodes to `temp_pu`.
            for vnode in VirtualNode::all() {
                temp_pu.balance += 1;
                temp_pu.builder.set(vnode.to_index(), true);
            }
        }
    }

    // The final step is to move vnodes from parallel units with positive balance to parallel units
    // with negative balance, until all parallel units are of 0 balance.
    // A double-ended queue with parallel units ordered by balance in descending order is consumed:
    // 1. Peek 2 parallel units from front and back.
    // 2. It any of them is of 0 balance, pop it and go to step 1.
    // 3. Otherwise, move vnodes from front to back.
    let mut balances: VecDeque<_> = balances
        .into_values()
        .chain(std::iter::once(temp_pu))
        .sorted_by_key(|b| b.balance)
        .rev()
        .collect();
    let mut results: HashMap<ParallelUnitId, Bitmap> = HashMap::default();
    while !balances.is_empty() {
        if balances.len() == 1 {
            let single = balances.pop_front().unwrap();
            assert_eq!(single.balance, 0);
            if !single.is_temp {
                results.insert(single.pu_id, single.builder.finish());
            }
            break;
        }
        let mut src = balances.pop_front().unwrap();
        let mut dst = balances.pop_back().unwrap();
        let n = std::cmp::min(src.balance.abs(), dst.balance.abs());
        let mut moved = 0;
        for idx in 0..VirtualNode::COUNT {
            if moved >= n {
                break;
            }
            if src.builder.is_set(idx) {
                src.builder.set(idx, false);
                assert!(!dst.builder.is_set(idx));
                dst.builder.set(idx, true);
                moved += 1;
            }
        }
        src.balance -= n;
        dst.balance += n;
        if src.balance != 0 {
            balances.push_front(src);
        } else if !src.is_temp {
            results.insert(src.pu_id, src.builder.finish());
        }

        if dst.balance != 0 {
            balances.push_back(dst);
        } else if !dst.is_temp {
            results.insert(dst.pu_id, dst.builder.finish());
        }
    }

    Some(ParallelUnitMapping::from_bitmaps(&results))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_pb::common::worker_node::Property;
    use risingwave_pb::common::{ParallelUnit, WorkerNode};

    use crate::hash::{ParallelUnitId, ParallelUnitMapping, VirtualNode};
    use crate::vnode_mapping::vnode_placement::place_vnode;

    #[test]
    fn test_place_vnode() {
        assert_eq!(VirtualNode::COUNT, 256);
        let mut pu_id_counter: ParallelUnitId = 0;
        let mut pu_to_worker: HashMap<ParallelUnitId, u32> = Default::default();
        let serving_property = Property {
            is_unschedulable: false,
            is_serving: true,
            is_streaming: false,
        };

        let mut gen_pus_for_worker =
            |worker_node_id: u32, number: u32, pu_to_worker: &mut HashMap<ParallelUnitId, u32>| {
                let mut results = vec![];
                for i in 0..number {
                    results.push(ParallelUnit {
                        id: pu_id_counter + i,
                        worker_node_id,
                    })
                }
                pu_id_counter += number;
                for pu in &results {
                    pu_to_worker.insert(pu.id, pu.worker_node_id);
                }
                results
            };

        let count_same_vnode_mapping = |pm1: &ParallelUnitMapping, pm2: &ParallelUnitMapping| {
            assert_eq!(pm1.len(), 256);
            assert_eq!(pm2.len(), 256);
            let mut count: usize = 0;
            for idx in 0..VirtualNode::COUNT {
                let vnode = VirtualNode::from_index(idx);
                if pm1.get(vnode) == pm2.get(vnode) {
                    count += 1;
                }
            }
            count
        };

        let worker_1 = WorkerNode {
            id: 1,
            parallel_units: gen_pus_for_worker(1, 1, &mut pu_to_worker),
            property: Some(serving_property.clone()),
            ..Default::default()
        };
        assert!(
            place_vnode(None, &[worker_1.clone()], Some(0)).is_none(),
            "max_parallelism should >= 0"
        );

        let re_pu_mapping_2 = place_vnode(None, &[worker_1.clone()], None).unwrap();
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 1);
        let worker_2 = WorkerNode {
            id: 2,
            parallel_units: gen_pus_for_worker(2, 50, &mut pu_to_worker),
            property: Some(serving_property.clone()),
            ..Default::default()
        };
        let re_pu_mapping = place_vnode(
            Some(&re_pu_mapping_2),
            &[worker_1.clone(), worker_2.clone()],
            None,
        )
        .unwrap();

        assert_eq!(re_pu_mapping.iter_unique().count(), 51);
        // 1 * 256 + 0 -> 51 * 5 + 1
        let score = count_same_vnode_mapping(&re_pu_mapping_2, &re_pu_mapping);
        assert!(score >= 5);

        let worker_3 = WorkerNode {
            id: 3,
            parallel_units: gen_pus_for_worker(3, 60, &mut pu_to_worker),
            property: Some(serving_property),
            ..Default::default()
        };
        let re_pu_mapping_2 = place_vnode(
            Some(&re_pu_mapping),
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            None,
        )
        .unwrap();

        // limited by total pu number
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 111);
        // 51 * 5 + 1 -> 111 * 2 + 34
        let score = count_same_vnode_mapping(&re_pu_mapping_2, &re_pu_mapping);
        assert!(score >= (2 + 50 * 2));
        let re_pu_mapping = place_vnode(
            Some(&re_pu_mapping_2),
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            Some(50),
        )
        .unwrap();
        // limited by max_parallelism
        assert_eq!(re_pu_mapping.iter_unique().count(), 50);
        // 111 * 2 + 34 -> 50 * 5 + 6
        let score = count_same_vnode_mapping(&re_pu_mapping, &re_pu_mapping_2);
        assert!(score >= 50 * 2);
        let re_pu_mapping_2 = place_vnode(
            Some(&re_pu_mapping),
            &[worker_1.clone(), worker_2, worker_3.clone()],
            None,
        )
        .unwrap();
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 111);
        // 50 * 5 + 6 -> 111 * 2 + 34
        let score = count_same_vnode_mapping(&re_pu_mapping_2, &re_pu_mapping);
        assert!(score >= 50 * 2);
        let re_pu_mapping =
            place_vnode(Some(&re_pu_mapping_2), &[worker_1, worker_3.clone()], None).unwrap();
        // limited by total pu number
        assert_eq!(re_pu_mapping.iter_unique().count(), 61);
        // 111 * 2 + 34 -> 61 * 4 + 12
        let score = count_same_vnode_mapping(&re_pu_mapping, &re_pu_mapping_2);
        assert!(score >= 61 * 2);
        assert!(place_vnode(Some(&re_pu_mapping), &[], None).is_none());
        let re_pu_mapping = place_vnode(Some(&re_pu_mapping), &[worker_3], None).unwrap();
        assert_eq!(re_pu_mapping.iter_unique().count(), 60);
        assert!(place_vnode(Some(&re_pu_mapping), &[], None).is_none());
    }
}
