// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::ArrowPredicate;
use std::time::Duration;

/// Stable predicate identities and the policy generation that selected them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateOrder {
    /// Predicate identity sequence requested by the policy.
    pub identities: Vec<usize>,
    /// Monotonic order generation used to reject observations from an old order.
    pub epoch: u64,
}

/// Supplies a predicate order at asynchronous reader filter-window boundaries.
/// Implementations must only reorder predicates that are safe to evaluate in a
/// different order. Invalid permutations are ignored by [`super::RowFilter`].
pub trait PredicateOrderPolicy: Send + Sync {
    /// Return stable predicate identities in evaluation order, or `None` to
    /// retain the current order.
    fn next_order(&self, current_order: &[usize], current_epoch: u64) -> Option<PredicateOrder>;

    /// Receive elapsed time for the predicate chain on one filter window.
    fn observe_filter(&self, _elapsed: Duration, _epoch: u64) {}
}

pub(super) fn apply_order(
    predicates: &mut Vec<Box<dyn ArrowPredicate>>,
    current_order: &mut Vec<usize>,
    current_epoch: &mut u64,
    policy: Option<&dyn PredicateOrderPolicy>,
) -> bool {
    let Some(order) = policy.and_then(|policy| policy.next_order(current_order, *current_epoch))
    else {
        return false;
    };
    let len = predicates.len();
    if current_order.len() != len
        || !is_permutation(current_order, len)
        || !is_permutation(&order.identities, len)
    {
        return false;
    }
    let order_changed = reorder_by_identity(predicates, current_order, &order.identities);
    let epoch_changed = *current_epoch != order.epoch;
    *current_epoch = order.epoch;
    order_changed || epoch_changed
}

fn reorder_by_identity<T>(items: &mut [T], current_order: &mut [usize], order: &[usize]) -> bool {
    let len = items.len();
    if order == current_order
        || current_order.len() != len
        || !is_permutation(current_order, len)
        || !is_permutation(order, len)
    {
        return false;
    }

    for target in 0..len {
        if current_order[target] == order[target] {
            continue;
        }
        let relative_source = current_order[target + 1..]
            .iter()
            .position(|identity| *identity == order[target])
            .expect("validated permutations contain the same identities");
        let source = target + 1 + relative_source;
        items.swap(target, source);
        current_order.swap(target, source);
    }
    true
}

fn is_permutation(order: &[usize], len: usize) -> bool {
    order.len() == len
        && order
        .iter()
        .enumerate()
        .all(|(index, identity)| *identity < len && !order[..index].contains(identity))
}

#[cfg(test)]
#[path = "reorder_tests.rs"]
mod tests;
