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

use crate::arrow::arrow_reader::RowSelection;

/// Tracks a row group's ordered vector windows and their accumulated filter result.
#[derive(Debug)]
pub(super) struct FilterWindowProgress {
    row_count: usize,
    window_size: usize,
    next_row: usize,
    input_selection: Option<RowSelection>,
    completed: RowSelection,
}

impl FilterWindowProgress {
    pub(super) fn new(
        row_count: usize,
        window_size: usize,
        input_selection: Option<RowSelection>,
    ) -> Self {
        let completed = RowSelection::from_consecutive_ranges(
            std::iter::empty::<std::ops::Range<usize>>(),
            row_count,
        );
        Self {
            row_count,
            window_size: window_size.max(1),
            next_row: 0,
            input_selection,
            completed,
        }
    }

    /// Return the next non-empty input window, preserving absolute row positions.
    pub(super) fn next_window(&mut self) -> Option<RowSelection> {
        while self.next_row < self.row_count {
            let start = self.next_row;
            let end = start.saturating_add(self.window_size).min(self.row_count);
            self.next_row = end;

            let window = RowSelection::from_consecutive_ranges(
                std::iter::once(start..end),
                self.row_count,
            );
            let selection = self
                .input_selection
                .as_ref()
                .map_or(window.clone(), |input| input.intersection(&window));
            if selection.selects_any() {
                return Some(selection);
            }
        }
        None
    }

    pub(super) fn record_completed_window(&mut self, selection: &RowSelection) {
        debug_assert_eq!(selection.total_row_count(), self.row_count);
        self.completed = self.completed.union(selection);
    }

    pub(super) fn selected_rows(&self) -> usize {
        self.completed.row_count()
    }

    pub(super) fn output_selection(&self) -> RowSelection {
        self.completed.clone()
    }

    pub(super) fn stop_after_current_window(&mut self) {
        self.next_row = self.row_count;
    }
}

#[cfg(test)]
#[path = "filter_window_tests.rs"]
mod tests;
