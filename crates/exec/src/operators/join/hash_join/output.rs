use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};
use common::Result;
use error::VeinError;

use super::super::hash_table::HashTable;
use super::super::rows::OutputRow;

/// Builds bounded join batches from rows referencing any retained build chunk.
pub(super) struct JoinOutputComposer {
    columns: Vec<ViewColumnRef>,
    build_projection: Vec<usize>,
    build_types: Vec<DataType>,
    invalid_build_column: Option<usize>,
    build_views: Vec<DataChunkView>,
    build_positions: HashMap<u32, usize>,
    source_rows: Vec<(usize, usize)>,
    probe_rows: Vec<Option<usize>>,
    build_rows: Vec<Option<usize>>,
}

impl JoinOutputComposer {
    pub(super) fn new(columns: &[ViewColumnRef], build_types: &[DataType]) -> Self {
        let mut build_projection = Vec::new();
        let mut projected_types = Vec::new();
        let mut projected_positions = HashMap::new();
        let mut output_columns = Vec::with_capacity(columns.len());
        let mut invalid_build_column = None;

        for column in columns {
            if column.input != 1 {
                output_columns.push(*column);
                continue;
            }
            let projected = *projected_positions.entry(column.column).or_insert_with(|| {
                let projected = build_projection.len();
                build_projection.push(column.column);
                if let Some(data_type) = build_types.get(column.column) {
                    projected_types.push(data_type.clone());
                } else {
                    invalid_build_column = Some(column.column);
                }
                projected
            });
            output_columns.push(ViewColumnRef {
                input: 1,
                column: projected,
            });
        }

        Self {
            columns: output_columns,
            build_projection,
            build_types: projected_types,
            invalid_build_column,
            build_views: Vec::new(),
            build_positions: HashMap::new(),
            source_rows: Vec::new(),
            probe_rows: Vec::new(),
            build_rows: Vec::new(),
        }
    }

    pub(super) fn compose(
        &mut self,
        table: &HashTable,
        probe: Option<&DataChunkView>,
        probe_types: &[DataType],
        rows: &[OutputRow],
    ) -> Result<DataChunkView> {
        if let Some(column) = self.invalid_build_column {
            return Err(VeinError::internal(format!(
                "view output column references missing column {column} in input 1"
            )));
        }
        self.build_views.clear();
        self.build_positions.clear();
        self.source_rows.clear();

        for row in rows {
            if let Some((chunk, _)) = row.build {
                if !self.build_positions.contains_key(&chunk) {
                    let position = self.build_views.len();
                    self.build_positions.insert(chunk, position);
                    self.build_views
                        .push(table.build_chunk(chunk).project(&self.build_projection)?);
                }
            }
        }
        let null_build = rows.iter().any(|row| row.build.is_none());
        let null_build_position = if null_build {
            let position = self.build_views.len();
            self.build_views
                .push(DataChunkView::nulls(&self.build_types, 1)?);
            Some(position)
        } else {
            None
        };

        for row in rows {
            match row.build {
                Some((chunk, source_row)) => self.source_rows.push((
                    *self.build_positions.get(&chunk).ok_or_else(|| {
                        VeinError::internal("join output build chunk mapping disappeared")
                    })?,
                    source_row as usize,
                )),
                None => self.source_rows.push((
                    null_build_position.ok_or_else(|| {
                        VeinError::internal("join output is missing its null build mapping")
                    })?,
                    0,
                )),
            }
        }
        let build_inputs = self.build_views.iter().collect::<Vec<_>>();
        let build = DataChunkView::compose_rows(
            &build_inputs,
            &self.source_rows,
            self.build_projection.len(),
        )?;

        let missing_probe = probe.is_none();
        let null_probe;
        let probe = match probe {
            Some(probe) => probe,
            None => {
                null_probe = DataChunkView::nulls(probe_types, rows.len())?;
                &null_probe
            }
        };
        self.probe_rows.clear();
        if missing_probe {
            self.probe_rows.extend((0..rows.len()).map(Some));
        } else {
            self.probe_rows
                .extend(rows.iter().map(|row| row.probe.map(|row| row as usize)));
        }
        self.build_rows.clear();
        self.build_rows.extend((0..rows.len()).map(Some));
        DataChunkView::combine(
            &[
                ViewInputRows {
                    view: probe,
                    rows: &self.probe_rows,
                },
                ViewInputRows {
                    view: &build,
                    rows: &self.build_rows,
                },
            ],
            &self.columns,
        )
    }
}
