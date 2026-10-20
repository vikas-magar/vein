use std::collections::HashMap;

use catalog::ColumnStatistics;
use parquet::arrow::arrow_reader::{RowGroupSelection, RowSelection, RowSelector};
use parquet::basic::Encoding;

use super::super::dictionary::{DictionaryPredicate};
use super::super::page::{PageHeaderRowStatistics, PageHeaderStatistics};
use super::prune_dictionary_encoded_pages;

fn page(first_row: usize, row_count: usize, encoding: Encoding) -> PageHeaderRowStatistics {
    PageHeaderRowStatistics {
        first_row,
        row_count,
        encoding,
        statistics: ColumnStatistics::default(),
    }
}

fn predicate() -> DictionaryPredicate {
    DictionaryPredicate {
        index: 0,
        column: "shipmode".to_string(),
        candidates: Vec::new(),
    }
}

fn headers(pages: Vec<PageHeaderRowStatistics>) -> PageHeaderStatistics {
    HashMap::from([(0, HashMap::from([("shipmode".to_string(), pages)]))])
}

fn membership(outcome: Option<bool>) -> HashMap<usize, Vec<Option<bool>>> {
    HashMap::from([(0, vec![outcome])])
}

#[test]
fn mixed_encoding_chunk_skips_only_dictionary_encoded_pages() {
    let pruning = prune_dictionary_encoded_pages(
        vec![RowGroupSelection::new(0, None)],
        &headers(vec![
            page(0, 3, Encoding::RLE_DICTIONARY),
            page(3, 2, Encoding::PLAIN),
            page(5, 5, Encoding::PLAIN_DICTIONARY),
        ]),
        &[predicate()],
        &membership(Some(false)),
    );

    assert_eq!(pruning.dictionary_pages_proven_absent, 2);
    assert_eq!(pruning.rows_skipped, 8);
    assert_eq!(pruning.selections.len(), 1);
    assert_eq!(pruning.selections[0].selection().unwrap().row_count(), 2);
}

#[test]
fn page_dictionary_pruning_intersects_existing_selection() {
    let existing = RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(5)]);
    let pruning = prune_dictionary_encoded_pages(
        vec![RowGroupSelection::new(0, Some(existing))],
        &headers(vec![
            page(0, 3, Encoding::RLE_DICTIONARY),
            page(3, 2, Encoding::PLAIN),
            page(5, 5, Encoding::RLE_DICTIONARY),
        ]),
        &[predicate()],
        &membership(Some(false)),
    );

    assert_eq!(pruning.rows_skipped, 3);
    assert_eq!(pruning.selections[0].selection().unwrap().row_count(), 2);
}

#[test]
fn unknown_or_incomplete_dictionary_evidence_fails_open() {
    let input = vec![RowGroupSelection::new(0, None)];
    for (pages, result) in [
        (vec![page(0, 10, Encoding::RLE_DICTIONARY)], None),
        (
            vec![page(0, 4, Encoding::RLE_DICTIONARY), page(5, 5, Encoding::RLE_DICTIONARY)],
            Some(false),
        ),
        (vec![page(0, 10, Encoding::PLAIN)], Some(false)),
    ] {
        let pruning = prune_dictionary_encoded_pages(
            input.clone(),
            &headers(pages),
            &[predicate()],
            &membership(result),
        );
        assert_eq!(pruning.rows_skipped, 0);
        assert_eq!(pruning.selections[0].selection(), None);
    }
}
