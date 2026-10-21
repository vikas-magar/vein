//! Predicate extraction for conservative dictionary pruning.

use catalog::StatisticsPredicate;

use super::DictionaryPredicate;

/// Extracts equality and same-column `IN` conditions from a conjunction.
///
/// Mixed `OR` branches cannot prove dictionary absence and are ignored.
pub fn dictionary_predicates(predicates: &[StatisticsPredicate]) -> Vec<DictionaryPredicate> {
    fn collect(predicate: &StatisticsPredicate, output: &mut Vec<DictionaryPredicate>) {
        match predicate {
            StatisticsPredicate::And(children) => {
                for child in children {
                    collect(child, output);
                }
            }
            StatisticsPredicate::Comparison {
                column,
                comparison: catalog::StatisticsComparison::Equal,
                literal,
            } => output.push(DictionaryPredicate {
                index: output.len(),
                column: column.clone(),
                candidates: vec![literal.clone()],
            }),
            StatisticsPredicate::Or(children) => collect_same_column_or(children, output),
            _ => {}
        }
    }

    let mut output = Vec::new();
    for predicate in predicates {
        collect(predicate, &mut output);
    }
    output
}

fn collect_same_column_or(children: &[StatisticsPredicate], output: &mut Vec<DictionaryPredicate>) {
    let mut column: Option<&str> = None;
    let mut candidates = Vec::with_capacity(children.len());
    for child in children {
        let StatisticsPredicate::Comparison {
            column: child_column,
            comparison: catalog::StatisticsComparison::Equal,
            literal,
        } = child
        else {
            return;
        };
        if column.is_some_and(|column| column != child_column) {
            return;
        }
        column = Some(child_column);
        candidates.push(literal.clone());
    }
    if let Some(column) = column {
        output.push(DictionaryPredicate {
            index: output.len(),
            column: column.to_string(),
            candidates,
        });
    }
}
