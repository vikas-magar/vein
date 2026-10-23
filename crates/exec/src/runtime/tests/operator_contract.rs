use super::*;

#[test]
fn physical_join_kinds_map_to_their_executor_types() {
    let mappings = [
        (JoinKind::Inner, JoinType::Inner),
        (JoinKind::Left, JoinType::Left),
        (JoinKind::Right, JoinType::Right),
        (JoinKind::Full, JoinType::Full),
        (JoinKind::Semi, JoinType::Semi),
        (JoinKind::Anti, JoinType::Anti),
        (JoinKind::RightSemi, JoinType::RightSemi),
        (JoinKind::RightAnti, JoinType::RightAnti),
        (JoinKind::Single, JoinType::Single),
        (JoinKind::Cross, JoinType::Cross),
    ];

    for (physical, executor) in mappings {
        assert_eq!(
            join_type(physical).expect("join flavor is supported"),
            executor
        );
    }
}
