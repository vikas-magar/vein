use super::super::*;

#[test]
fn inverse_join_kinds_preserve_the_original_semantics_after_two_swaps() {
    for kind in [JoinKind::Inner, JoinKind::Left, JoinKind::Right, JoinKind::Full,
        JoinKind::Semi, JoinKind::Anti, JoinKind::RightSemi, JoinKind::RightAnti, JoinKind::Cross] {
        assert_eq!(kind.inverse().unwrap().inverse(), Some(kind));
    }
    for kind in [JoinKind::Single, JoinKind::NullAwareAnti] { assert_eq!(kind.inverse(), None); }
}

#[test]
fn runtime_probe_filtering_never_removes_preserved_probe_rows() {
    for kind in [JoinKind::Left, JoinKind::Full, JoinKind::Anti, JoinKind::NullAwareAnti, JoinKind::Single, JoinKind::Cross] {
        assert!(!kind.allows_probe_filtering(), "{kind:?}");
    }
    for kind in [JoinKind::Inner, JoinKind::Semi, JoinKind::RightSemi, JoinKind::RightAnti] {
        assert!(kind.allows_probe_filtering(), "{kind:?}");
    }
}
