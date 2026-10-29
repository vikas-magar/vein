use super::*;

#[test]
fn shared_error_adapters_retain_identity_category_and_original_causes() {
    let original = Arc::new(VeinError::io("operation failed", std::io::Error::other("source cause")));
    let adapted = VeinError::from_shared(Arc::clone(&original));
    assert_eq!(adapted.category(), original.category());
    assert!(adapted.causes().any(|cause| cause.to_string().contains("source cause")));
    assert!(Arc::ptr_eq(&adapted.into_shared(), &original));
}

#[test]
fn shared_cancellation_keeps_its_direct_variant() {
    assert!(matches!(VeinError::from_shared(Arc::new(VeinError::cancelled())), VeinError::Cancelled));
}
