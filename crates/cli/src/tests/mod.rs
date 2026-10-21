use super::{QUERY_USAGE, query_argument};
use common::VeinError;

#[test]
fn empty_query_argument_returns_central_input_error() {
    assert!(matches!(
        query_argument(String::new()),
        Err(VeinError::InvalidInput(message)) if message == QUERY_USAGE
    ));
}

#[test]
fn nonempty_query_argument_is_preserved() {
    let query = "FROM part |> SELECT *";
    assert_eq!(query_argument(query.to_owned()).unwrap(), query);
}
