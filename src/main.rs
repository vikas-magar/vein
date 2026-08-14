
use parser::Parser;
use planner::binder::pipe::query::QueryBinder;
use planner::testing::{InMemoryVcsCatalog, MockFunctionRegistry};
use planner::traits::LogicalBinder;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let query = "FROM users |> SELECT id, name, age";
    println!("Parsing query: {}", query);

    let mut parser = Parser::new(query);

    match parser.parse_query() {
        Ok(ast) => {
            println!("--- AST ---");
            println!("{:#?}", ast);

            // 1. Setup the dummy catalog & functions (so the binder has something to validate against)
            let catalog = Arc::new(InMemoryVcsCatalog::new());
            let functions = Arc::new(MockFunctionRegistry);

            // 2. Instantiate the Binder
            let binder = QueryBinder::new(catalog, functions);

            // 3. Bind the AST into a LogicalPlan
            match binder.bind(&ast).await {
                Ok(logical_plan) => {
                    println!("--- LOGICAL PLAN ---");
                    println!("{:#?}", logical_plan);
                }
                Err(e) => {
                    println!("Binder Error: {:?}", e);
                }
            }
        }
        Err(e) => {
            println!("Parser Error: {:?}", e);
        }
    }
}