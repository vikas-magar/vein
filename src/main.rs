use binder::testing::MockFunctionRegistry;
use binder::{LogicalBinder, QueryBinder};
use parser::Parser;
use std::sync::Arc;
use vcs::InMemoryCatalog;

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
            let catalog = Arc::new(InMemoryCatalog::new());
            let functions = Arc::new(MockFunctionRegistry::new());

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
