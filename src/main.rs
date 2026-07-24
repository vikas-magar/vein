use parser::PlanBuilder;
use parser::Parser;

fn main() {
    let query = "FROM  users |> WHERE age > 18 |> SELECT id, name";
    println!("Parsing query: {}", query);

    let mut parser = Parser::new(query);

    match parser.parse_query() {
        Ok(ast) => {
            println!("{:?}", ast);
            match PlanBuilder::build(&ast) {
                Ok(plan) => println!("{:?}", plan),
                Err(e) => println!("{:?}", e),
            }
        }
        Err(e) => {
            println!("{:?}", e);
        }
    }
}
