use parser::PlanBuilder;
use parser::Parser;

fn main() {
    let query = "FROM table1 |> SELECT c1, c2, c3 |> AGGREGATE sum(c1) AS sumc1, sum(c2) AS sumc2 GROUP BY c1, c2 |> WRITE 's3://target'";
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
