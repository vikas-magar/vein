VEIN_DATA_DIR=./vein_data ./target/debug/cli vcs --bind 127.0.0.1:9090 --warehouse file://./vein_data/vcs-warehouse --cert certs/server.crt --key certs/server.key --raft-id 1 --raft-advertise 127.0.0.1:9090 --raft-state ./vein_data/vcs-raft.json --raft-bootstrap

VEIN_DATA_DIR=./vein_data ./target/debug/cli master --bind 127.0.0.1:8080 --cert certs/server.crt --key certs/server.key --vcs 127.0.0.1:9090

VEIN_DATA_DIR=./vein_data ./target/debug/cli executor --bind 127.0.0.1:8081 --master 127.0.0.1:8080 --cert certs/server.crt --key certs/server.key --vcs 127.0.0.1:9090

./target/debug/vein "FROM './data/input' |> WRITE input"

./target/debug/vein "FROM input AS a |> JOIN input AS b ON a.id = b.id |> WHERE a.id < 10 |> SELECT a.id, a.random_float, a.random_int"

./target/debug/vein "FROM input AS a |> WHERE a.id < 10 |> SELECT a.id, a.random_float, a.random_int"

./target/debug/vein "FROM './data/input2' |> WRITE input2"

./target/debug/vein "FROM './data/input2' |> select *"
./target/debug/vein "EXPLAIN FROM './data/input2' |> select *"



Here are some Pipe-SQL test queries you can run against ./data/input2:

  ./target/debug/vein "FROM input2 |> LIMIT 10 |> SELECT *"

  Filter flights by year and project a few columns:

  ./target/debug/vein "FROM input2 |> LIMIT 10 |> WHERE Year = 2022 |> SELECT FlightDate, Airline, Origin, Dest, Distance"

  Filter delayed departures:

  ./target/debug/vein "FROM input2 |> WHERE DepDelayMinutes > 60 |> SELECT FlightDate, Airline, Origin, Dest, DepDelayMinutes |> LIMIT 100"

  Count cancelled flights:

  ./target/debug/vein "FROM input2 |> WHERE Cancelled = true |> SELECT COUNT(*)"

  Summarize flights by airline:

  ./target/debug/vein "FROM input2 |> GROUP BY Airline |> SELECT Airline, COUNT(*)"

  Summarize average departure delay by origin:

  ./target/debug/vein "FROM input2 |> GROUP BY Origin |> SELECT Origin, AVG(DepDelayMinutes), COUNT(*)"

  Find the longest flights:

  ./target/debug/vein "FROM input2 |> SELECT FlightDate, Airline, Origin, Dest, Distance |> LIMIT 20"

  Test a self-join on airport IDs:

  ./target/debug/vein "FROM input2 AS a |> JOIN input2 AS b ON a.OriginAirportID = b.DestAirportID |> WHERE a.Year = 2022 |> SELECT a.Origin, b.Dest"
