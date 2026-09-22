VEIN_DATA_DIR=./vein_data ./target/debug/cli vcs --bind 127.0.0.1:9090 --warehouse file://./vein_data/vcs-warehouse --cert certs/server.crt --key certs/server.key --raft-id 1 --raft-advertise 127.0.0.1:9090 --raft-state ./vein_data/vcs-raft.json --raft-bootstrap

VEIN_DATA_DIR=./vein_data ./target/debug/cli master --bind 127.0.0.1:8080 --cert certs/server.crt --key certs/server.key --vcs 127.0.0.1:9090

VEIN_DATA_DIR=./vein_data ./target/debug/cli executor --bind 127.0.0.1:8081 --master 127.0.0.1:8080 --cert certs/server.crt --key certs/server.key --vcs 127.0.0.1:9090

VEIN_DATA_DIR=./vein_data VEIN_CATALOG_WAREHOUSE=file://./vein_data/local-catalog ./target/debug/cli local --master-bind 127.0.0.1:8080 --executor-bind 127.0.0.1:8081

./target/debug/vein "FROM input |> WHERE id < 10 |> SELECT *"


./target/debug/vein "FROM input |> WHERE id < 10 |> SELECT *"

./target/debug/vein "FROM './data/input' |> WRITE input"