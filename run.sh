VEIN_DATA_DIR=./vein_data ./target/debug/cli vcs --bind 127.0.0.1:9090 --warehouse file://./vein_data/vcs-warehouse --cert certs/server.crt --key certs/server.key --raft-id 1 --raft-advertise 127.0.0.1:9090 --raft-state ./vein_data/vcs-raft.json --raft-bootstrap

VEIN_DATA_DIR=./vein_data ./target/debug/cli master --bind 127.0.0.1:8080 --cert certs/server.crt --key certs/server.key --vcs 127.0.0.1:9090

VEIN_DATA_DIR=./vein_data ./target/debug/cli executor --bind 127.0.0.1:8081 --master 127.0.0.1:8080 --cert certs/server.crt --key certs/server.key --vcs 127.0.0.1:9090

./target/debug/vein "FROM './data/input' |> WRITE input"

./target/debug/vein "FROM input AS a |> JOIN input AS b ON a.id = b.id |> WHERE a.id < 10 |> SELECT a.id, a.random_float, a.random_int"