# pyraft

Partial implementation of the Raft consensus algorithm[^1].

#### Not Implemented
persistence, snapshot

### Run
```sh
make start_cluster
```

### Stop
```sh
make stop_cluster
```

### Scenarios
An up to date node should always be available.
You can find the leader from the __kv_client.py__ output.
You can use `docker-compose stop $peer_name` to kill a leader and force a switch over
with `docker-compose start $peer_name` you can start the peer again and let it join the cluster.

### Usage
Add the `pyraft` package to your `PYTHONPATH` and execute the command from the project directory.

To read a key

```sh
env PYTHONPATH=./pyraft:$PYTHONPATH python ./pyraft/client/kv_client.py -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -get your_key
```

To add a key

```sh
env PYTHONPATH=./pyraft:$PYTHONPATH python ./pyraft/client/kv_client.py -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -put your_key value
```

To remove a key

```sh
env PYTHONPATH=./pyraft:$PYTHONPATH python ./pyraft/client/kv_client.py -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -del your_key 
```

### Example

```sh
# start the cluster, you can find the leader from the output
make start_cluster
```

```sh
# add a new key, from a different terminal, you can find the leader id from the command output
env PYTHONPATH=./pyraft:$PYTHONPATH python ./pyraft/client/kv_client.py -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -put foo bar
# read the new added key
env PYTHONPATH=./pyraft:$PYTHONPATH python ./pyraft/client/kv_client.py -nodes 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -get foo
# kill the leader node
docker-compose stop $peer_name
# add a couple of keys and start the node again, verify your keys
docker-compose stop $peer_name
```

[^1]: https://raft.github.io/

