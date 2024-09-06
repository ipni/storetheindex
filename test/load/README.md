# Load tests

## Start a daemon
To run load tests over `storetheindex` we need to first initialize and run the indexer daemon:

### Initialize and start a `storetheindex` daemon:
```
export STORETHEINDEX_PATH=/tmp/sti_test
./storetheindex init
./storetheindex daemon --cachesize <cache_size> --dir <data-dir>
```

### Initialize and start an `index-provider` daemon
```
./provider init
edit ~/.index-provider/config
```

Configure the provider to announce to the indexer.
```json
  "DirectAnnounce": {
    "URLs": ["http://127.0.0.1:3001"]
  }
```

Start the daemon
```
./provider daemon
```

### Import sample car files into the provider, which will then go to the indexer indexer:
```
./provider import car -i testdata/sample-v1.car 
``` 

## Run the test
We can then start the load tests client. The load test script has `locust` and `numpy` as dependencies. Be sure that you have `python3` installed
and that you run `pip install locust numpy`.

- A test run with 4 workers and a master client can be easily run through the `./run.sh` script. Be sure to generate the test data as described above. The `run.sh` script starts the client workers. To configure the test run and visualize the results, go to locust UI at `http://localhost:8089`.
- Locust can also be run from the CLI. To stress test a single endpoint, use [this tool](https://github.com/rakyll/hey) as follows:
```
./hey_linux_amd64 -m GET -z <test-duration> -c <concurrent_clients> <endpoint>/cid/bafkreigxvijvpvmt7xnk2nxzudha22jf7fawi2vbjpsnh7cejagquq6z4y

# Example
./hey_linux_amd64 -m GET -z 30s -c 10000 http://127.0.0.1:3000/cid/bafkreigxvijvpvmt7xnk2nxzudha22jf7fawi2vbjpsnh7cejagquq6z4y
```

**_File Descriptor Limit_**

When trying to run a huge load over the node, the local client may reach the OS file descriptor limit (even if this is set to the maximum limit allowed). To send more load to the node, the load generation needs to be distributed over multiple machines.
