# Load tests

### Start a daemon
To run load tests over `storetheindex` we need to first run the node in the infrastructure:
- Start a `storetheindex` daemon:
```
./storetheindex daemon --storage <persistence> -e 0.0.0.0:3000 --cachesize <cache_size> --dir <data-dir>
```
- Import cid data into the indexer:
```
./storetheindex import cidlist --dir ./utils/load/cids.data --provider 12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA --metadata <metadata> -e 127.0.0.1:3000
``` 

### Run the test
We can then start the load tests client. The load test script has `locust` and `numpy` as dependencies. Be sure that you have `python3` installed
and that you `pip install locust numpy`.

- A test run with 4 workers and a master client can be easily run through the `./run.sh` script. This script starts the client workers. To configure the
test run and visualize the results go to locust UI at `http://localhost:8089`.
- Locust can also be run from the CLI, and to stress test a single endpoint we can also use [this tool](https://github.com/rakyll/hey), which can be run
using:
```
./hey_linux_amd64 -m GET -z <test-duration> -c <concurrent_clients> <endpoint>/cid/bafkreigxvijvpvmt7xnk2nxzudha22jf7fawi2vbjpsnh7cejagquq6z4y

# Example
./hey_linux_amd64 -m GET -z 30s -c 10000 http://18.169.134.123:3000/cid/bafkreigxvijvpvmt7xnk2nxzudha22jf7fawi2vbjpsnh7cejagquq6z4y
```

### File Descriptor Limit
When trying to run a huge load over the node, the local client may reach the OS file descriptor limit (even if this is set to the maximum limit allowed).
To send more load to the node the load generation needs to be distributed in different machines.

