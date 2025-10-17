# Detock

[![Build Status](https://github.com/ctring/Detock/workflows/Build%20and%20Test/badge.svg)](https://github.com/ctring/Detock/actions)


Detock is a distributed data store designed to be deployed in multiple geographic regions.
Detock employs a deterministic execution framework like [SLOG](https://www.cs.umd.edu/~abadi/papers/1154-Abadi.pdf) to move most of intra- and inter-region coordination out of transactional execution. As a result, it can achieve high throughput, even for high contention workloads.

While SLOG can guarantee low latency for transactions accessing data in a single region, it has to route all multi-region transactions through a global ordering mechanism to avoid deadlocks, incurring additional latency from this global ordering layer on top of normal processing of the transactions. Unlike SLOG, Detock eliminates the global ordering layer completely by using a graph-based concurrency control protocol that deterministically resolves deadlocks without aborting transactions, hence, achieving much lower latency.

This repository contains an experimental implementations of the system used in the paper [Detock: High Performance Multi-region Transactions at Scale](https://doi.org/10.1145/3589293). 

## How to Build

The following guide has been tested on Ubuntu 20.04 with GCC 9.3.0 and CMake 3.16.3. Additional docs are in [the SLOG's wiki](https://github.com/umd-dslam/SLOG/wiki).


First, install the build tools:
```
sudo apt install cmake build-essential pkg-config -y
```

Run the following commands to build the system. The dependencies will be downloaded and built automatically.

```
rm -rf build && mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=release
make -j$(nproc)
```

### Build on TU Delft machines

These building instructions have been tested on the TU Delft st5. Whole process takes ~2 mins when building for the 1st time

```
sudo apt update
sudo apt install cmake build-essential pkg-config -y
rm -rf build && mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=release
make -j$(nproc)
```

To create a Docker image, use the `build_detock.sh` script:

```
bash build_detock.sh
```

## How to Run
<details>

<summary>Run Detock on a single machine</summary>

The following command starts Detock using the example configuration for a single-node cluster.
```
$ build/slog -config examples/single.conf -address /tmp/slog
```

After that, use the client to send a transaction that writes some data.
```
$ build/client txn examples/write.json
...
Transaction ID: 100
Status: COMMITTED
Key set:
[WRITE] 0
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (0, 0)
[WRITE] 1
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[WRITE] 2
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (0, 0)
Type: SINGLE_HOME
Code:
SET 0 Hello
SET 1 World
SET 2 !!!!!
Coordinating server: 0
Involved partitions: 0
Involved regions: 0
```

Send a transaction to copy data from the previous keys to different keys:
```
$ build/client txn examples/copy.json
...
Transaction ID: 200
Status: COMMITTED
Key set:
[WRITE] 3
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (0, 0)
[WRITE] 4
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[WRITE] 5
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (0, 0)
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (0, 0)
Type: SINGLE_HOME
Code:
COPY 0 3
COPY 1 4
COPY 2 5
Coordinating server: 0
Involved partitions: 0
Involved regions: 0
```

Send a transaction to read the written data.
```
$ build/client txn examples/read.json
...
Transaction ID: 300
Status: COMMITTED
Key set:
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (0, 0)
[READ] 3
        Value: Hello
        Metadata: (0, 0)
[READ] 4
        Value: World
        Metadata: (0, 0)
[READ] 5
        Value: !!!!!
        Metadata: (0, 0)
Type: SINGLE_HOME
Code:
GET 0
GET 1
GET 2
GET 3
GET 4
GET 5
Coordinating server: 0
Involved partitions: 0
Involved regions: 0
```

</details>

<details>
<summary>Run Detock on a cluster</summary>

The following guide shows how to manually run Detock on a cluster of multiple machines. This can be time-consuming when the number of machines is large so you should use the [Admin tool](https://github.com/umd-dslam/SLOG/wiki/Using-the-Admin-tool) instead.

In this example, we start Detock on a cluster using the configuration in `examples/cluster.conf`. You need to change the IP addresses in this file to match with the addresses of your machines. You can add more machines by increasing either the number of regions or the number of partitions in a region. The number of machines in a region must be the same across all regions and equal to `num_partitions`.

After cloning and building Detock, run the following command on each machine.
```
$ build/slog -config examples/cluster.conf -address <ip-address> -region <region-id> -partition <partition-id>
```

For example, assuming the machine configuration is
```
regions: {
    addresses: "192.168.2.11",
    addresses: "192.168.2.12",
}
regions: {
    addresses: "192.168.2.13",
    addresses: "192.168.2.14",
}
```

The commands to be run for the machines respectively from top to bottom are:
```
$ build/slog -config examples/cluster.conf -address 192.168.2.11 
``` 

```
$ build/slog -config examples/cluster.conf -address 192.168.2.12 
``` 

```
$ build/slog -config examples/cluster.conf -address 192.168.2.13 
``` 

```
$ build/slog -config examples/cluster.conf -address 192.168.2.14
```

Use the client to send a write transaction to a machine in the cluster. If you changed the `port` option in the configuration file, you need to use the `--port` argument in the command to match with the new port.
```
$ build/client txn examples/write.json --host 131.180.125.40
...
Transaction ID: 100
Status: COMMITTED
Key set:
[WRITE] 1
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[WRITE] 0
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (0, 0)
[WRITE] 2
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (1, 0)
Type: MULTI_HOME_OR_LOCK_ONLY
Code:
SET 0 Hello
SET 1 World
SET 2 !!!!!
Coordinating server: 0
Involved partitions: 0 1
Involved regions: 0 1
```

Send a copy transaction that copies the values from the written keys to new keys.
```
$ build/client txn examples/copy.json --host 192.168.2.11
...
Transaction ID: 200
Status: COMMITTED
Key set:
[WRITE] 3
        Value: aaaaaaaaaa
        New value: Hello
        Metadata: (1, 0)
[WRITE] 5
        Value: aaaaaaaaaa
        New value: !!!!!
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[WRITE] 4
        Value: aaaaaaaaaa
        New value: World
        Metadata: (0, 0)
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (1, 0)
Type: MULTI_HOME_OR_LOCK_ONLY
Code:
COPY 0 3
COPY 1 4
COPY 2 5
Coordinating server: 0
Involved partitions: 0 1
Involved regions: 0 1
```

Send a read transaction to read the written data. This time, we read from a different region to demonstrate that the data has been replicated.
```
$ build/client txn examples/read.json --host 192.168.2.13
...
Transaction ID: 102
Status: COMMITTED
Key set:
[READ] 0
        Value: Hello
        Metadata: (0, 0)
[READ] 2
        Value: !!!!!
        Metadata: (1, 0)
[READ] 4
        Value: World
        Metadata: (0, 0)
[READ] 1
        Value: World
        Metadata: (0, 0)
[READ] 3
        Value: Hello
        Metadata: (1, 0)
[READ] 5
        Value: !!!!!
        Metadata: (0, 0)
Type: MULTI_HOME_OR_LOCK_ONLY
Code:
GET 0
GET 1
GET 2
GET 3
GET 4
GET 5
Coordinating server: 2
Involved partitions: 0 1
Involved regions: 0 1
```
</details>

NOTE: If your deployment involves network link that is over 100ms round-trip time, for better performance, run the following commands on each node to increase the network buffer:

```
sudo sysctl -w net.core.rmem_max=10485760
sudo sysctl -w net.core.wmem_max=10485760
```
and set the following fields in the config:
```
broker_rcvbuf: 10485760
long_sender_sndbuf: 10485760
```

## Acknowledgements

This work is part of project number 19708 of the Vidi research program, which is partly financed by the Dutch Research Council (NWO).

## Directory Structure

```
|- aws/
|--- Scripts and config files for spawning an AWS cluster to run the final experiments.
|- build/
|--- Makefiles, dependencies, compilier settings. Should mostly work as is.
|- common/
|--- Various I/O functionality, sharding, batching, logging, thread utils
|- connection/
|--- Broker, logging, polling, sending functionality.
|- examples/
|--- Example lists of transactions (having their read/write sets, operations, and cluster configs, e.g., )
|- execution/
|--- Detailed code for how the individual txn types execute. (Also includes OrderStatus, Delivery, StockLevel)
|- experiments/
|--- Configs for the individual experiments. For us tpcc is most important.
| |- cockroach
| |--- Configs for Cockrach DB comaprison experiments (Fig. 13)
| |- tpcc
| |--- Configs for TPC-C experiments (Fig. 10, 11, 12?)
| |- ycsb
| |--- Configs for YCSB-T experiment (Fig. 6 - 9)
|- latex_generators/
|--- Python scripts that create some of the latex code for certain tables in the paper Overleaf.
|- module/
|--- Actuall logic of the system (i.e. the sequencer, scheduler, orderer, forwarder, etc.)
|- paxos/
|--- Paxos logic implementation. Used for (asyncronous) replication?
|- plots/
|--- Scripts for extracting results from experiments scripts and for generating the plots.
|- proto/
|--- Protobuf message specifications for txn, config, internal, etc. objects
|- service/
|--- Service on the client side for the compared systems
|- storage/
|--- Implements the storage layer, initializes metadata, loads tables (tpcc_partitioning for TPC-C, simple_partitioning for YCSB-T microbenchmark, simple_partitioning2 for Cockroach experiments again using YCSB-T)
|- test/
|--- Test for various parts of the system. Also isolates sequencer/scheduler pretty well.
|- tools/
|--- Helper scripts to run expeiments. 'tools/run_experiment.py' is the entry point to running all experiments.
|- workload/
|--- Other setup for TPC-C, cockroachDB, remastering experiments. What's the relation to the 'storage' dir?
|- 
|- 
```
