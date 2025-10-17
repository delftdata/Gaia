# Gaia

Gaia is a novel benchmarking framework for geo-distributed OLTP databases. It provides a set of six scenarios and four metrics, to obtain a comprehensive understanding of the capabilities of the systems under test.

Gaia is built on top of the Detock codebase as that already provides abstractions for four SOTA systems under the same codebase implementation.

## Running Gaia

To run Gaia, first compile using the instructions in [README_Detock.md](README_Detock.md). After compilation, you can spin up a database service. Instructions to spin up the database service are provided in [tools/README.md](tools/README.md). Finally you can run experiments, and extract results, and plot the results using those instructions.

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
