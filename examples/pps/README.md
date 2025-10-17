# Tables Generation
For tables generation, we have the following initial observation:
 - The numbers of products, parts, and suppliers are taken as argument from the configuration files (e.g., `single.conf`).
 - The number of parts per product and the number of parts per supplier are fixed, and given by the constants from `exeuction/pps/constants`.
 - The names of the products, parts, and suppliers are randomly generated (they are irrelevant for the transactions types we consider).

The main transaction type of our workload is `OrderProduct`. Since it is a dependent transaction, we are limited when it comes to configuring it with different %MH and %MP (in other words, we cannot set beforehand which parts will be accessed, and thus we cannot anticipate which partitions and regions will be accessed). To overcome this issue, we assume that the products are part of one of the following categories:
 - Products that contain parts from the same partition and the same region;
 - Products that contain parts from the same region, but different partitions;
 - Products that contain parts from the same partition, but different regions;
 - Products that contain parts from different regions, and different partitions.

We will consider a proportion of 0.25-0.25-0.25-0.25 of these categories.


# Toy Examples

## 1 Region, 1 Partition
Start the server with the commad:
```bash
build/slog --config examples/pps/single.conf --address /tmp/slog
```
The client can use the commands:
```bash
build/client txn examples/pps/toy_transactions/get_product.json
build/client txn examples/pps/toy_transactions/get_part.json
```

## 1 Region, 2 Partitions
For now, for the running the experiments locally, we have created a custom docker network (bridge type), and three containers (2 for the servers, and 1 for the client), that we have connected to the network.

Start the servers with the commands:
```bash
build/slog --config examples/pps/one-region.conf --address 172.18.0.2
```
```bash
build/slog --config examples/pps/one-region.conf --address 172.18.0.3
```
The client can use the commands:
```bash
build/client txn examples/pps/toy_transactions/get_product.json --host 172.18.0.2
build/client txn examples/pps/toy_transactions/get_part.json --host 172.18.0.2
```

## 2 Regions, 2 Partitions
Start the servers with the commands:
```bash
build/slog --config examples/pps/two-regions.conf --address 172.18.0.2
```
```bash
build/slog --config examples/pps/two-regions.conf --address 172.18.0.3
```
```bash
build/slog --config examples/pps/two-regions.conf --address 172.18.0.4
```
```bash
build/slog --config examples/pps/two-regions.conf --address 172.18.0.5
```
The client can use the following command for `product_id` being 3 in the json file and get the corresponding answer:
```bash
$ build/client txn examples/pps/toy_transactions/get_product.json --host 172.18.0.2
```
```
I0430 23:31:48.940928  4651 client.cpp:377] Connecting to tcp://172.18.0.2:2021
I0430 23:31:48.941128  4651 client.cpp:54] Parsed JSON: {"workload":"pps","txn_type":"get_product","arguments":{"product_id":3}}
I0430 23:31:48.941170  4651 client.cpp:130] Request size in bytes: 36
I0430 23:31:48.960427  4651 client.cpp:143] Response size in bytes: 96
Transaction ID: 12884901888
Status: COMMITTED
Key set:
[READ] \3\0\0\0\1\1
        Value: iUXPvDzOUZ
        Metadata: (1, 0)
Type: SINGLE_HOME
Code:
get_product 3
Coordinating server: 0
Involved partitions: 0
Involved regions: 1
```
We observe that id 3 is part of the partition 0 and region 1. The sharding and mastering is done using the table:
| Partition / Home | 0 | 1 | 0 | 1 | 0 | 1 | 0 | 1 | ... |
|------------------|---|---|---|---|---|---|---|---|-----|
| **0**            | 1 | 3 | 5 | 7 | 9 |11 |13 |15 | ... |
| **1**            | 2 | 4 | 6 | 8 |10 |12 |14 |16 | ... |

# Benchmark

## 1 Region, 1 Partition

Server:
```bash
build/slog --config examples/pps/single.conf --address /tmp/slog
```

Client:
```bash
build/benchmark --wl pps --clients 10 --txns 0 --generators 3 --config examples/pps/single.conf --duration 10 --params "mh=50,mp=30,mix=44:44:4:4:4" --out_dir /tmp/slog --seed 42
```

## Cluster
### Running a Complete Scenario
The complete instructions about setting up the Python environment and all the customizable parameters of the scripts can be found [here](../../tools/README.md).

In case of PPS, here is an example of the step-by-step instructions followed to generate the final results for Detock in the baseline scenario. For the other databases and scenarios, the workflow is identical with small modifications: change the configuration file, change the database binary, change the database name, and tweak the number of clients based on the scalability scenario (the scalability scenario doesn't require a predefined number of clients, and will give use the appropriate number of clients for each system, such that it won't be overwhelmed or undersaturated).

1. Spin up the cluster using the command: `python3.8 tools/admin.py start --image emihai1/detock:latest --user emihai --bin slog examples/pps/tu_cluster_detock.conf`.
2. Verify that all the server machines are running as intended using the command: `python3.8 tools/admin.py status --image emihai1/detock:latest --user emihai examples/pps/tu_cluster_detock.conf`.
3. Run the scenario with the command: `python3.8 tools/run_config_on_remote.py -s baseline -w pps -c examples/pps/tu_cluster_detock.conf -i emihai1/detock:latest -d 60 -u emihai -m st1 --clients 1000 --generators 1 -db Detock --trial_tag final`. The results should be generated in the folder `data/pps/baseline/final`.
4. Copy the results to the `plots/` folder to process them using the command: `mkdir -p plots/raw_data/pps/baseline && cp -r data/pps/baseline/final/. plots/raw_data/pps/baseline`.
5. Create the plots using the command: `python3.8 plots/extract_exp_results.py -s baseline -w pps -sa 1`.