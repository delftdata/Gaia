# MovR Benchmark Instructions

## General Information

MovR models a simplified workflow of a vehicle-sharing company such as Uber or Lyft. Registered users can arrange transportation between various cities supported by the system and vehicle owners generate revenue based on rides completed.

### Schema

The schema features the following tables:

* *users*: stores basic user information.
* *vehicles*: keeps track of registered vehicles and vehicle owner.
* *rides*: stores past and ongoing rides (which users uses which vehcile to go where).
* *vehicle_location_histories*: stores periodic location data of vehicles during a ride.
* *promo_codes*: stores all global promotional codes available to users.
* *user_promo_codes*: keeps track of which promotional code is applied by which user and how many times the code has been applied.

![MovR Schema](assets/movr-schema.png)

### Transactions

The benchmark generates a mix 6 transaction types during execution:

* View Vehicles (30\%): the user checks for the nearest 25 available vehicles. Performs single-home read operations for vehicles in the same city.
* Add User (5\%): the user signs up for the service. Performs single-home write operations to the city where the user is based.
* Add Vehicle (5\%): the user registers their vehicle to the service. Can perform multi-home write operations if the user registers the vehicle to a different city.
* Start Ride (15\%): the user reserves a vehicle and starts their journey. Performs read, write, and update operations. Can be multi-home if the user and vehicle are registered to different cities.
* Track Location (30\%): the vehicle's location is constantly written to the system. The transaction is single home because the write happens to only one city at a time.
* End Ride (15\%): the user reaches their destination and releases the vehicle. Can be multi-home if the ride spanned cities or if the user and the vehicle are registered to different cities.

### Partitioning

Since most tables have 'city' as part of their composite keys, we partition the tables based on this attribute. Cities in the system are tracked using a 3-dimensional array *city_index*: partitions -> regions -> cities. Modulo based mapping is used to evenly distribute the cities. Example city mapping with 8 cities distributes over 2 partitions and 2 regions:


|             | Region 0       | Region 1       |
| ------------- | ---------------- | ---------------- |
| Partition 0 | city_0, city_4 | city_2, city_6 |
| Partition 1 | city_1, city_5 | city_3, city_7 |

### Workload Arguments

* *mh*: the probability (as a percentage) for an eligible transaction to become multi-home (default is 10%)
* *mp*: the probability (as a percentage) for an eligible transaction to become multi-partiton (default is 10%)
* *txn-mix*: colon-separated percentages for a MovR transaction type being generated (default is 30:5:5:15:30:15)
* *reg-mix*: colon-separated percentages for a MovR transaction originating from a specific region (default is uniform)
* *skew*: the skew factor used to create hot records (default is 0 which is uniform record access)
* *sunflower-max*: the maximum percentage of transactions originating from peak region (default is 40%)
* *sunflower-falloff*: the transaction activity falloff (from 0.0 to 1.0) from peak region to neighbouring regions (default is 0.0)
* *sunflower-cycles*: the number of times a region experiences peak activity during the experiment's duration (default is 1)

## How to Run Locally

1. Build the Docker image (from the root directory)

   ```bash
   docker build -t detock-movr .
   ```
2. Setup Docker network

   ```bash
   docker network create \
     --driver=bridge \
     --subnet=172.18.0.0/16 \
     --gateway=172.18.0.1 \
     detock-net
   ```
3. Setup configuration file
   Template configuration files for each databae system (Detock, SLOG, Calvin, and Janus) can found in `examples/movr`. The next steps make use of the `examples/movr/movr-2-regions.conf` configuration which is for local use. Note: when running the latency decomposition scenario, make sure that the configuration file includes the following: `enabled_events: ALL` (use the configuration files in examples/movr/lat_breakdown).
4. Start the containers

   ```bash
   docker run -dit --name detock-r1p1 --network detock-net --ip 172.18.0.2 \
     -v $(pwd):/detock detock-movr \
     /detock/build/slog -config /detock/examples/movr/movr-2-regions.conf -address 172.18.0.2
   ```

   ```bash
   docker run -dit --name detock-r1p2 --network detock-net --ip 172.18.0.3 \
     -v $(pwd):/detock detock-movr \
     /detock/build/slog -config /detock/examples/movr/movr-2-regions.conf -address 172.18.0.3
   ```

   ```bash
   docker run -dit --name detock-r2p1 --network detock-net --ip 172.18.0.4 \
     -v $(pwd):/detock detock-movr \
     /detock/build/slog -config /detock/examples/movr/movr-2-regions.conf -address 172.18.0.4
   ```

   ```bash
   docker run -dit --name detock-r2p2 --network detock-net --ip 172.18.0.5 \
     -v $(pwd):/detock detock-movr \
     /detock/build/slog -config /detock/examples/movr/movr-2-regions.conf -address 172.18.0.5
   ```
5. Run the benchmark inside a container

   ```bash
   docker exec -it detock-r1p1 /bin/bash
   ```

   ```bash
   cd /detock && \
   ./build/benchmark \
     -config examples/movr/movr-2-regions.conf \
     -duration 10 \
     -clients 5 \
     -txns 100 \
     -wl movr \
     -params="mh=20,mp=20,skew=0.2,txn_mix=30:5:5:15:30:15" \
     -seed 99 \
     -out_dir .
   ```

   For more information about the benchmarking script, refer to the [SLOG Wiki](https://github.com/umd-dslam/SLOG/wikihttps:/).

## How to run a Whole Experiment

An experiment consists of multiple benchmarks which are executed sequentially with different parameters. Each experiment targets a specific scenario, outlined below, and measures throughput, latency, aborts, bytes transfered and cost. To run an experiment for a specific scenario, use the `tools/run_config_on_remote.py` and `tools/admin.py` scripts. Instructions for how to use these scripts in the ST cluster environment can be found in this [README](https://github.com/anon/anon/blob/main/tools/README.md) file.

Setup Python environment:

```bash
python3.8 -m venv build_detock && source build_detock/bin/activate
```

Install requirements:

```bash
pip install -r tools/requirements.txt
```

Start the cluster:

```bash
python3 tools/admin.py start --image your-username/detock-movr:latest examples/movr/movr-tu-cluster.conf -u your-username -e GLOG_v=1 --bin slog
```

Run a scenario:

```bash
python3 tools/run_config_on_remote.py \
   -m st1 \
   -s baseline \
   -w movr \
   -c examples/movr/movr-tu-cluster-detock.conf \
   -u username \
   -db Detock \
   -b benchmark-container-name \
   -sc server-container-name
```

### Scenarios

The scenario is set using the `-s` flag in the above command. Currently, the following scenarios are supported:

* `baseline`: varies the probability of generating multi-home transactions
* `skew`: varies the skew factor used for determining which records the transcations access
* `scalability`: varies the number of clients used to generate transactions
* `network`: slowly introduces artificial network delay during benchmark execution
* `packet_loss`: slowly increases the probability for the network to drop packets at random
* `sunflower`: varies the sunflower falloff used to determine how active neighbouring regions are compared to the peak region
* `lat_breakdown`: measures the time spent during the benchmark across various system components: server, forwarder, sequencer, multi-home orderer, log manager, scheduler, lock manager, worker, other. Note: the configuration file must have `enabled_events: ALL`.

## How to Generate the Plots

The plots are generated using the `plots/extract_exp_results.py` and `plots/extract_latency_breakdown.py` scripts. Before running the scripts, make sure that dependencies are installed (same as `tools/run_config_on_remote.py` and `tools/admin.py`, refer to this [README](https://github.com/anon/anon/blob/main/tools/README.md) file) The scripts expect the data to be in the `plots/raw_data/movr/scenario` directory so copy the data there if it is not present and then run the scripts.

```bash
python3 plots/extract_exp_results.py \
   -s baseline \
   -w movr \
   -sa true \
   -lp "50;90;95"
```

```bash
python3 plots/extract_latency_breakdown.py \
   -df plots/raw_data/movr/lat_breakdown
   -w movr
   -o plots/data/final/movr/latency_breakdown
```
