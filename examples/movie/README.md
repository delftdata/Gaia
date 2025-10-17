
# DeathStar Movie Benchmark

  

The DeathStar movie benchmark simulates a IMDB-like movie review site. Its workload consists of submitting new reviews.

  

## Schema

*  **Movie**: stores basic info of the movies. 1000 movies are loaded as initial data.

*  **User**: stores information about the users. 1000 users are loaded as initial data.

*  **Review**: stores the reviews that the users have submitted

  

## Transactions
This benchmark includes only one transaction:
* **NewReview**: a user submits a review to the website. This transaction first fetches a movie based on its title and a user based on their username. It then inserts a new review and increments the review counter for the user.

## Partitioning
Each primary key represents a 12-digit ID, which is used for partitioning. In the case of integer primary keys, this ID is stored directly as its value. In the case of string primary keys, the first 12 characters are used to store the digits of the ID as text.

The partitions and home regions are determined as follows:
Partition = ID % num_partitions
Home = (ID / num_partitions) % num_regions

As an example, the partitioning for IDs 0-11:

|             | Home 0 | Home 1 | Home 0 | Home 1 | Home 0 | Home 1 |
|-------------|--------|--------|--------|--------|--------|--------|
| Partition 0 | 0      | 2      | 4      | 6      | 8      | 10     |
| Partition 1 | 1      | 3      | 5      | 7      | 9      | 11     |

## Workload arguments
* **mh**: Sets the percentage of transactions that will involve data from multiple home regions. (Default: 50)
* **mp**: Sets the percentage of transactions that will involve data from multiple partitions. (Default: 50)
* **skew**: A factor controlling the amount of skew in selecting the records involved in a transaction. Higher means more skew, 0 means uniform selection. (Set between 0.0-1.0, Default: 0.0)
* **sunflower**: This controls whether the sunflower scenario is active. 0 for inactive, 1 for active. (Default: 0)
* **sf_home**: If sunflower=1, this sets the home which will be targeted and receive extra load. (Default: 0).
* **sf_fraction**: If sunflower=1, this sets the fraction of transactions that will have the user record belong to the home set in sf_home.

## Running an experiment locally
You can run an experiment locally with 1 server and 1 client. 
First set up the server:
```bash
$ build/slog -config examples/movie/single.conf -address /tmp/slog
```
If this fails, it might be necessary to use the commands first:
```bash
$ mkdir /tmp/slog/
$ chmod 777 /tmp/slog/
```
Second, start the client:
```bash
$ build/benchmark --wl movie --clients 10 --generators 3 --config examples/movie/single.conf --duration 10 --out_dir /tmp/slog --seed 42
```

## Running a full scenario
Full, generalized instructions for running scenarios can be found [here](../../tools/README.md).

The available scenarios for the movie benchmark are: baseline, skew, sunflower, scalability, network, packet_loss and lat_breakdown

The movie benchmark supports the following systems: Detock, Calvin, SLOG and Janus

The following is an example of running a full scenario of the DeathStar Movie benchmark. Here we test the baseline scenario on the Detock system.



1. SSH into one of the ST machines.
2. Start the cluster:
```bash
$ python3 tools/admin.py start --image samnl3000/detock:latest examples/movie/tu_cluster_movie_ddr_ts.conf -u svandenhouten -e GLOG_v=1 --bin slog
```
3. Verify the cluster is running correctly:
```bash
$ python3 tools/admin.py status --image samnl3000/detock:latest examples/movie/tu_cluster_movie_ddr_ts.conf -u svandenhouten
```
4.  Run the scenario:
```bash
$ python3 tools/run_config_on_remote.py -s baseline -w movie -u svandenhouten -m st1 -c examples/movie/tu_cluster_movie_ddr_ts.conf -i samnl3000/detock:latest -w movie -db Detock
```
5. After it has finished, exit the SSH session.
6. Copy the results to your local machine:
```bash
$ scp -r st1:/home/svandenhouten/Detock/data/movie/baseline plots/raw_data/movie
```
7. Generate a plot from the results:
```bash
$ python3 plots/extract_exp_results.py -s baseline -w movie
```


lat_breakdown is the only scenario which uses a different script to generate its plots. E.g:
```bash
$ python3 plots/extract_latency_breakdown.py -w movie -df plots/raw_data/movie/lat_breakdown -o plots/data/final/movie/latency_breakdown
```
