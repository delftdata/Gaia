# Spanner Omni Evaluation

This directory contains the necessary scripts and resources to deploy and test Google's Spanner Omni (preview) distributed database for benchmarking purposes alongside the existing systems like CockroachDB (CRDB) and SLOG.

## Prerequisites

Spanner Omni is provided via a Docker image from Google's Artifact Registry. Ensure your host machine(s) have Docker installed and are authenticated to pull the image: `us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r1-beta.2`

You also need the Google Cloud Spanner SDK installed on the machine running these Python scripts. The required dependencies are listed in the root `tools/requirements.txt`:
```bash
pip install -r tools/requirements.txt
```

### Kubernetes Prerequisites (For ST Nodes)

Because Spanner Omni requires a Kubernetes environment to orchestrate a distributed, multi-region cluster, `k3s` and `helm` must be installed on the ST nodes before deploying.

To install `k3s` (Kubernetes) and `helm` on the ST nodes, SSH into each node and run the following commands:
```bash
# 1. Install k3s (without starting the service immediately)
curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true sh -

# 2. Install Helm
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
```
If you are running tests on AWS, the `aws/launch_cluster.py` script automatically handles this installation during instance boot.

## Building the Benchmark Client

The C++ benchmark clients (like `benchmark_ycsb_spanner`) are packaged in a dedicated Docker image. Because they rely on the heavy `google-cloud-cpp` SDK (which in turn requires gRPC and Protobuf), the image builds these dependencies from source using `vcpkg`.

To build the Spanner benchmark image, run this from the repository root:
```bash
docker build -t omraz/seq_eval:spanner_benchmark -f spanner/Dockerfile .
docker push omraz/seq_eval:spanner_benchmark
```

## Management Script (`manage_spanner.py`)

The `manage_spanner.py` script automates the deployment and initialization of the Spanner Omni cluster across multiple remote nodes. It closely mirrors the behavior of the CRDB management scripts.

### Basic Usage

**Start the Cluster:**
```bash
python3 spanner/manage_spanner.py --action start -c spanner/tu_cluster_spanner.json -u <ssh_user>
```
OR on AWS:
```bash
python3 spanner/manage_spanner.py --action start -c aws/aws_spanner.json -u ubuntu -cp 16 -me 128
```
*This command pulls the Spanner Omni image, creates a persistent Docker volume, and launches the container on every node defined in the cluster JSON config. It will also initialize a `geo-bench` database on the primary node.*

**Populate the Database (YCSB):**
```bash
python3 spanner/manage_spanner.py --action populate -c spanner/tu_cluster_spanner.json -u <ssh_user>
```
OR on AWS:
```bash
python3 spanner/manage_spanner.py --action populate -c aws/aws_spanner.json -u ubuntu
```
*Connects directly to the Spanner Omni emulator API (bypassing GCP auth) and efficiently populates the `usertable` with 10 million rows using batch insertions.*

**Stop and Clean the Cluster:**
```bash
python3 spanner/manage_spanner.py --action stop -c spanner/tu_cluster_spanner.json -u <ssh_user>
```
OR on AWS:
```bash
python3 spanner/manage_spanner.py --action stop -c aws/aws_spanner.json -u ubuntu
```
*Forces the removal of all `spanneromni` containers and wipes the associated local data volumes.*

## Inspecting the K3s Cluster

To view the active Spanner pods (or nodes/services) running on a host machine, you need to use the specific K3s configuration file. Also, this only works on the very 1st server node. SSH into that node and run:

```bash
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get pods -n spanner-ns
```

## Inspecting the Data

Unlike CockroachDB, Spanner Omni does not bundle a standard SQL CLI directly into the container. However, we can inspect the contents of the database on the ST cluster using Python:

```python
import os
from google.cloud import spanner
os.environ["SPANNER_EMULATOR_HOST"] = "131.180.125.40:15000"
client = spanner.Client(project="default")
database = client.instance("default").database("geo-bench")
with database.snapshot() as snapshot:
    results = snapshot.execute_sql("SELECT * FROM usertable LIMIT 5")
    for row in results:
        print(row)
```

## Running Benchmarks

To trigger a single short YCSB benchmark use the admin script:

```bash
python3 spanner/admin.py --action benchmark -co examples/ycsb/tu_cluster_ycsb_spanner.conf -u omraz -d 20 -c 4 -wl ycsb
```
Or on AWS:
```bash
python3 spanner/admin.py --action benchmark -co aws/conf_files/ycsb/aws_ycsb_spanner.conf -u ubuntu -d 20 -c 4 -wl ycsb
```

To collect the generated CSV results file:

```bash
python3 spanner/admin.py --action collect_client -co examples/ycsb/tu_cluster_ycsb_spanner.conf -u omraz -wl ycsb
```
Or on AWS:
```bash
python3 spanner/admin.py --action collect_client -co aws/conf_files/ycsb/aws_ycsb_spanner.conf -u ubuntu -wl ycsb
```

### Running experiments with run_config_on_remote.py

Use the following command to run a full YCSB scenario on Spanner:

`python3 tools/run_config_on_remote.py -w ycsb -c examples/ycsb/tu_cluster_ycsb_spanner.conf -u omraz -db spanner -s scalability -i omraz/seq_eval:spanner_benchmark`

### Running Fault Tolerance Experiments

Fault tolerance experiments (which run for 120 seconds by default) require manual intervention to simulate a node failure.

1. Start the experiment on the client machine using: `python3 tools/run_config_on_remote.py -i omraz/seq_eval:spanner_benchmark -m ubuntu@54.193.108.232 -s fault_tolerance -w ycsb -c aws/conf_files/ycsb/aws_ycsb_spanner.conf -u ubuntu -bl True -db spanner -d 120 2>&1 | tee scenario_$(date +"%d-%m-%y_%H-%M-%S").log`. We use a longer duration than usual since we need to give the system 10s to warm up properly, and then we kill the node after 15s. We then restart the node after another ~30s.
2. SSH into the node you wish to simulate the failure on (e.g., the prime node).
3. Stop the K3s agent to kill the Spanner Omni container:
   ```bash
   sudo systemctl stop detock-k3s-agent
   ```
4. After some time (e.g., 30 seconds), restart the agent to bring the node back online:
   ```bash
   sudo /tmp/start_k3s.sh
   ```

For the whole region failure simulation, we instead stop all nodes in that region.

Alternatively, you can now use the built-in automatic failure inducing scenario:
`python3 tools/run_config_on_remote.py -i omraz/seq_eval:spanner_benchmark -m ubuntu@18.144.208.126 -s fault_tolerance -w ycsb -c aws/conf_files/ycsb/aws_ycsb_spanner.conf -u ubuntu -bl True -db spanner -d 120 2>&1 | tee scenario_$(date +"%d-%m-%y_%H-%M-%S").log`

## Supported Benchmarks

Currently, data population is implemented for the following workloads:
- **YCSB**: (Implemented in `manage_spanner.py`)
- *TPC-C / BenchX*: (To be implemented)

*Note: Since Spanner Omni is in preview, some enterprise features like TLS or automated backup are not included.*
