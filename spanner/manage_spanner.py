import json
import subprocess as sp
import time
import argparse
import os
import sys
import concurrent.futures
import warnings
import base64

# Suppress Python 3.8 deprecation warnings from Google SDK and Cryptography
warnings.filterwarnings("ignore")

import datetime
import builtins

def _custom_print(*args, **kwargs):
    timestamp = datetime.datetime.now().strftime('%H:%M:%S')
    kwargs['flush'] = True
    builtins.print(f"[{timestamp}]", *args, **kwargs)
print = _custom_print

from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials
import socket
from tpcc_populate import run_tpcc_population

TARGET_INSTANCE_ID = "default"
TARGET_PROJECT_ID = "default"
TARGET_DB_NAME = "geo-bench"
YCSB_ROW_COUNT = 10_000_000
YCSB_TABLE_NAME = "usertable"
ROW_INSERT_BATCH = 5_000
VALID_CONFIG_JSONS = ['spanner/tu_cluster_spanner.json', 'aws/aws_spanner.json']
VALID_ACTIONS = ['start', 'stop', 'populate']
DEFAULT_USER = "omraz"
SPANNER_PORT = 30000 # Spanner Omni grpc nodePort default

def run_remote(ip, cmd, user=DEFAULT_USER):
    """Executes a command on a remote host via SSH."""
    ssh_cmd = f"ssh -o StrictHostKeyChecking=no {user}@{ip} \"{cmd}\""
    result = sp.run(ssh_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error on {ip}: {result.stderr}")
    return result

def sudo_cmd(cmd, password):
    if password:
        return f"echo {password} | sudo -S {cmd}"
    return f"sudo {cmd}"

def stop_node(ip, user, password=None):
    """Logic for a single node stop/clean"""
    print(f"Stopping and cleaning k3s/Spanner at {ip}...")
    run_remote(ip, sudo_cmd("systemctl stop detock-k3s-server detock-k3s-agent || true", password), user)
    run_remote(ip, sudo_cmd("systemctl reset-failed detock-k3s-server detock-k3s-agent || true", password), user)
    run_remote(ip, sudo_cmd("bash -c 'if [ -f /usr/local/bin/k3s-killall.sh ]; then /usr/local/bin/k3s-killall.sh; fi'", password), user)
    run_remote(ip, sudo_cmd("bash -c 'if [ -f /usr/local/bin/k3s-agent-uninstall.sh ]; then /usr/local/bin/k3s-agent-uninstall.sh; fi'", password), user)
    run_remote(ip, sudo_cmd("bash -c 'if [ -f /usr/local/bin/k3s-uninstall.sh ]; then /usr/local/bin/k3s-uninstall.sh; fi'", password), user)
    return f"Done: {ip}"

def stop_cluster_parallel(all_ips, user, password=None):
    print("--- Action: Stopping k3s Cluster (Parallel) ---")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_ips)) as executor:
        future_to_ip = {executor.submit(stop_node, ip, user, password): ip for ip in all_ips}
        for future in concurrent.futures.as_completed(future_to_ip):
            ip = future_to_ip[future]
            try:
                data = future.result()
                print(f"Successfully cleaned {ip}")
            except Exception as exc:
                print(f"Node {ip} generated an exception: {exc}")

def install_dependencies_on_node(ip, user, password):
    print(f"[{ip}] Checking and installing k3s/helm dependencies...")
    install_script = """#!/bin/bash
if ! command -v /usr/local/bin/k3s &> /dev/null; then
    echo "Installing k3s..."
    curl -sfL https://get.k3s.io > /tmp/get_k3s.sh
    chmod +x /tmp/get_k3s.sh
    INSTALL_K3S_SKIP_START=true /tmp/get_k3s.sh
fi
if ! command -v /usr/local/bin/helm &> /dev/null; then
    echo "Installing helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 > /tmp/get_helm.sh
    chmod +x /tmp/get_helm.sh
    /tmp/get_helm.sh
fi
"""
    # Write script to remote
    import base64
    script_b64 = base64.b64encode(install_script.encode()).decode()
    run_remote(ip, f"echo '{script_b64}' | base64 -d > /tmp/install_deps.sh", user)
    run_remote(ip, sudo_cmd("bash /tmp/install_deps.sh", password), user)

def start_cluster_parallel(all_ips, ip_to_region, primary_ip, image, user, password=None, cpus=16, memory=128):
    print("--- Action: Checking dependencies on all nodes ---")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_ips)) as executor:
        futures = [executor.submit(install_dependencies_on_node, ip, user, password) for ip in all_ips]
        concurrent.futures.wait(futures)
        
    print(f"--- Bootstrapping k3s Control Plane on {primary_ip} ---")
    
    server_script = f"#!/bin/bash\\nsystemctl reset-failed detock-k3s-server || true\\nsystemd-run --unit=detock-k3s-server -p CPUQuota={cpus*100}% -p MemoryMax={memory}G /usr/local/bin/k3s server --cluster-init --write-kubeconfig-mode 644"
    run_remote(primary_ip, f"echo -e '{server_script}' > /tmp/start_k3s.sh && chmod +x /tmp/start_k3s.sh", user)
    run_remote(primary_ip, sudo_cmd("/tmp/start_k3s.sh", password), user)
    
    print("Waiting 15 seconds for k3s server to initialize...")
    time.sleep(15)
    
    # Get token
    token_res = run_remote(primary_ip, sudo_cmd("cat /var/lib/rancher/k3s/server/node-token", password), user)
    token = token_res.stdout.strip()
    
    replica_ips = [ip for ip in all_ips if ip != primary_ip]
    if replica_ips:
        print("--- Joining Worker Nodes ---")
        agent_script = f"#!/bin/bash\\nsystemctl reset-failed detock-k3s-agent || true\\nsystemd-run --unit=detock-k3s-agent -p CPUQuota={cpus*100}% -p MemoryMax={memory}G /usr/local/bin/k3s agent --server https://{primary_ip}:6443 --token {token}"
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(replica_ips)) as executor:
            future_to_node = {}
            for ip in replica_ips:
                run_remote(ip, f"echo -e '{agent_script}' > /tmp/start_k3s.sh && chmod +x /tmp/start_k3s.sh", user)
                future_to_node[executor.submit(run_remote, ip, sudo_cmd("/tmp/start_k3s.sh", password), user)] = ip
            for future in concurrent.futures.as_completed(future_to_node):
                print(f"Joined node {future_to_node[future]}")
        print("Waiting 20 seconds for agents to join...")
        time.sleep(20)

    print("--- Applying Kubernetes Topology Labels ---")
    for ip in all_ips:
        region = ip_to_region[ip]
        # Use KUBECONFIG explicitly
        kubectl = "KUBECONFIG=/etc/rancher/k3s/k3s.yaml /usr/local/bin/kubectl"
        node_line_res = run_remote(primary_ip, f"{kubectl} get nodes -o wide | grep {ip}", user)
        node_name = ""
        if node_line_res.stdout.strip():
            node_name = node_line_res.stdout.strip().split()[0]
            
        if node_name:
            run_remote(primary_ip, f"{kubectl} label node {node_name} topology.kubernetes.io/region={region} --overwrite", user)
            run_remote(primary_ip, f"{kubectl} label node {node_name} topology.kubernetes.io/zone={region} --overwrite", user)
        else:
            print(f"Warning: Could not find node name for IP {ip}")
            
    print("--- Generating Helm values.yaml ---")
    region_counts = {}
    for region in ip_to_region.values():
        region_counts[region] = region_counts.get(region, 0) + 1
        
    yaml_zones = []
    for region, count in region_counts.items():
        yaml_zones.append(f'''        - name: "{region}"
          shortName: "{region}"
          replicas: {count}
          rootServers: 1
          singleServer: false
          replicaType: "READ_WRITE"''')
          
    zones_str = "\n".join(yaml_zones)
    
    values_yaml = f"""
deployment:
  singleServer: false
  multiCluster: false
  rootServersPerZone: 0

service:
  type: NodePort
  nodePort: 30000

defaults:
  locations:
    - name: global
      namespace: spanner-ns
      zones:
{zones_str}
"""
    # Write values.yaml on primary using base64 to avoid shell escaping issues
    yaml_b64 = base64.b64encode(values_yaml.encode()).decode()
    run_remote(primary_ip, f"echo '{yaml_b64}' | base64 -d > /tmp/spanner_values.yaml", user)
    
    print("--- Deploying Spanner Omni via Helm ---")
    deploy_cmd = (
        "KUBECONFIG=/etc/rancher/k3s/k3s.yaml /usr/local/bin/helm install spanner-omni oci://us-docker.pkg.dev/spanner-omni/charts/spanner-omni "
        "--version 0.2.0 -f /tmp/spanner_values.yaml --set currentLocation=global -n spanner-ns --create-namespace"
    )
    res = run_remote(primary_ip, deploy_cmd, user)
    print(res.stdout)
    if res.returncode != 0:
        print(f"Helm install failed: {res.stderr}")
    
    print("Waiting for Spanner Omni bootstrap job to complete (this may take a few minutes)...")
    wait_cmd = "KUBECONFIG=/etc/rancher/k3s/k3s.yaml /usr/local/bin/kubectl wait --for=condition=complete --timeout=600s job/spanner-bootstrap-job -n spanner-ns"
    run_remote(primary_ip, wait_cmd, user)

def initialize_spanner_db(primary_ip):
    print(f"--- Initializing Spanner Instance and DB on {primary_ip} ---")
    # Tell the SDK to point to our Omni/Emulator instance without needing GCP auth
    os.environ["SPANNER_EMULATOR_HOST"] = f"{primary_ip}:{SPANNER_PORT}"
    
    # Wait for emulator to be responsive on the port
    print(f"Waiting for Spanner emulator port {SPANNER_PORT} to be reachable on {primary_ip}...")
    start_time = time.time()
    while time.time() - start_time < 300:
        try:
            with socket.create_connection((primary_ip, SPANNER_PORT), timeout=2):
                break
        except OSError:
            time.sleep(2)
    else:
        print("Warning: Spanner emulator port did not become reachable in time.")
    
    # Wait for emulator to be responsive
    client = spanner.Client(project=TARGET_PROJECT_ID, credentials=AnonymousCredentials())
    
    # Create Instance if it doesn't exist (Emulator requires this)
    config_name = f"{client.project_name}/instanceConfigs/emulator-config"
    instance = client.instance(TARGET_INSTANCE_ID, configuration_name=config_name)
    try:
        if not instance.exists():
            print(f"Creating instance {TARGET_INSTANCE_ID}...")
            op = instance.create()
            op.result(120)
            print("Instance created.")
    except Exception as e:
        print(f"Instance check failed: {e}")
    instance = client.instance(TARGET_INSTANCE_ID)
    
    # Create Database
    database = instance.database(TARGET_DB_NAME)
    if not database.exists():
        print(f"Creating database {TARGET_DB_NAME}...")
        operation = database.create()
        operation.result(120)
        print("Database created.")
    
    return database

def populate_ycsb(primary_ip, row_count=YCSB_ROW_COUNT):
    print(f"--- Manually Populating YCSB: {int(row_count/1_000_000)}M rows ---")
    database = initialize_spanner_db(primary_ip)
    
    # 1. Prepare Schema
    print("Applying schema...")
    try:
        operation = database.update_ddl([
            f"CREATE TABLE {YCSB_TABLE_NAME} ("
            f"  ycsb_key INT64 NOT NULL,"
            f"  field0 BYTES(MAX)"
            f") PRIMARY KEY (ycsb_key)"
        ])
        operation.result(120)
    except Exception as e:
        print(f"Schema update message (may already exist): {e}")

    # 2. Insert Data
    print(f"--- Streaming {int(row_count/1_000_000)}M rows ---")
    cur_rows = 0
    while cur_rows < row_count:
        batch_size = min(ROW_INSERT_BATCH, row_count - cur_rows)
        with database.batch() as batch:
            columns = ("ycsb_key", "field0")
            values = []
            for i in range(cur_rows, cur_rows + batch_size):
                key = i
                val = base64.b64encode(os.urandom(100))
                values.append((key, val))
            
            batch.insert_or_update(
                table=YCSB_TABLE_NAME,
                columns=columns,
                values=values
            )
        cur_rows += batch_size
        if cur_rows % 100_000 == 0:
            print(f"Inserted {int(cur_rows/1_000)}k rows...")

    print("Data population finished!")

def main():
    parser = argparse.ArgumentParser(description="Manage a Spanner Omni cluster.")
    parser.add_argument('-c',  "--config", required=True, help="Path to cluster configuration JSON")
    parser.add_argument('-a',  "--action", required=True, choices=VALID_ACTIONS, help="Action to perform")
    parser.add_argument('-i',  "--image", default="spanner-omni-server:2026.r1-beta.1", help="Spanner Docker image to deploy")
    parser.add_argument('-u',  "--user", default=DEFAULT_USER, help="SSH user")
    parser.add_argument('-p',  "--password", default=None, help="SSH password for sudo")
    parser.add_argument('-cp', "--cpus", type=int, default=16, help="CPUs to allocate per server node")
    parser.add_argument('-me', "--memory", type=int, default=128, help="Memory (in GB) to allocate per server node")
    parser.add_argument('-w',  "--workload", default="ycsb", choices=["ycsb", "tpcc"], help="Workload to populate")
    parser.add_argument('-r',  "--rows", type=int, default=YCSB_ROW_COUNT, help="Number of YCSB rows (or TPCC warehouses) to populate")

    args = parser.parse_args()
    config = args.config
    action = args.action
    image = args.image
    user = args.user
    password = args.password
    cpus = args.cpus
    memory = args.memory
    workload = args.workload
    rows = args.rows

    with open(config) as f:
        cluster_config = json.load(f)

    all_ips = []
    ip_to_region = {}
    for region in cluster_config:
        for ip in cluster_config[region]:
            all_ips.append(ip)
            ip_to_region[ip] = region
    primary_ip = all_ips[0]

    if action == "stop":
        stop_cluster_parallel(all_ips, user, password=password)
    elif action == "start":
        start_cluster_parallel(all_ips, ip_to_region, primary_ip, image, user, password=password, cpus=cpus, memory=memory)
        # We must initialize the database only on the primary instance because they form a cluster now
        try:
            if workload == "tpcc":
                run_tpcc_population(primary_ip, rows, TARGET_PROJECT_ID, TARGET_INSTANCE_ID, TARGET_DB_NAME, SPANNER_PORT)
            else:
                populate_ycsb(primary_ip, row_count=rows)
        except Exception as e:
            print(f"Failed to populate {primary_ip}: {e}")
    elif action == "populate":
        try:
            if workload == "tpcc":
                run_tpcc_population(primary_ip, rows, TARGET_PROJECT_ID, TARGET_INSTANCE_ID, TARGET_DB_NAME, SPANNER_PORT)
            else:
                populate_ycsb(primary_ip, row_count=rows)
        except Exception as e:
            print(f"Failed to populate {primary_ip}: {e}")
    else:
        print("Invalid action specified.")

if __name__ == "__main__":
    main()
