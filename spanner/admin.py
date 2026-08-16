import argparse
import subprocess as sp
from datetime import datetime
import os
from os.path import join
import time
import logging

VALID_ACTIONS = ['benchmark', 'collect_client', 'collect_server']
VALID_WORKLOADS = ['ycsb', 'tpcc', 'benchx']

LOG_FORMAT = "%(asctime)s %(name)10s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger("spanner_admin")

def run_remote(ip, cmd, user):
    """Executes a command on a remote host via SSH."""
    ssh_cmd = f"ssh -o StrictHostKeyChecking=no {user}@{ip} \"{cmd}\""
    return sp.run(ssh_cmd, shell=True, capture_output=True, text=True)

def benchmark_ycsb(server_ips, image, user, clients, duration, params, seed, txns, workload, prime_region_ips, client_ips):
    mp = 0.5
    if 'mp=' in params:
        mp = int(params.split('mp=')[1].split(',')[0])/100.0
    mh = 0.5
    if 'mh=' in params:
        mh = int(params.split('mh=')[1].split(',')[0])/100.0
    nparts = len(server_ips) / len(prime_region_ips)
    nregs = len(prime_region_ips)
    skew = 0.0
    if 'skew=' in params:
        skew = float(params.split('skew=')[1].split(',')[0])

    LOG.info("--- Deploying benchmark containers on client machines concurrently ---")
    def deploy_client(i, client_ip, prime_region_ip):
        run_remote(client_ip, "docker rm -f spanner-client || true", user)
        LOG.info("  - Cleaned up old containers on %s", client_ip)
        LOG.info("Deploying to %s (Region %s)...", client_ip, i)

        cpp_cmd = (
            f"./benchmark_ycsb_spanner {prime_region_ip} {clients} 0 {skew} {mp} {mh} {int(nparts)} {int(nregs)} {duration} {i}"
        )
        LOG.info("  - C++ Command: %s", cpp_cmd)
        run_remote(client_ip, f"docker pull {image}", user)
        
        docker_cmd = (
            f"docker run -d --name spanner-client "
            f"-e SPANNER_EMULATOR_HOST={prime_region_ip}:30000 "
            f"--net=host {image} {cpp_cmd}"
        )
        LOG.info("  - Running command: %s", docker_cmd)
        result = run_remote(client_ip, docker_cmd, user)
        
        if result.returncode == 0:
            LOG.info("Successfully started benchmark on %s", client_ip)
        else:
            LOG.warning("Failed to start on %s: %s", client_ip, result.stderr)

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(client_ips)) as executor:
        futures = [executor.submit(deploy_client, i, c_ip, p_ip) for i, (c_ip, p_ip) in enumerate(zip(client_ips, prime_region_ips))]
        concurrent.futures.wait(futures)

    time.sleep(duration+5)
    benchmark_complete = False
    while not benchmark_complete:
        benchmark_complete = True
        for client_ip in client_ips:
            result = run_remote(client_ip, "docker logs spanner-client", user)
            logs = result.stdout + result.stderr
            if "--- RESULTS ---" not in logs:
                benchmark_complete = False
                LOG.info("  - Benchmark still running on %s...", client_ip)
        if not benchmark_complete:
            time.sleep(5)

    LOG.info("--- All clients finished. Use 'docker logs -f spanner-client' for more details. ---")
    tag = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    LOG.info("Tag: %s", tag)

def benchmark_tpcc(server_ips, image, user, clients, duration, params, seed, txns, workload, prime_region_ips, client_ips, scenario='scalability'):
    total_warehouses = 1200
    if 'warehouses=' in params:
        total_warehouses = int(params.split('warehouses=')[1].split(',')[0])

    active_warehouses = clients
    if 'active=' in params:
        active_warehouses = int(params.split('active=')[1].split(',')[0])
    
    nregs = len(prime_region_ips)
    nparts = len(server_ips) // len(prime_region_ips)

    LOG.info("--- Deploying benchmark containers on client machines concurrently ---")
    def deploy_client(i, client_ip, prime_region_ip):
        run_remote(client_ip, "docker rm -f spanner-client || true", user)
        LOG.info("  - Cleaned up old containers on %s", client_ip)
        LOG.info("Deploying TPC-C to %s (Region %s)...", client_ip, i)
        run_remote(client_ip, f"docker pull {image}", user)
        
        # We will build a custom C++ TPC-C client for Spanner
        cpp_cmd = f"./benchmark_tpcc_spanner {prime_region_ip} {clients} {duration} {total_warehouses} {active_warehouses} {nparts} {nregs} {i} '{params}' {seed + i}"
        
        docker_cmd = (
            f"docker run -d --name spanner-client "
            f"-e SPANNER_EMULATOR_HOST={prime_region_ip}:30000 "
            f"--net=host {image} {cpp_cmd}"
        )

        LOG.info("  - Running command: %s", docker_cmd)
        result = run_remote(client_ip, docker_cmd, user)
        
        if result.returncode == 0:
            LOG.info("Successfully started TPC-C on %s", client_ip)
        else:
            LOG.warning("Failed to start on %s: %s", client_ip, result.stderr)

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(client_ips)) as executor:
        futures = [executor.submit(deploy_client, i, c_ip, p_ip) for i, (c_ip, p_ip) in enumerate(zip(client_ips, prime_region_ips))]
        concurrent.futures.wait(futures)

    time.sleep(duration + 10)
    benchmark_complete = False
    while not benchmark_complete:
        benchmark_complete = True
        for client_ip in client_ips:
            result = run_remote(client_ip, "docker logs spanner-client", user)
            logs = result.stdout + result.stderr
            if "--- RESULTS ---" not in logs and "Error: " not in logs:
                benchmark_complete = False
                LOG.info("  - TPC-C still running on %s...", client_ip)
        if not benchmark_complete:
            time.sleep(10)
    
    LOG.info("✅ TPC-C Benchmark Run Complete.")
    tag = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    LOG.info("Tag: %s", tag)

def benchmark_benchx(server_ips, image, user, clients, duration, params, seed, txns, workload, prime_region_ips, client_ips, scenario='scalability'):
    nregs = len(prime_region_ips)
    nparts = len(server_ips) // len(prime_region_ips)

    LOG.info("--- Deploying benchmark containers on client machines concurrently ---")
    def deploy_client(i, client_ip, prime_region_ip):
        run_remote(client_ip, "docker rm -f spanner-client || true", user)
        LOG.info("  - Cleaned up old containers on %s", client_ip)
        LOG.info("Deploying BenchX to %s (Region %s)...", client_ip, i)
        run_remote(client_ip, f"docker pull {image}", user)

        cpp_cmd = f"./benchmark_benchx_spanner {prime_region_ip} {clients} {duration} {nparts} {nregs} {i} '{params}' {seed + i}"
        
        docker_cmd = (
            f"docker run -d --name spanner-client "
            f"-e SPANNER_EMULATOR_HOST={prime_region_ip}:30000 "
            f"--net=host {image} {cpp_cmd}"
        )
        
        LOG.info("  - Running command: %s", docker_cmd)
        result = run_remote(client_ip, docker_cmd, user)
        
        if result.returncode == 0:
            LOG.info("Successfully started BenchX on %s", client_ip)
        else:
            LOG.warning("Failed to start on %s: %s", client_ip, result.stderr)

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(client_ips)) as executor:
        futures = [executor.submit(deploy_client, i, c_ip, p_ip) for i, (c_ip, p_ip) in enumerate(zip(client_ips, prime_region_ips))]
        concurrent.futures.wait(futures)

    time.sleep(duration + 10)
    benchmark_complete = False
    while not benchmark_complete:
        benchmark_complete = True
        for client_ip in client_ips:
            result = run_remote(client_ip, "docker logs spanner-client", user)
            logs = result.stdout + result.stderr
            if "Total Committed:" not in logs and "Error: " not in logs:
                benchmark_complete = False
                LOG.info("  - BenchX still running on %s...", client_ip)
        if not benchmark_complete:
            time.sleep(10)
    
    LOG.info("✅ BenchX Benchmark Run Complete.")
    tag = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    LOG.info("Tag: %s", tag)

def main():
    parser = argparse.ArgumentParser(description="Spawn remote C++ benchmark clients for Spanner Omni.")
    parser.add_argument('-a',  '--action', choices=VALID_ACTIONS, required=True, help="Action to perform")
    parser.add_argument('-co', '--config', default='examples/ycsb/tu_cluster_ycsb_spanner.conf', help="Path to the configuration file")
    parser.add_argument('-i',  '--image', default="omraz/seq_eval:spanner_benchmark", help="Docker image containing benchmark clients")
    parser.add_argument('-u',  '--user', default="omraz", help="SSH user")
    parser.add_argument('-c',  '--clients', type=int, default=4, help="Threads per client machine")
    parser.add_argument('-d',  '--duration', type=int, default=60, help="Benchmark duration in seconds")
    parser.add_argument('-p',  '--params', '--param', dest='params', default="mh=50,mp=50", help="Workload params: <param1>=<val1>,<param2>=<val2>,...")
    parser.add_argument('-s',  '--seed', type=int, default=1, help="Base seed for randomization")
    parser.add_argument('-t',  '--txns', type=int, default=2_000_000, help="Number of transactions to run (if applicable)")
    parser.add_argument('-wl', '--workload', choices=VALID_WORKLOADS, default='ycsb', help="Workload type for benchmarking")
    parser.add_argument('-od', '--out-dir', default='data', help="Base output directory for collected results")
    parser.add_argument('-ta', '--tag', default=None, help="Custom tag for this experiment run")
    parser.add_argument('-sc', '--scenario', default='scalability', help="Experiment scenario")
    
    args = parser.parse_args()
    action = args.action
    conf = args.config
    image = args.image
    user = args.user
    clients = args.clients
    duration = args.duration
    params = args.params
    seed = args.seed
    txns = args.txns
    workload = args.workload
    out_dir = args.out_dir
    tag = args.tag
    scenario = args.scenario

    with open(conf, 'r') as f:
        conf_contents = f.readlines()

    server_ips = []
    prime_region_ips = []
    client_ips = []
    for l in conf_contents:
        if '    addresses: "' in l:
            ip = l.split('addresses: "')[1].split('"')[0]
            server_ips.append(ip)
            if len(prime_region_ips) == len(client_ips):
                prime_region_ips.append(ip)
        elif '    client_addresses: "' in l:
            ip = l.split('client_addresses: "')[1].split('"')[0]
            client_ips.append(ip)
    
    if server_ips == [] or client_ips == []:
        LOG.info("No client addresses found in config.")
        return
    LOG.info("--- Launching Benchmark on %s machines ---", len(client_ips))

    if action == 'benchmark':
        if workload == 'ycsb':
            benchmark_ycsb(server_ips, image, user, clients, duration, params, seed, txns, workload, prime_region_ips, client_ips)
        elif workload == 'tpcc':
            benchmark_tpcc(server_ips, image, user, clients, duration, params, seed, txns, workload, prime_region_ips, client_ips, scenario)
        elif workload == 'benchx':
            benchmark_benchx(server_ips, image, user, clients, duration, params, seed, txns, workload, prime_region_ips, client_ips, scenario)
        else:
            LOG.warning("Workload %s not supported.", workload)

    elif action == 'collect_client':
        base_dir = join("data", tag) if tag else join("data", "spanner_run")
        LOG.info("--- Collecting Results into %s ---", base_dir)
        
        for i, client_ip in enumerate(client_ips):
            client_dir = os.path.join(base_dir, "client", f"0-{i}")
            os.makedirs(client_dir, exist_ok=True)

            LOG.info("Collecting logs from client %s...", client_ip)
            remote_tmp = f"/tmp/client_{i}_results"

            if workload in ('ycsb', 'benchx', 'tpcc'):
                run_remote(client_ip, f"mkdir -p {remote_tmp}", user)
                run_remote(client_ip, f"docker cp spanner-client:/app/summary.csv {remote_tmp}/summary.csv || true", user)
                run_remote(client_ip, f"docker cp spanner-client:/app/transactions.csv {remote_tmp}/transactions.csv || true", user)
                
                if workload == 'ycsb':
                    run_remote(client_ip, f"docker cp spanner-client:/app/metadata.csv {remote_tmp}/metadata.csv || true", user)
                    run_remote(client_ip, f"docker cp spanner-client:/app/txn_events.csv {remote_tmp}/txn_events.csv || true", user)

                is_local = client_ip in ["localhost", "127.0.0.1"]
                if is_local:
                    sp.run(f"cp {remote_tmp}/summary.csv {client_dir}/summary.csv", shell=True)
                    sp.run(f"cp {remote_tmp}/transactions.csv {client_dir}/transactions.csv", shell=True)
                    if workload == 'ycsb':
                        sp.run(f"cp {remote_tmp}/metadata.csv {client_dir}/metadata.csv", shell=True)
                        sp.run(f"cp {remote_tmp}/txn_events.csv {client_dir}/txn_events.csv", shell=True)
                else:
                    sp.run(f"scp {user}@{client_ip}:{remote_tmp}/summary.csv {client_dir}/summary.csv", shell=True)
                    sp.run(f"scp {user}@{client_ip}:{remote_tmp}/transactions.csv {client_dir}/transactions.csv", shell=True)
                    if workload == 'ycsb':
                        sp.run(f"scp {user}@{client_ip}:{remote_tmp}/metadata.csv {client_dir}/metadata.csv", shell=True)
                        sp.run(f"scp {user}@{client_ip}:{remote_tmp}/txn_events.csv {client_dir}/txn_events.csv", shell=True)

        LOG.info("✅ All results stored in %s", base_dir)

if __name__ == "__main__":
    main()
