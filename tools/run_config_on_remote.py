import sys
import time
import os
import subprocess as sp
import shutil
import argparse
import simulate_network
import pps.simulate_regions as simulate_regions

VALID_SCENARIOS = ['baseline', 'skew', 'scalability', 'network', 'packet_loss', 'sunflower', 'lat_breakdown', 'vary_hw']
VALID_WORKLOADS = ['ycsb', 'tpcc', 'movr', 'movie', 'pps', 'dsh', 'smallbank']
VALID_DATABASES = ['Detock', 'ddr_ts', 'ddr_only', 'slog', 'calvin', 'janus']
VALID_ENVIRONMENTS = ['local', 'st', 'aws']

parser = argparse.ArgumentParser(description="Run Detock experiment with a given scenario.")
parser.add_argument('-s',  '--scenario', default='skew', choices=VALID_SCENARIOS, help='Type of experiment scenario to run (default: baseline)')
parser.add_argument('-w',  '--workload', default='ycsb', choices=VALID_WORKLOADS, help='Workload to run (default: ycsb)')
parser.add_argument('-e',  '--environment', default='aws', choices=VALID_ENVIRONMENTS, help='What type of machine the experiment was run on.')
parser.add_argument('-c',  '--conf', default='examples/tu_cluster.conf', help='.conf file used for experiment')
parser.add_argument('-i',  '--img', default='omraz/seq_eval:latest', help='The Docker image of your built Detock system')
parser.add_argument('-d',  '--duration', default=60, help='Duration (in seconds) of a single experiment')
parser.add_argument('-dr', '--dry_run', default=False, help='Whether to run this as a dry run')
parser.add_argument('-u',  '--user', default="omraz", help='Username when logging into a remote machine')
parser.add_argument('-m',  '--machine', default="st5", help='The machine from which this script is (used to write out the scp command for collecting the results.)')
parser.add_argument(       '--clients', default=3000, help='Number of clients to use for a client machine')
parser.add_argument('-g',  '--generators', default=1, help='Number of generators to use for a client machine')
parser.add_argument('-tt', '--trial_tag', default="", help='Tag for differentiating between trials done with the same scenario (data will be collected to data/{workload}/{scenario}/{trial_tag}/{system}/{x_val})')
parser.add_argument('-db', '--database', default='Detock', choices=VALID_DATABASES, help='The database to test')

args = parser.parse_args()
scenario = args.scenario
workload = args.workload
environment = args.environment
conf = args.conf
image = args.img
duration = args.duration
dry_run = args.dry_run
user = args.user
machine = args.machine
generators = args.generators
trial_tag = args.trial_tag
benchmark_container = f"{user}_benchmark"
server_container = f"{user}_slog"
database = args.database

print(f"Running scenario: '{scenario}', workload: '{workload}', and trial tag: '{trial_tag}'")

BASIC_IFTOP_CMD = 'iftop 2>&1'

interfaces = {}

detock_dir = os.path.expanduser("~/Detock")
systems_to_test = [database]
#tag = None #"2025-04-09-14-20-49" # This is extracted from the benchmark command stderr
short_benchmark_log = "benchmark_cmd.log"
log_dir = "data/{}/raw_logs"
cur_log_dir = None

if workload == 'ycsb':
    multi_partition_settings = 'mp=50,'
elif workload == 'tpcc':
    multi_partition_settings = ''
elif workload == 'movr':
    multi_partition_settings = ''
elif workload == 'pps':
    multi_partition_settings = 'mp=50,'
elif workload == 'movie':
    multi_partition_settings = "mp=50,"
elif workload == 'dsh':
    multi_partition_settings = "mp=50,"
elif workload == 'smallbank':
    multi_partition_settings = "mp=50,"
else:
    raise Exception(f"Multipartition settings not defined for workload {workload}")

if workload == 'ycsb':
    if scenario == 'baseline':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    elif scenario == 'skew':
        benchmark_params = "\"mh=50,mp=50,hot={}\""
        clients = 3000
        x_vals = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250]
    elif scenario == 'scalability':
        benchmark_params = "\"mh=50,mp=50\""
        clients = None
        x_vals = [1, 10, 100, 1000, 10_000, 100_000, 200_000]
    elif scenario == 'network':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 10, 50, 100, 250, 500, 1000]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mh={},mp=50\""  # For the latency breakdown we just run the vanilla workload
        clients = 3000
        x_vals = [50]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mh={},mp=50\"" # For the varying HW we just run the vanilla workload
        clients = 3000
        x_vals = [50]
    elif scenario == 'sunflower':
        raise Exception(f"The sunflower scenario is not yet implemented for the {workload} workload.")
elif workload == 'tpcc':
    if scenario == 'baseline':
        benchmark_params = "\"mix=44:44:4:4:4,rem_item_prob={},rem_payment_prob={}\""
        clients = 3000
        x_vals = [0.0, 0.01, 0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.14, 0.16, 0.18, 0.20]
    if scenario == 'skew':
        benchmark_params = "\"mix=44:44:4:4:4,skew={}\""
        clients = 3000
        x_vals = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    elif scenario == 'scalability':
        benchmark_params = "\"mix=44:44:4:4:4\""
        clients = None
        x_vals = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000]
    elif scenario == 'network':
        benchmark_params = "\"mix=44:44:4:4:4\""
        clients = 3000
        x_vals = [0, 10, 50, 100, 250, 500, 1000]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mix=44:44:4:4:4\""
        clients = 3000
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mix=44:44:4:4:4,rem_item_prob={},rem_payment_prob={}\""
        clients = 3000
        x_vals = [0.01]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mix=44:44:4:4:4,rem_item_prob={},rem_payment_prob={}\""
        clients = 3000
        x_vals = [0.01]
    elif scenario == 'sunflower':
        raise Exception("The sunflower scenario is not yet implemented")
elif workload == 'dsh':
    if scenario == 'baseline':
        benchmark_params = "\"mh={},mp=.25\""
        clients = 3000
        x_vals = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    elif scenario == 'skew':
        benchmark_params = "\"mh=.25,mp=.25,hot_chance=.9,hot={}\""
        clients = 3000
        x_vals = [0.01, 0.05, 0.1, 0.25, 0.5]
    elif scenario == 'scalability':
        benchmark_params = "\"mh=.25,mp=.25\""
        clients = None
        x_vals = [1, 10, 100, 1000, 10_000, 100_000, 200_000]
    elif scenario == 'network':
        benchmark_params = "\"mh=.25,mp=.25\""
        clients = 3000
        x_vals = [0, 10, 50, 100, 250, 500, 1000]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mh=.25,mp=.25\""
        clients = 3000
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'sunflower':
        benchmark_params = "\"mh=.25,mp=.25,sf=/opt/slog/dsh/flower-{}.csv,duration=2000000\""
        clients = 3000
        x_vals = [0.5, 0.65, 0.85, 1.0]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mh={},mp=.25\""
        clients = 3000
        x_vals = [0.25]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mh={},mp=.25\""
        clients = 3000
        x_vals = [0.25]
    else:
        raise Exception(f"Scenario {scenario} not implemented for workload {workload}")
elif workload == 'movie':
    if scenario == 'baseline':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    elif scenario == 'skew':
        benchmark_params = "\"mh=50,mp=50,skew={}\""
        clients = 3000
        x_vals = [0, 0.01, 0.1, 0.2, 0.5, 1]
    elif scenario == 'scalability':
        benchmark_params = "\"mh=50,mp=50\""
        clients = None
        x_vals = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000]
    elif scenario == 'network':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 10, 50, 100, 250]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mh={},mp=50\"" 
        clients = 3000
        x_vals = [50]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [50]
    elif scenario == 'sunflower':
        benchmark_params = "\"mh=50,mp=50,sunflower=1,sf_fraction={},sf_home=0\""
        x_vals = [0.6, 0.8, 0.9, 0.95, 1]
    else:
        raise Exception(f"Scenario {scenario} not implemented for workload {workload}")
elif workload == 'movr':
    if scenario == 'baseline':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [0, 20, 40, 60, 80, 100]
    elif scenario == 'skew':
        benchmark_params = "\"mh=50,mp=50,skew={}\""
        clients = 3000
        x_vals = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
    elif scenario == 'scalability':
        benchmark_params = "\"mh=50,mp=50\""
        clients = None
        x_vals = [1, 10, 100, 1000, 10000, 1000000]
    elif scenario == 'network':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 10, 50, 100, 250, 500, 1000]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'sunflower':
        benchmark_params = "\"mh=50,mp=50,sunflower-falloff={},sunflower-max=40,sunflower-cycles=1\""
        clients = 3000
        x_vals = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mh={},mp={}\""
        clients = 3000
        x_vals = [50]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mh={},mp={}\""
        clients = 3000
        x_vals = [50]
    else:
        raise Exception(f"Scenario {scenario} not implemented for workload {workload}")
elif workload == 'pps':
    if scenario == 'baseline':
        benchmark_params = "\"mh={},mp=50,nearest=1,mix=80:8:8:2:2\""
        clients = args.clients
        x_vals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    elif scenario == 'skew':
        benchmark_params = "\"mh=50,mp=50,nearest=1,mix=80:8:8:2:2,hot={}\""
        clients = args.clients
        x_vals = [0, 0.0001, 0.001, 0.01, 0.1, 0.5, 1.0]
    elif scenario == 'scalability':
        benchmark_params = "\"mh=50,mp=50,nearest=1,mix=80:8:8:2:2\""
        clients = None
        x_vals = [1, 10, 100, 500, 1000, 5000, 10_000, 50_000, 100_000]
    elif scenario == 'network':
        benchmark_params = "\"mh=50,mp=50,nearest=1,mix=80:8:8:2:2\""
        clients = args.clients
        x_vals = [0, 10, 50, 100, 250, 500]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mh=50,mp=50,nearest=1,mix=80:8:8:2:2\""
        clients = args.clients
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mh={},mp=50,nearest=1,mix=80:8:8:2:2\""
        clients = 3000
        x_vals = [50]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mh={},mp=50,nearest=1,mix=80:8:8:2:2\""
        clients = 3000
        x_vals = [50]
    elif scenario == 'sunflower':
        benchmark_params = "\"mh=50,mp=50,nearest=1,mix=80:8:8:2:2,sunflower={}\""
        clients = args.clients
        x_vals = [0]
    else:
        raise Exception(f"Scenario {scenario} not implemented for workload {workload}")
elif workload == 'smallbank':
    if scenario == 'baseline':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    elif scenario == 'skew':
        benchmark_params = "\"mh=50,mp=50,hot={}\""
        clients = 3000
        x_vals = [0, 0.0001, 0.001, 0.01, 0.1, 0.5, 1.0]
    elif scenario == 'scalability':
        benchmark_params = "\"mh=50,mp=50\""
        clients = None
        x_vals = [1, 10, 100, 1000, 10_000, 100_000, 1_000_000]
    elif scenario == 'network':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 10, 50, 100, 250, 500]
    elif scenario == 'packet_loss':
        benchmark_params = "\"mh=50,mp=50\""
        clients = 3000
        x_vals = [0, 0.1, 0.2, 0.5, 1, 2, 5, 10]
    elif scenario == 'lat_breakdown':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [50]
    elif scenario == 'vary_hw':
        benchmark_params = "\"mh={},mp=50\""
        clients = 3000
        x_vals = [50]
    elif scenario == 'sunflower':
        benchmark_params = "\"mh={},mp=50,sunflower_target_regions=0:0:0:0:0:0:0:0:0:0:0:0,sunflower_target_probabilities=0:10:20:30:40:50:60:70:80:90:100:100\""
        clients = 3000
        x_vals = [50]
    else:
        raise Exception(f"Scenario {scenario} not implemented for workload {workload}")
else:
    raise Exception(f"Workload {workload} not implemented")

single_ycsb_benchmark_cmd = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 2000000 --seed 1 --clients {clients} --duration {duration} -wl basic --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"
single_tpcc_benchmark_cmd = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 2000000 --seed 1 --clients {clients} --duration {duration} -wl tpcc --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"
single_dsh_benchmark_cmd = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 2000000 --seed 1 --clients {clients} --duration {duration} -wl dsh --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"
single_movie_benchmark_cmd = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 2000000 --seed 1 --clients {clients} --duration {duration} -wl movie --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"
single_movr_benchmark_cmd = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 2000000 --seed 1 --clients {clients} --duration {duration} -wl movr --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"
single_pps_benchmark_cmd  = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 0 --seed 1 --clients {clients} --generators {generators} --duration {duration} -wl pps --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"
single_smallbank_benchmark_cmd = "python3 tools/admin.py benchmark --image {image} {conf} -u {user} --txns 0 --seed 1 --clients {clients} --duration {duration} -wl smallbank --param {benchmark_params} 2>&1 | tee {short_benchmark_log}"

if workload == 'ycsb':
    single_benchmark_cmd = single_ycsb_benchmark_cmd
elif workload == 'tpcc':
    single_benchmark_cmd = single_tpcc_benchmark_cmd
elif workload == 'dsh':
    single_benchmark_cmd = single_dsh_benchmark_cmd
elif workload == 'movie':
    single_benchmark_cmd = single_movie_benchmark_cmd
elif workload == 'movr':
    single_benchmark_cmd = single_movr_benchmark_cmd
elif workload == 'pps':
    single_benchmark_cmd = single_pps_benchmark_cmd
elif workload == 'smallbank':
    single_benchmark_cmd = single_smallbank_benchmark_cmd

collect_client_cmd = "python3 tools/admin.py collect_client -u {user} --config {conf} --out-dir data --tag {tag}"

def run_subprocess(cmd, dry_run=False):
    if dry_run:
        print(f"Would have run command: {cmd}")
        return True # TODO: fix properly
    else:
        return sp.run(cmd, shell=True, capture_output=True, text=True)

def get_server_ips_from_conf(conf_path):
    with open(conf_path, "r") as f:
        conf_data = f.readlines()
    ips_used = set()
    for line in conf_data:
        if '    addresses: ' in line:
            ips_used.add(line.split('    addresses: "')[1].split('"')[0])
    ips_used = list(ips_used)
    return ips_used

def get_client_ips_from_conf(conf_path):
    with open(conf_path, "r") as f:
        conf_data = f.readlines()
    ips_used = set()
    for line in conf_data:
        if '    client_addresses: ' in line:
            ips_used.add(line.split('    client_addresses: "')[1].split('"')[0])
    ips_used = list(ips_used)
    return ips_used

def get_network_interfaces(ips_used):
    interface = run_subprocess(BASIC_IFTOP_CMD).stdout.split('\n')[0].split('interface: ')[1]
    print(f"This machine uses the network interface: {interface}")
    for ip in ips_used:
        try:
            ssh_target = f"{user}@{ip}" if user else ip
            ssh_cmd = f"ssh {ssh_target} '{BASIC_IFTOP_CMD}'"
            result = run_subprocess(ssh_cmd, dry_run)
            cur_interface = result.stdout.split('\n')[0].split('interface: ')[1]
            print(f"IP {ip} uses interface {cur_interface}")
            interfaces[ip] = cur_interface
        except:
            print(f"Unable to find interface for IP: {ip}")

def start_net_monitor(user, interfaces):
    for ip, iface in interfaces.items():  # assuming interfaces is a dict {ip: interface}
        cmd = (
            f"ssh {user}@{ip} '"
            f"echo \"timestamp_ms,bytes_sent\" > net_traffic.csv; "
            f"prev=$(awk '\\''$1 ~ \"{iface}:\" {{print $10}}'\\'' /proc/net/dev); "
            f"while true; do "
            f"sleep 1; "
            f"now=$(date +%s%3N); "
            f"curr=$(awk '\\''$1 ~ \"{iface}:\" {{print $10}}'\\'' /proc/net/dev); "
            f"delta=$((curr - prev)); "
            f"echo \"$now,$delta\" >> net_traffic.csv; "
            f"prev=$curr; "
            f"done' > /dev/null 2>&1 &"
        )
        result = sp.run(cmd, shell=True)
        if hasattr(result, "returncode") and result.returncode != 0:
            print(f"Launch network monitoring command in ip '{ip}' failed with exit code {result.returncode}!")
    print("Started network monitoring on all server ips")

def stop_and_collect_monitor(user, interfaces, cur_log_dir):
    for ip in interfaces.keys():
        sp.run(f"ssh {user}@{ip} pkill -f net_traffic.csv", shell=True)
        result = sp.run(f"scp {user}@{ip}:net_traffic.csv {cur_log_dir}/net_traffic_{ip.replace('.', '_')}.csv", shell=True)
        if hasattr(result, "returncode") and result.returncode != 0:
            print(f"Collecting network monitoring command failed with exit code {result.returncode}!")
            break

# Helper function for TPC-C (possibly other benchmarks)
# Since loading the tables can take 
def check_table_loading_finished(ips, workload, conf_path):
    if workload == 'tpcc':
        # Check if all the orders in each warehouse have been loaded already
        total_warehouses = 1200
        no_regions = 0
        with open(conf_path, "r") as f:
            conf_data = f.readlines()
        for line in conf_data:
            if 'regions: {' in line:
                no_regions += 1
            elif 'num_partitions: ' in line:
                num_partitions = int(line.split('num_partitions: ')[1])
        target_warehouses_per_region = int(total_warehouses / no_regions)
        # Special case for Calvin, since we only have 1 region in that case
        if database == 'calvin':
            target_warehouses_per_region = int(total_warehouses / num_partitions)
        for ip in ips:
            print(f"Checking readiness on server on IP {ip}. It should have {target_warehouses_per_region} warehouses .....")
            try:
                ssh_target = f"{user}@{ip}" if user else ip
                server_container_cmd = f'docker container logs {server_container} 2>&1'
                ssh_cmd = f"ssh {ssh_target} '{server_container_cmd}'"
                result = run_subprocess(ssh_cmd, dry_run)
                warehouses_ready = [l for l in result.stdout.split('\n') if 'Loading orders in warehouse' in l]
                if len(warehouses_ready) < target_warehouses_per_region:
                    return False
            except:
                print(f"Unable to check loading status for IP: {ip}")
        return True
    else:
        return True

ips_used = get_server_ips_from_conf(conf_path=conf)
client_ips_used = get_client_ips_from_conf(conf_path=conf)
print(f"The IPs used in this experiment are: {ips_used}")
get_network_interfaces(ips_used=ips_used)

# Check that all network emulation settings are switched off
print("-------------------------------------------------------------------")
print("Removing any leftover network settings from previous experiments .....")
print("It's ok if the following removals fail. It's only a safeguard in case there were unremoved network settings from before.")
print()
simulate_network.remove_netem(ips=interfaces, user=user)
print("-------------------------------------------------------------------")
print()

if workload == 'tpcc':
    while not check_table_loading_finished(ips_used, workload, conf):
        time.sleep(10)
    print("All TPC-C tables loaded")

scenario_folder = f'data/{workload}/{scenario}' if trial_tag == "" else f'data/{workload}/{scenario}/{trial_tag}'
os.makedirs(scenario_folder, exist_ok=True)
# For now, we hard code this for the baseline exp (varying MH from 0 to 100) and just for Detock
tags = []
for system in systems_to_test:
    print("#####################")
    print(f"Testing system: {system}")
    systems_folder = f'{scenario_folder}/{system}'
    os.makedirs(systems_folder, exist_ok=True)
    # Run the benchmark for all x_vals and collect all results
    for x_val in x_vals:
        print("---------------------")
        print(f"Running experiment with x_val: {x_val}")
        tag = None
        cur_benchmark_params = benchmark_params.format(x_val, x_val) # Works for: baseline, skew, scalability, network, packet_loss
        cur_clients = clients if clients is not None else x_val
        cur_benchmark_cmd = single_benchmark_cmd.format(image=image, conf=conf, user=user, clients=cur_clients, generators=generators, duration=duration, benchmark_params=cur_benchmark_params, short_benchmark_log=short_benchmark_log)
        print(f"\n>>> Running: {cur_benchmark_cmd}")
        if scenario == 'network':
            # Emulate the network conditions first
            delay = f"{x_val}ms"
            jitter = f"{int(x_val / 10)}ms"
            loss = "0%"
        elif scenario == 'packet_loss':
            delay = "0ms"
            jitter = "0ms"
            loss = f"{x_val}%"
        # Note: the netem command may require allowing passwordless sudo for tc commands
        # I.e., add something like 'omraz ALL=(ALL) NOPASSWD: /usr/sbin/tc' to 'sudo visudo'
        if scenario == 'network' or scenario == 'packet_loss':
            simulate_network.apply_netem(delay=delay, jitter=jitter, loss=loss, ips=interfaces, user=user)
            print(f"All servers simulating an additional delay of {delay}, jitter of {jitter}, and packet loss of {loss}")
        elif workload == "pps":
            # Apply the inter-region delays
            simulate_regions.apply_all_delays(user=user, interfaces=interfaces)
        # End any leftover monitoring, and start a new monitoring of the outbound traffic on remote machines
        for ip in interfaces.keys():
            sp.run(f"ssh {user}@{ip} pkill -f net_traffic.csv", shell=True)
        start_net_monitor(user=user, interfaces=interfaces)
        # THE ACTUAL EXPERIMENT RUN
        result = run_subprocess(cur_benchmark_cmd, dry_run) #sp.run(cur_benchmark_cmd, shell=True, capture_output=True, text=True)
        # Print and collect output
        benchmark_cmd_log = ['']
        if not dry_run:
            print(result.stdout)
            print("[stderr]:", result.stderr)
            if result.returncode != 0:
                print(f"Benchmark command failed with exit code {result.returncode}!")
                #break
            # Get tag from benchmark cmd log
            benchmark_cmd_log = result.stdout.split('\n')
            for line in benchmark_cmd_log:
                if 'admin INFO: Tag: ' in line:
                    tag = line.split('admin INFO: Tag: ')[1]
        else:
            tag = 'dry_run'
        if tag is None:
            break
        tags.append(tag)
        cur_log_dir = log_dir.format(tag)
        # Make new (local) dir for storing result
        os.makedirs(cur_log_dir, exist_ok=True)
        # Store captured logs into file
        with open(f"{cur_log_dir}/{short_benchmark_log}", 'w') as f:
            for line in benchmark_cmd_log:
                f.write(f"{line}\n")
        # Remove any network restrictions
        if scenario == 'network' or scenario == 'packet_loss':
            # Remove emulated network conditions first
            simulate_network.remove_netem(ips=interfaces, user=user)
            print(f"Network settings on all servers back to normal!")
        elif workload == "pps":
            # Remove the inter-region delays
            simulate_regions.remove_all_delays(user=user, interfaces=interfaces)
        # Collect the metrics from all clients (TODO: add iftop metrics too)
        result = run_subprocess(collect_client_cmd.format(user=user, conf=conf, tag=tag), dry_run)
        if hasattr(result, "returncode") and result.returncode != 0:
            print(f"collect_client command failed with exit code {result.returncode}!")
            break
        collect_benchmark_container_cmd = f"docker container logs {benchmark_container} 2>&1"
        # Collect logs from all the benchmark container (for throughput)
        client_count = 0
        for client in client_ips_used:
            log_file_name = f"data/{tag}/raw_logs/benchmark_container_{client.replace('.', '_')}.log"
            ssh_cmd = f"ssh {user}@{client} '{collect_benchmark_container_cmd}'"
            result = run_subprocess(ssh_cmd, dry_run)
            if hasattr(result, "returncode") and result.returncode != 0:
                print(f"collect_benchmark_container command failed with exit code {result.returncode}!")
                break
            with open(log_file_name, 'w') as f:
                if not dry_run:
                    for line in result.stdout.split('\n'):
                        f.write(f"{line}\n")
        stop_and_collect_monitor(user, interfaces, cur_log_dir)
        # Save '.conf' file that was used to set up the cluster & experiment and ips with their respective regions
        shutil.copyfile(conf, os.path.join(cur_log_dir, conf.split('/')[-1]))
        if machine in ["st1", "st2", "st3", "st5"]:
            ips_file = 'examples/st_ips.json'
        else:
            ips_file = 'aws/ips.json'
        shutil.copyfile(ips_file, os.path.join(cur_log_dir, 'ips.json'))
        # Move and rename the folder accordingly
        if scenario == 'lat_breakdown': # For the latency breakdown we anyway just have 1 x_val
            target_folder = systems_folder
        else:
            target_folder = f'{systems_folder}/{x_val}'
        if os.path.exists(target_folder): # We need to do this to make sure 'shutil.move()' doesn't just dump the folder inside the target folder if it already exists
            shutil.rmtree(target_folder)
        shutil.move(f'data/{tag}', target_folder)

print("#####################")
zip_folder = f'{detock_dir}/{scenario_folder}'
zip_name = f'{scenario}_{trial_tag}' if trial_tag else scenario
print(f"\nAll {scenario} on {workload} experiments done (Trial: {trial_tag}). Zipping up files into {zip_folder}.zip ....")
shutil.make_archive(zip_folder, 'zip', zip_folder)
print("You can now copy logs with one of:")
print(f"scp -r {machine}:{zip_folder}/. plots/raw_data/{environment}/{workload}/{scenario}")
print(f"scp -r {machine}:{zip_folder}.zip plots/raw_data/{environment}/{workload}/{zip_name}.zip")
print("============================================")