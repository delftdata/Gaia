import os
from os.path import join, isdir
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import argparse
import re
from datetime import datetime
import json

import eval_systems

'''
Script for extracting the final results out of the logs and CSVs created during the experiment runs.
Intended to be run on own PC, just before the actual plotting of the results.
The script will populate the CSVs in 'plots/data' and generate a graph in 'plots/output'.
'''

VALID_SCENARIOS = ['baseline', 'skew', 'scalability', 'network', 'packet_loss', 'vary_hw', 'sunflower', 'example']
VALID_WORKLOADS = ['ycsb', 'tpcc', 'movr', 'movie', 'pps', 'dsh', 'smallbank']
DEFAULT_LAT_PERCENTILES = "50;99" # Extracted data will contain p50, p90, p95, p99. For the plots we will use p50, p95, p99
VALID_ENVIRONMENTS = ['local', 'st', 'aws']
SUPPORTED_VM_TYPES = ['t2.micro', 'm4.2xlarge',  'm5.2xlarge', 'r5.2xlarge', 'r5a.2xlarge', 'm5.4xlarge', 'r5.4xlarge',  'm8g.4xlarge', 'm6i.8xlarge']
# By default we use r5.4xlarge (following Detock's setup)
DEFAULT_VM_TYPE = 'r5.4xlarge'

# Argument parser
parser = argparse.ArgumentParser(description="Extract experiment results and plot graph for a given scenario.")
parser.add_argument('-s',  '--scenario', default='baseline', choices=VALID_SCENARIOS, help='Type of experiment scenario to analyze (default: baseline)')
parser.add_argument('-w',  '--workload', default='ycsb', choices=VALID_WORKLOADS, help='Workload to run (default: ycsb)')
parser.add_argument('-e',  '--environment', default='aws', choices=VALID_ENVIRONMENTS, help='What type of machine the experiment was run on.')
parser.add_argument('-it', '--instance_type', default=DEFAULT_VM_TYPE, choices=SUPPORTED_VM_TYPES, help='What type of machine the experiment was run on.')
parser.add_argument("-sa", "--skip_aborts", default=True, help="Whether or not to plot the aborts (since many workloads don't have any).")
parser.add_argument("-lp", "--latency_percentiles", default=DEFAULT_LAT_PERCENTILES, help="The latency percentiles to plot")
parser.add_argument("-rt", "--use_raw_tps", default=False, help="Whether or not to use the raw or 'real' throughputs.")
parser.add_argument("-sl", "--separate_latencies", default=False, help="Whether or not to separate latencies by txn type.")
parser.add_argument("-ll", "--log_latencies", default=True, help="Whether or not to plot the latency on a log scale.")
parser.add_argument("-ct", "--costs_per_txn", default=True, help="Whether or not to plot the cost per transaction.")

args = parser.parse_args()
scenario = args.scenario
workload = args.workload
env = args.environment
instance_type = args.instance_type
skip_aborts = args.skip_aborts
use_raw_tps = args.use_raw_tps
separate_latencies = args.separate_latencies
log_latencies = args.log_latencies
costs_per_txn = args.costs_per_txn

print(f"Extracting data for scenario: '{scenario}', workload: '{workload}' and environment: '{env}'")

# Define paths
BASE_DIR_PATH = join("plots/raw_data", env, workload, scenario)

out_csv = f'{scenario}.csv'
OUT_CSV_PATH = join("plots/data/", env, workload, out_csv)
SYSTEMS_LIST = ['Calvin', 'SLOG', 'Detock', 'Janus', 'Caerus', 'Mencius']
METRICS_LIST = ['throughput', 'p50', 'p90', 'p95', 'p99', 'aborts', 'bytes', 'cost']

MAX_YCSBT_HOT_RECORDS = 250.0 # Check whether this needs to be adjusted per current exp setup

# Hourly costs of storing a backup in S3. Note this estimates:
# 4 partitions x 8 regions x 128GB per machine = 4,096GB
# 4,096GB x 0.026 $/GB/month = #106 $/month => $0.15 / h
HOURLY_STORAGE_COST = 0.15

#              euw1  euw2  usw1  usw2  use1  use2  apne1 apne2
r54x_COSTS =  [1.128,1.184,1.12,1.008,1.008,1.008,1.216,1.216]
m52x_COSTS =  [0.428,0.444,0.448,0.384,0.384,0.384,0.496,0.472]
r52x_COSTS =  [0.564,0.592,0.56,0.504,0.504,0.504,0.608,0.608]
m54x_COSTS =  [0.856,0.888,0.896,0.768,0.768,0.768,0.992,0.944]
m6i8x_COSTS = [1.712,1.776,1.792,1.536,1.536,1.536,1.984,1.888]

# Constants for the hourly cost of deploying all the servers on m4.2xlarge VMs (each region has 4 VMs). Price as of 28.9.25
servers_per_region = 4
# Default costs
aws_regional_vm_costs = r54x_COSTS
if instance_type == 'r5.4xlarge':
    aws_regional_vm_costs = r54x_COSTS
elif instance_type == 'm5.2xlarge':
    aws_regional_vm_costs = m52x_COSTS
elif instance_type == 'm8g.4xlarge':
    aws_regional_vm_costs = [0.80032,0.8304,0.83776,0.71808,0.71808,0.71808,0.92752,0.88272]
elif instance_type == 'm6i.8xlarge':
    aws_regional_vm_costs = m6i8x_COSTS
elif instance_type == 'm4.2xlarge': # Maybe don't use this one because network bandwidth specs are vague
    aws_regional_vm_costs = [0.444,0.464,0.468,0.400,0.400,0.400,0.516,0.492]
elif instance_type == 'r5a.2xlarge':
    aws_regional_vm_costs = [0.508,0.532,0.504,0.452,0.452,0.452,0.548,0.544]
elif instance_type == 'm5.4xlarge':
    aws_regional_vm_costs = m54x_COSTS
elif instance_type == 'r5.2xlarge':
    aws_regional_vm_costs = r52x_COSTS
base_vm_cost = servers_per_region * sum(aws_regional_vm_costs)

# The cost of transferring 1GB of data out from the source region (the row). Price as of 22.9.25
if env == 'local':
    data_transfer_cost_matrix = [ # In the single computer setup, there is no cross-regional data transfer, so no cost either.
        # Possibly the whole cost part of the script could just be removed
        [0,0],
        [0,0]
    ]
    st_regions = ["us-west-1", "us-west-2"]
    data_transfer_cost_df = pd.DataFrame(data_transfer_cost_matrix, columns=st_regions, index=st_regions)
elif env == 'st':
    data_transfer_cost_matrix = [ # Here we just pretend that we have data transfer costs and make them uniform for all source, destination pairs
        [0,0.02], # 131.180.125.57
        [0.02,0]  # 131.180.125.40
        # [0,0.02,0.02,0.02,0.02,0.02,0.02,0.02], # euw1
        # [0.02,0,0.02,0.02,0.02,0.02,0.02,0.02], # euw2
        # [0.02,0.02,0,0.02,0.02,0.02,0.02,0.02], # usw1
        # [0.02,0.02,0.02,0,0.02,0.02,0.02,0.02], # usw2
        # [0.02,0.02,0.02,0.02,0,0.02,0.02,0.02], # use1
        # [0.02,0.02,0.02,0.02,0.02,0,0.02,0.02], # use2
        # [0.02,0.02,0.02,0.02,0.02,0.02,0,0.02], # apne1
        # [0.02,0.02,0.02,0.02,0.02,0.02,0.02,0]  # apne2
    ]
    st_regions = ["us-west-1", "us-west-2"]
    data_transfer_cost_df = pd.DataFrame(data_transfer_cost_matrix, columns=st_regions, index=st_regions)
elif env == 'aws':
    data_transfer_cost_matrix = [
        [0,0.02,0.02,0.02,0.02,0.02,0.02,0.02], # euw1
        [0.02,0,0.02,0.02,0.02,0.02,0.02,0.02], # euw2
        [0.02,0.02,0,0.02,0.02,0.02,0.02,0.02], # usw1
        [0.02,0.02,0.02,0,0.02,0.02,0.02,0.02], # usw2
        [0.02,0.02,0.02,0.02,0,0.01,0.02,0.02], # use1
        [0.02,0.02,0.02,0.02,0.01,0,0.02,0.02], # use2
        [0.09,0.09,0.09,0.09,0.09,0.09,0,0.09], # apne1
        [0.08,0.08,0.08,0.08,0.08,0.08,0.08,0]  # apne2
    ]
    aws_regions = ["us-west-1", "us-west-2", "us-east-1", "us-east-2", "eu-west-1", "eu-west-2", "ap-northeast-1", "ap-northeast-2"]
    data_transfer_cost_df = pd.DataFrame(data_transfer_cost_matrix, columns=aws_regions, index=aws_regions)

bytes_transfered_df = None

def extract_timestamp(timestamp_str):
    # Extract timestamp: I0430 10:14:36.795380
    ts_str = re.search(r"I\d{4} (\d{2}:\d{2}:\d{2}\.\d+)", timestamp_str).group(1)
    # Extract date part: 0430 (MMDD)
    date_part = re.search(r"I(\d{4})", timestamp_str).group(1)
    month, day = int(date_part[:2]), int(date_part[2:])
    # Convert to full datetime
    now = datetime.now()
    ts = datetime(now.year, month, day, *map(int, ts_str.split(":")[:2]), int(float(ts_str.split(":")[2])))
    # On st machines, the time seems shifted by 2 hours for some reason
    return int((ts.timestamp() + 7200) * 1000)

def summarize_bytes_sent(df, start_ts, end_ts):
    """
    Summarize total bytes sent to each destination between two timestamps.

    
    :param data: Loaded raw CSV data as a Pandas dataframe.
    :param start_ts: Start timestamp (ms since epoch).
    :param end_ts: End timestamp (ms since epoch).
    :return: A pandas DataFrame with destinations and total bytes sent.
    """
    # Filter rows within the timestamp range
    df_filtered = df[(df["Time"] >= start_ts) & (df["Time"] <= end_ts)]
    # Group by destination and sum the bytes sent
    summary = df_filtered.groupby("To")["FromBytes"].sum().reset_index()
    # Sort by total bytes descending
    summary = summary.sort_values(by="FromBytes", ascending=False)
    return summary

def get_server_ips_from_conf(conf_data):
    ips_used = []
    for line in conf_data:
        if '    addresses: ' in line:
            ips_used.append(line.split('    addresses: "')[1].split('"')[0])
    ips_used = list(ips_used)
    return ips_used

def get_relevant_throughput(benchmark_container_lines):
    for index, line in enumerate(benchmark_container_lines):
        # Sometimes we are waiting for further txns to finish completion, but no new ones are being sent, so we shouldn't count this section
        if 'Duration: ' in line:
            duration = int(line.split('Duration: ')[1])
        elif 'Committed: ' in line:
            txns_committed = int(line.split('Committed: ')[1])
            return round(txns_committed/duration)

# Load log files into strings
log_files = {}
tags = {}
throughputs = {}
input_throughputs = {}
start_timestamps = {}
end_timestamps = {}
system_dirs = [join(BASE_DIR_PATH, dir) for dir in os.listdir(BASE_DIR_PATH) if isdir(join(BASE_DIR_PATH, dir))]
x_val_set = set() # Note this may be different for different systems

# Load CSV files into pandas DataFrames
# Load .log filoes into lists
csv_files = {}
for system in system_dirs:
    csv_files[system.split('/')[-1]] = {}
    log_files[system.split('/')[-1]] = {}
    tags[system.split('/')[-1]] = {}
    throughputs[system.split('/')[-1]] = {}
    input_throughputs[system.split('/')[-1]] = {}
    start_timestamps[system.split('/')[-1]] = {}
    end_timestamps[system.split('/')[-1]] = {}
    x_vals = [join(system, dir) for dir in os.listdir(system) if dir.replace('.', '').isnumeric()]
    if scenario == 'vary_hw': # Exception because VM types are not numerical
        x_vals = [join(system, dir) for dir in os.listdir(system) if dir in SUPPORTED_VM_TYPES]
    x_val_set.update([x.split('/')[-1] for x in x_vals])
    print(x_vals)
    for x_val in x_vals:
        csv_files[system.split('/')[-1]][x_val.split('/')[-1]] = {}
        log_files[system.split('/')[-1]][x_val.split('/')[-1]] = {}
        throughputs[system.split('/')[-1]][x_val.split('/')[-1]] = 0 # Initialize throughputs to 0, then sum up across all clients
        input_throughputs[system.split('/')[-1]][x_val.split('/')[-1]] = 0
        clients = [join(x_val, 'client', obj) for obj in os.listdir(join(x_val, 'client')) if isdir(join(x_val, 'client', obj))]
        for client in clients:
            csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]] = {}
            log_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]] = {}
            # Read in all 4 extected files
            csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['metadata'] = pd.read_csv(join(client, 'metadata.csv'))
            csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['summary'] = pd.read_csv(join(client, 'summary.csv'))
            csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['transactions'] = pd.read_csv(join(client, 'transactions.csv'))
            csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['txn_events'] = pd.read_csv(join(client, 'txn_events.csv'))
            if 'benchmark_container.log' in os.listdir(client):
                with open(join(x_val, 'client', client.split('/')[-1], 'benchmark_container.log'), "r", encoding="utf-8") as f:
                    log_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['benchmark_container'] = f.read().split('\n')
                for line in log_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['benchmark_container']:
                    # Get the timestamp between the actual start and end of the experiment. We only need a rough extimate from 1 of the clients, so the can just overwrite each other
                    if 'Start sending transactions with' in line:
                        start_timestamps[system.split('/')[-1]][x_val.split('/')[-1]] = extract_timestamp(line)
                    if 'Results were written to' in line:
                        end_timestamps[system.split('/')[-1]][x_val.split('/')[-1]] = extract_timestamp(line)
                    elif 'Avg. TPS: ' in line:
                        if use_raw_tps:
                            throughputs[system.split('/')[-1]][x_val.split('/')[-1]] += int(line.split('Avg. TPS: ')[1])
                        else:
                            throughputs[system.split('/')[-1]][x_val.split('/')[-1]] += get_relevant_throughput(log_files[system.split('/')[-1]][x_val.split('/')[-1]][client.split('/')[-1]]['benchmark_container'])
        # For newer versions of the script we move the benchmark container logs back into the 'raw_logs' subdirectory
        raw_log_files = os.listdir(join(x_val, 'raw_logs'))
        benchmark_container_files = [file for file in raw_log_files if 'benchmark_container_' in file]
        for container in benchmark_container_files:
            client_ip = container.split('benchmark_container_')[1].split('.')[0]
            with open(join(x_val, 'raw_logs', container), "r", encoding="utf-8") as f:
                log_files[system.split('/')[-1]][x_val.split('/')[-1]][client_ip] = f.read().split('\n')
            phase = 'initialization'
            cur_input_throughput = 0
            fmt = "%m%d %H:%M:%S.%f" # Timestamp format (MMDD HH:MM:SS.microsec)
            for line in log_files[system.split('/')[-1]][x_val.split('/')[-1]][client_ip]:
                if scenario == 'scalability' and x_val.split('/')[-1] == '1' and phase == 'initialization': # Only use the single client config to figure out the 'max' possible input throughput per client
                    if 'benchmark.cpp:150] ' in line:
                        phase = 'sending'
                        start_timestamp_str = line.split()[0][1:] + " " + line.split()[1]
                        start_timestamp = datetime.strptime(start_timestamp_str, fmt)
                elif scenario == 'scalability' and phase == 'sending':
                    ###### TODO: Fix that we also take into account the time and then do txn/s
                    if ' S: 0 ' in line or 'Results were written to ' in line:
                        phase = 'stop_sending'
                    if not 'Results were written to ' in line:
                        end_timestamp_str = line.split()[0][1:] + " " + line.split()[1]
                        end_timestamp = datetime.strptime(end_timestamp_str, fmt)
                        cur_input_throughput = int(line.split(' S: ')[1].split('); C: ')[0].split(' (')[1]) / (end_timestamp-start_timestamp).total_seconds()
                if 'Avg. TPS: ' in line:
                    if use_raw_tps:
                        throughputs[system.split('/')[-1]][x_val.split('/')[-1]] += int(line.split('Avg. TPS: ')[1])
                    else:
                        throughputs[system.split('/')[-1]][x_val.split('/')[-1]] += get_relevant_throughput(log_files[system.split('/')[-1]][x_val.split('/')[-1]][client_ip])
                # Get the timestamp between the actual start and end of the experiment. We only need a rough extimate from one of the clients, so the can just overwrite each other
                elif 'Start sending transactions with' in line:
                    start_timestamps[system.split('/')[-1]][x_val.split('/')[-1]] = extract_timestamp(line)
                elif 'Results were written to' in line:
                    end_timestamps[system.split('/')[-1]][x_val.split('/')[-1]] = extract_timestamp(line)
            input_throughputs[system.split('/')[-1]][x_val.split('/')[-1]] += cur_input_throughput
    # Compute the 'max' possible input throughput scaling from the single client config
    if scenario == 'scalability':
        for x_val in x_vals:
            input_throughputs[system.split('/')[-1]][x_val.split('/')[-1]] = int(int(x_val.split('/')[-1]) * input_throughputs[system.split('/')[-1]]['1'])
print("All CSV files loaded")

for system in system_dirs:
    x_vals = [join(system, dir) for dir in os.listdir(system) if dir.replace('.', '').isnumeric()]
    if scenario == 'vary_hw': # Exception because VM types are not numerical
        x_vals = [join(system, dir) for dir in os.listdir(system) if dir in SUPPORTED_VM_TYPES]
    for x_val in x_vals:
        with open(join(x_val, 'raw_logs', 'benchmark_cmd.log'), "r", encoding="utf-8") as f:
            log_files[system.split('/')[-1]][x_val.split('/')[-1]]['benchmark_cmd'] = f.read().split('\n')
        #with open(join(x_val, 'raw_logs', 'benchmark_container.log'), "r", encoding="utf-8") as f:
        #    log_files[system.split('/')[-1]][x_val.split('/')[-1]]['benchmark_container'] = f.read().split('\n')
        log_file_names = os.listdir(join(x_val, 'raw_logs'))
        # Get the '.conf' file (for getting all the IPs involved)
        for file in log_file_names:
            if '.conf' in file:
                with open(join(x_val, 'raw_logs', file), "r", encoding="utf-8") as f:
                    log_files[system.split('/')[-1]][x_val.split('/')[-1]]['conf_file'] = f.read().split('\n')
            if '.json' in file:
                with open(join(x_val, 'raw_logs', file), "r", encoding="utf-8") as f:
                    log_files[system.split('/')[-1]][x_val.split('/')[-1]]['ips_file'] = json.loads(f.read())
        server_ips = get_server_ips_from_conf(log_files[system.split('/')[-1]][x_val.split('/')[-1]]['conf_file'])
        # For non-AWS environments, adjust the (fixed) VM cost based on server count
        # Assume an ST machine has the cost of an average AWS VM
        if env == 'st':
            avg_vm_cost = (base_vm_cost / servers_per_region) / len(aws_regional_vm_costs)
            vm_cost = len(server_ips) * avg_vm_cost
        # For single computer experiments, just count use the average cost of a single AWS VM
        elif env == 'local':
            vm_cost = (vm_cost / servers_per_region) / len(aws_regional_vm_costs)
        elif env == 'aws':
            vm_cost = base_vm_cost
            if scenario == 'vary_hw': # We need to recompute the base_vm_cost for each x_val (machine type)
                if [x_val.split('/')[-1]] == 'm5.2xlarge':
                    vm_cost = servers_per_region * sum(m52x_COSTS)
                elif [x_val.split('/')[-1]] == 'r5.2xlarge':
                    vm_cost = servers_per_region * sum(r52x_COSTS)
                elif [x_val.split('/')[-1]] == 'm5.4xlarge':
                    vm_cost = servers_per_region * sum(m54x_COSTS)
                elif [x_val.split('/')[-1]] == 'r5.4xlarge':
                    vm_cost = servers_per_region * sum(r54x_COSTS)
                elif [x_val.split('/')[-1]] == 'm6i.8xlarge':
                    vm_cost = servers_per_region * sum(m6i8x_COSTS)
        # Load all the network traffic data
        log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'] = {}
        for ip in server_ips:
            underscore_ip = ip.replace('.', '_')
            log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip] = pd.read_csv(join(x_val, 'raw_logs', f'net_traffic_{underscore_ip}.csv'))
            if log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip].columns[0].isnumeric(): # Sometimes we lose the header for some reason
                log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip] = pd.read_csv(join(x_val, 'raw_logs', f'net_traffic_{underscore_ip}.csv'), header=None)
                log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip].columns = ['timestamp_ms', 'bytes_sent']
        # Extract tag name from cmd log
        for line in log_files[system.split('/')[-1]][x_val.split('/')[-1]]['benchmark_cmd']:
            if 'admin INFO: Tag: ' in line:
                tag = line.split('admin INFO: Tag: ')[1]
            elif 'Synced config and ran command: benchmark ' in line:
                duration = int(line.split(' --duration ')[1].split(' ')[0])
        tags[system.split('/')[-1]][x_val.split('/')[-1]] = tag
        # Get the data transfers relavant to the experiment period
        for ip in log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'].keys():
            byte_log = log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip]
            timestamps = byte_log['timestamp_ms']
            lower_bound = timestamps[timestamps < start_timestamps[system.split('/')[-1]][x_val.split('/')[-1]]].max()
            upper_bound = timestamps[timestamps > end_timestamps[system.split('/')[-1]][x_val.split('/')[-1]]].min()
            filtered = byte_log[(timestamps > lower_bound) & (timestamps < upper_bound)]
            log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip] = filtered
print(f"All log files loaded")

# Get the latencies (p50, p90, p95, p99)
percentiles = [50, 90, 95, 99]
latencies = {}
for system in system_dirs:
    latencies[system.split('/')[-1]] = {}
    x_vals = [join(system, dir) for dir in os.listdir(system) if dir.replace('.', '').isnumeric()]
    if scenario == 'vary_hw': # Exception because VM types are not numerical
        x_vals = [join(system, dir) for dir in os.listdir(system) if dir in SUPPORTED_VM_TYPES]
    for x_val in x_vals:
        all_latencies = []
        lsh_latencies = []
        fsh_latencies = []
        mh_latencies = []
        clients = [obj for obj in os.listdir(join(x_val, 'client')) if isdir(join(x_val, 'client', obj))]
        for client in clients:
            client_txns = csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client]['transactions']
            client_txns = client_txns.astype({'regions': 'str'})
            client_txns["duration"] = client_txns["received_at"] - client_txns["sent_at"]
            client_txns["mp"] = client_txns["partitions"].astype(str).str.contains(';')
            if 'janus' in system.lower() or 'calvin' in system.lower():
                client_txns["lsh"] = True # Calvin & Janus don't have the notion of homes, so all txns are lsh
                client_txns["fsh"] = False
                client_txns["mh"] = False
            else:
                client_txns["lsh"] = (~client_txns["regions"].str.contains(';'))
                client_txns["fsh"] = (client_txns["regions"].str.contains(';')) & (~client_txns["partitions"].astype(str).str.contains(';'))
                client_txns["mh"] = client_txns["regions"].str.contains(';') & client_txns["partitions"].astype(str).str.contains(';')
            if workload == "pps":
                # Filter our the aborted transactions (they miss the `received_at` timestamp)
                client_txns = client_txns[client_txns["duration"] > 0]
            all_latencies.extend(list(client_txns["duration"]))
            lsh_latencies.extend(list(client_txns[client_txns["lsh"]]["duration"]))
            fsh_latencies.extend(list(client_txns[client_txns["fsh"]]["duration"]))
            mh_latencies.extend(list(client_txns[client_txns["mh"]]["duration"]))
        latency_percentiles = {}
        #latency_percentiles = {f"p{p}": np.percentile(np.array(all_latencies) / 1_000_000_000, p) for p in percentiles}
        latency_units_conversion = 1_000_000 # 1_000_000_000
        for p in percentiles:
            latency_percentiles[f"all_p{p}"] = np.percentile(np.array(all_latencies) / latency_units_conversion, p)
            latency_percentiles[f"lsh_p{p}"] = None if lsh_latencies == [] else np.percentile(np.array(lsh_latencies) / latency_units_conversion, p)
            latency_percentiles[f"fsh_p{p}"] = None if fsh_latencies == [] else np.percentile(np.array(fsh_latencies) / latency_units_conversion, p)
            latency_percentiles[f"mh_p{p}"] = None if mh_latencies == [] else np.percentile(np.array(mh_latencies) / latency_units_conversion, p)
        latencies[system.split('/')[-1]][x_val.split('/')[-1]] = latency_percentiles
print("All latencies extracted")

# Get the abort rate
abort_rates = {}
for system in system_dirs:
    abort_rates[system.split('/')[-1]] = {}
    x_vals = [join(system, dir) for dir in os.listdir(system) if dir.replace('.', '').isnumeric()]
    if scenario == 'vary_hw': # Exception because VM types are not numerical
        x_vals = [join(system, dir) for dir in os.listdir(system) if dir in SUPPORTED_VM_TYPES]
    for x_val in x_vals:
        total_txns = 0
        total_aborts = 0
        clients = [obj for obj in os.listdir(join(x_val, 'client')) if isdir(join(x_val, 'client', obj))]
        for client in clients:
            total_txns += csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client]['summary']['single_partition'].iloc[0]
            total_txns += csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client]['summary']['multi_partition'].iloc[0]
            total_aborts += csv_files[system.split('/')[-1]][x_val.split('/')[-1]][client]['summary']['aborted'].iloc[0]
        abort_rates[system.split('/')[-1]][x_val.split('/')[-1]] = 100 * total_aborts / total_txns
print("All aborts extracted")

# Get the byte transfers
# Here we will need to consider the duration of the experiemnt
byte_transfers = {}
total_costs = {}
fixed_costs_per_txn = {}
for system in system_dirs:
    byte_transfers[system.split('/')[-1]] = {}
    total_costs[system.split('/')[-1]] = {}
    fixed_costs_per_txn[system.split('/')[-1]] = {}
    x_vals = [join(system, dir) for dir in os.listdir(system) if dir.replace('.', '').isnumeric()]
    if scenario == 'vary_hw': # Exception because VM types are not numerical
        x_vals = [join(system, dir) for dir in os.listdir(system) if dir in SUPPORTED_VM_TYPES]
    for x_val in x_vals:
        start = start_timestamps[system.split('/')[-1]][x_val.split('/')[-1]]
        end = end_timestamps[system.split('/')[-1]][x_val.split('/')[-1]]
        if env == 'local' or env == 'st':
            no_clients = len(csv_files[system.split('/')[-1]][x_val.split('/')[-1]].keys())
            bytes_transfered_matrix = [ # The hard-coded values if we don't have real data, otherwise overwite this below
                [0, 1], # 131.180.125.57
                [1, 0]  # 131.180.125.40
                # [111,112,113,114,115,116,117,118], # euw1
                # [211,212,213,214,215,216,217,218], # euw2
                # [311,312,313,314,315,316,317,318], # usw1
                # [411,412,413,414,415,416,417,418], # usw2
                # [511,512,513,514,515,516,517,518], # use1
                # [611,612,613,614,615,616,617,618], # use2
                # [711,712,713,714,715,716,717,718], # apne1
                # [811,812,813,814,815,816,817,818]  # apne2
            ]
            if 'ips_file' in log_files[system.split('/')[-1]][x_val.split('/')[-1]].keys():
                ips_file = log_files[system.split('/')[-1]][x_val.split('/')[-1]]['ips_file']
                regions_used = ips_file.keys()
                bytes_transfered_df = pd.DataFrame(0, columns=regions_used, index=regions_used) # Rows are source, Cols are dest
                for region in regions_used:
                    cur_ips = []
                    for ip in log_files[system.split('/')[-1]][x_val.split('/')[-1]]['ips_file'][region]:
                        if ip['server']:
                            cur_ips.append(ip['ip'])
                    #cur_ips = [ip['ip'] for ip in log_files[system.split('/')[-1]][x_val.split('/')[-1]]['ips_file'][region]]
                    # Collect and summarize the data transfers for all ips in the current region
                    total_bytes_sent_per_location = 0
                    for ip in cur_ips:
                        total_bytes_sent_per_location += log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip]['bytes_sent'].sum() / (len(regions_used)-1)
                    bytes_transfered_df.loc[region] = total_bytes_sent_per_location
                    bytes_transfered_df.loc[region][region] = 0 # Fix for the 'self-sending cell' which doesn't actually cost anything
        elif env == 'aws':
            bytes_transfered_matrix = [
                [111,112,113,114,115,116,117,118], # euw1
                [211,212,213,214,215,216,217,218], # euw2
                [311,312,313,314,315,316,317,318], # usw1
                [411,412,413,414,415,416,417,418], # usw2
                [511,512,513,514,515,516,517,518], # use1
                [611,612,613,614,615,616,617,618], # use2
                [711,712,713,714,715,716,717,718], # apne1
                [811,812,813,814,815,816,817,818]  # apne2
            ]
            private_server_ips = get_server_ips_from_conf(log_files[system.split('/')[-1]][x_val.split('/')[-1]]['conf_file'])
            num_partitions = -1
            for line in log_files[system.split('/')[-1]][x_val.split('/')[-1]]['conf_file']:
                if 'num_partitions: ' in line:
                    num_partitions = int(line.split('num_partitions: ')[1])
            regions_used = []
            for i in range(int(len(private_server_ips)/num_partitions)):
                regions_used.append(private_server_ips[num_partitions*i : num_partitions*i + num_partitions])
            aws_regions = ['euw1', 'euw2', 'usw1', 'usw2', 'use1', 'use2', 'apne1', 'apne2']
            bytes_transfered_df = pd.DataFrame(0, columns=aws_regions, index=aws_regions) # Rows are source, Cols are dest
            for i, region in enumerate(regions_used):
                cur_transfers = 0
                for ip in region:
                    cur_transfers += log_files[system.split('/')[-1]][x_val.split('/')[-1]]['net_traffic_logs'][ip]['bytes_sent'].sum()
                for aws_region in aws_regions:
                    bytes_transfered_df.at[aws_regions[i], aws_region] = cur_transfers / len(aws_regions)
                bytes_transfered_df.at[aws_regions[i], aws_regions[i]] = 0 # Fix for the 'self-sending cell' which doesn't actually cost anything
        else:
            bytes_transfered_matrix = [
                [0,0,0,0,0,0,0,0], # euw1
                [0,0,0,0,0,0,0,0], # euw2
                [0,0,0,0,0,0,0,0], # usw1
                [0,0,0,0,0,0,0,0], # usw2
                [0,0,0,0,0,0,0,0], # use1
                [0,0,0,0,0,0,0,0], # use2
                [0,0,0,0,0,0,0,0], # apne1
                [0,0,0,0,0,0,0,0]  # apne2
            ]
        total_bytes_transfered = 0
        total_data_transfer_cost = 0
        if bytes_transfered_df is None:
            for i in range(len(data_transfer_cost_matrix)):
                for j in range(len(data_transfer_cost_matrix[0])):
                    total_bytes_transfered += bytes_transfered_matrix[i][j]
                    total_data_transfer_cost += data_transfer_cost_matrix[i][j] * bytes_transfered_matrix[i][j]
        else:
            if env == 'aws':
                for i, aws_region_row in enumerate(aws_regions):
                    for j, aws_region_col in enumerate(aws_regions):
                        total_bytes_transfered += bytes_transfered_df.at[aws_region_row, aws_region_col]
                        total_data_transfer_cost += data_transfer_cost_matrix[i][j] * bytes_transfered_df.at[aws_region_row, aws_region_col] / 1_000_000_000
            else:
                for i in range(len(list(regions_used))):
                    for j in range(len(list(regions_used))):
                        total_bytes_transfered += bytes_transfered_df.loc[list(regions_used)[i]][list(regions_used)[j]]
                        total_data_transfer_cost += data_transfer_cost_matrix[i][j] * bytes_transfered_df.loc[list(regions_used)[i]][list(regions_used)[j]] / 1_000_000_000
        total_hourly_cost = HOURLY_STORAGE_COST + vm_cost + (total_data_transfer_cost/duration) * 3600
        byte_transfers[system.split('/')[-1]][x_val.split('/')[-1]] = (total_bytes_transfered/duration) # * 3600 / 1_000
        if costs_per_txn: # If we plot per txn cost, use cents intead of dollars. Also we plot cost per 10k transactions
            total_costs[system.split('/')[-1]][x_val.split('/')[-1]] = ((total_hourly_cost / 3600) / throughputs[system.split('/')[-1]][x_val.split('/')[-1]]) * 100 * 10_000
            fixed_costs_per_txn[system.split('/')[-1]][x_val.split('/')[-1]] = (((HOURLY_STORAGE_COST + vm_cost) / 3600) / throughputs[system.split('/')[-1]][x_val.split('/')[-1]]) * 100 * 10_000
        else:
            total_costs[system.split('/')[-1]][x_val.split('/')[-1]] = total_hourly_cost
print("All byte transfers & costs extracted")

# Write the obtained values to file ('x_var' is the x-axis value for the row). We need to store the following variable (populated above)
# 'x_var_val' (is it does not exist yet), 'throughput', 'latency_percentiles['p50']', 'latency_percentiles['p90']', 'latency_percentiles['p95']', 'latency_percentiles['p99']',
# 'abort_rate', 'bytes_transfered', 'total_hourly_cost'

# For mow we will give 4 latencies (p50, p90, p95, p99) and later pick which one we actually want to plot

colnames = ['x_var']
df = pd.DataFrame(data=[], columns=colnames)
for x_val in x_val_set:
    x_val = x_val.split('/')[-1]
    new_row = {col: np.nan for col in df.columns}
    if scenario == 'skew':
        if workload in ['tpcc', 'movr', 'movie', 'pps', 'dsh']:
            new_row['x_var'] = float(x_val)
        else:
            new_row['x_var'] = (MAX_YCSBT_HOT_RECORDS - float(x_val)) / MAX_YCSBT_HOT_RECORDS
    elif scenario == 'vary_hw':
        new_row['x_var'] = x_val
    else:
        new_row['x_var'] = float(x_val)
    for system in system_dirs:
        sys_name = system.split('/')[-1]
        # In case there is an inconsistency in x_values measures
        if x_val.split('/')[-1] in throughputs[system.split('/')[-1]].keys():
            new_row[f'{sys_name}_throughput'] = throughputs[system.split('/')[-1]][x_val.split('/')[-1]]
            new_row[f'{sys_name}_p50'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['all_p50']
            new_row[f'{sys_name}_p90'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['all_p90']
            new_row[f'{sys_name}_p95'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['all_p95']
            new_row[f'{sys_name}_p99'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['all_p99']
            new_row[f'{sys_name}_lsh_p50'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['lsh_p50']
            new_row[f'{sys_name}_lsh_p90'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['lsh_p90']
            new_row[f'{sys_name}_lsh_p95'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['lsh_p95']
            new_row[f'{sys_name}_lsh_p99'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['lsh_p99']
            new_row[f'{sys_name}_fsh_p50'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['fsh_p50']
            new_row[f'{sys_name}_fsh_p90'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['fsh_p90']
            new_row[f'{sys_name}_fsh_p95'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['fsh_p95']
            new_row[f'{sys_name}_fsh_p99'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['fsh_p99']
            new_row[f'{sys_name}_mh_p50'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['mh_p50']
            new_row[f'{sys_name}_mh_p90'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['mh_p90']
            new_row[f'{sys_name}_mh_p95'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['mh_p95']
            new_row[f'{sys_name}_mh_p99'] = latencies[system.split('/')[-1]][x_val.split('/')[-1]]['mh_p99']
            new_row[f'{sys_name}_aborts'] = abort_rates[system.split('/')[-1]][x_val.split('/')[-1]]
            new_row[f'{sys_name}_bytes'] = byte_transfers[system.split('/')[-1]][x_val.split('/')[-1]]
            new_row[f'{sys_name}_cost'] = total_costs[system.split('/')[-1]][x_val.split('/')[-1]]
            if scenario == 'scalability':
                new_row[f'{sys_name}_input_throughput'] = input_throughputs[system.split('/')[-1]][x_val.split('/')[-1]]
            elif scenario == 'vary_hw':
                new_row[f'{sys_name}_fixed_cost'] = fixed_costs_per_txn[system.split('/')[-1]][x_val.split('/')[-1]]
    # Append the row
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

# For the vary HW scenario, we custom sort the rows by machine type strength
if scenario == 'vary_hw':
    vm_type_by_power_order = ["m5.2xlarge", "r5.2xlarge", "m5.4xlarge", "r5.4xlarge", "m6i.8xlarge"] # Define categorical type with this order
    cat_type = CategoricalDtype(categories=vm_type_by_power_order, ordered=True)
    df['x_var'] = df['x_var'].astype(cat_type)
df = df.sort_values('x_var')
os.makedirs('/'.join(OUT_CSV_PATH.split('/')[:-1]), exist_ok=True)
df.to_csv(OUT_CSV_PATH, index=False)

# Create plots directly
latencies = [int(latency) for latency in args.latency_percentiles.split(';')]
if scenario != 'vary_hw':
    eval_systems.make_plot(plot=scenario,
                           workload=workload,
                           env=env,
                           latency_percentiles=latencies,
                           skip_aborts=skip_aborts,
                           separate_latencies=separate_latencies,
                           log_latencies=log_latencies,
                           costs_per_txn=costs_per_txn)
else:
    eval_systems.make_hw_plot(workload=workload,
                              env=env,
                              latency_percentiles=latencies,
                              skip_aborts=skip_aborts,
                              separate_latencies=separate_latencies,
                              log_latencies=log_latencies,
                              costs_per_txn=costs_per_txn)

print("Done")
