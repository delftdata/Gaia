import os
from os.path import join
import subprocess as sp
import shutil
import argparse

import simulate_network
#import run_config_on_remote

'''
Script to run experiments for ALL systems for a specific scenario.
It uses the logic of the 'run_config_on_remote.py' script, but also takes care of spining up and tearing down the cluster for each system tested
'''

VALID_SCENARIOS = ['baseline', 'skew', 'scalability', 'network', 'packet_loss', 'sunflower', 'lat_breakdown', 'vary_hw']
VALID_WORKLOADS = ['ycsb', 'tpcc', 'movr', 'movie', 'pps', 'dsh', 'smallbank']
USED_DATABASES =  ['calvin', 'ddr_only', 'ddr_ts', 'janus', 'slog']

parser = argparse.ArgumentParser(description="Run Detock experiment with a given scenario.")
parser.add_argument('-s',  '--scenario', default='skew', choices=VALID_SCENARIOS, help='Type of experiment scenario to run (default: baseline)')
parser.add_argument('-w',  '--workload', default='ycsb', choices=VALID_WORKLOADS, help='Workload to run (default: ycsb)')
parser.add_argument('-cf', '--conf_folder', default='examples/ycsb/lat_breakdown', help='Folder with the conf files for each system. Make sure their name contains the corresponding string in USED_DATABASES')
parser.add_argument('-i',  '--img', default='omraz/seq_eval:latest', help='The Docker image of your built Detock system')
parser.add_argument('-d',  '--duration', default=60, help='Duration (in seconds) of a single experiment')
parser.add_argument('-dr', '--dry_run', default=False, help='Whether to run this as a dry run')
parser.add_argument('-u',  '--user', default='omraz', help='Username when logging into a remote machine')
parser.add_argument('-m',  '--machine', default='st5', help='The machine from which this script is (used to write out the scp command for collecting the results.)')
parser.add_argument('-b',  '--benchmark_container', default='benchmark', help='The name of the benchmark container (so your experiment doesn\'t interfere with others)')
parser.add_argument('-sc', '--server_container', default='slog', help='The name of the server container')

args = parser.parse_args()
scenario = args.scenario
workload = args.workload
conf_folder = args.conf_folder
image = args.img
duration = args.duration
dry_run = args.dry_run
user = args.user
machine = args.machine
benchmark_container = args.benchmark_container
server_container = args.server_container

detock_dir = os.path.expanduser("~/Detock")

def run_subprocess(cmd, dry_run=False):
    if dry_run:
        print(f"Would have run command: {cmd}")
        return True # TODO: fix properly
    else:
        return sp.run(cmd, shell=True, capture_output=True, text=True)

def start_database(conf_file, binary):
    start_db_command = f"python3 tools/admin.py start --image {image} {conf_file} -u {user} -e GLOG_v=1 --bin {binary}"
    result = run_subprocess(start_db_command)
    if hasattr(result, "returncode") and result.returncode != 0:
        print(f"Starting database command failed with exit code {result.returncode}!")
    else:
        print(f"Database with conf file: {conf_file} started!")

def stop_database(conf_file):
    stop_db_command = f"python3 tools/admin.py stop --image {image} {conf_file} -u {user}"
    result = run_subprocess(stop_db_command)
    if hasattr(result, "returncode") and result.returncode != 0:
        print(f"Stopping database command failed with exit code {result.returncode}!")
    else:
        print(f"Database with conf file: {conf_file} stopped!")

def run_database_experiment(conf_file, system):
    run_db_exp_command = f"python3 tools/run_config_on_remote.py -i {image} -m {machine} -s {scenario} -w {workload} -c {conf_file} -u {user} -db {system}"
    result = run_subprocess(run_db_exp_command)
    if hasattr(result, "returncode") and result.returncode != 0:
        print(f"Running {system} database experiment command failed with exit code {result.returncode}!")
    else:
        print(f"Successful preformed all {scenario} experiments with {system}!")

# Stop and leftover running system from before
stop_database(conf_file=join(conf_folder, os.listdir(conf_folder)[0])) # For the stopping of the cluster it doesn't matter which '.conf' file we use.

# Main experiment loop
print(f"Running scenario: '{scenario}' and workload: '{workload}' on the systems {USED_DATABASES}")
conf_files = [join(conf_folder, file) for file in os.listdir(conf_folder)]
for system in USED_DATABASES:
    print("***************************************************")
    print(f"Testing system: {system}")
    cur_conf_file = ''
    for conf_file in conf_files:
        if system in conf_file:
            cur_conf_file = conf_file
    if cur_conf_file == '':
        print(f"Conf file for {system} not found. Make sure it is in {conf_folder}")
    # Phase 1: Spin up database
    if system == 'janus':
        binary = 'janus'
    else:
        binary = 'slog'
    start_database(conf_file=cur_conf_file, binary=binary)
    # Phase 2: Run the experiments for a single database system in a single scenario
    run_database_experiment(conf_file=cur_conf_file, system=system)
    # Phase 3: Stop the database
    stop_database(conf_file=cur_conf_file)

print("#####################")
print(f"\nAll systems evaluated on {scenario} on {workload}. Zipping up files into {detock_dir}/data/{workload}/{scenario}.zip ....")
shutil.make_archive(f"{detock_dir}/data/{workload}/{scenario}", 'zip', f"{detock_dir}/data/{workload}/{scenario}")
print("You can now copy logs with one of:")
print(f"scp -r {machine}:{detock_dir}/data/{workload}/{scenario} plots/raw_data/{workload}")
print(f"scp -r {machine}:{detock_dir}/data/{workload}/{scenario}.zip plots/raw_data/{workload}")
print("============================================")