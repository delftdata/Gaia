import argparse
import boto3
import copy
import csv
import itertools
import json
import os
import datetime
import logging
import shlex
import random
from collections import defaultdict
from tempfile import gettempdir
from time import sleep
from multiprocessing import Process
from pprint import pprint

import google.protobuf.text_format as text_format

import admin
from proto.configuration_pb2 import Configuration, Region

LOG = logging.getLogger("experiment")

NAME = "ycsb"
# Parameters of the workload
WORKLOAD_PARAMS = []
# Parameters of the benchmark tool and the environment other than the 'params' argument
OTHER_PARAMS = [
    "generators",
    "clients",
    "txns",
    "duration",
    "rate_limit",
    "num_partitions",
]
DEFAULT_PARAMS = {
    "generators": 2,
    "rate_limit": 0,
    "txns": 2000000,
    "num_partitions": None,
}

# Params specific to YCSB-T experiment
WORKLOAD_PARAMS = [
    "writes",
    "records",
    "hot_records",
    "mp_parts",
    "mh_homes",
    "mh_zipf",
    "hot",
    "mp",
    "mh",
]
DEFAULT_PARAMS = {**DEFAULT_PARAMS, **{
    "writes": 10,
    "records": 10,
    "hot_records": 2,
    "mp_parts": 2,
    "mh_homes": 2,
    "mh_zipf": 1,
}}

settings_file_name = 'settings'
settings_path = f'experiments/{settings_file_name}.json'
skip_starting_server = False

# Basically figure out which combinations of parameters we want to really test in our experiment
def combine_parameters(params, default_params, workload_settings):
    common_values = {}
    ordered_value_lists = []
    for p in params:
        if p in workload_settings:
            value_list = workload_settings[p]
            if isinstance(value_list, list):
                ordered_value_lists.append([(p, v) for v in value_list])
            else:
                common_values[p] = value_list
    combinations = [dict(v) for v in itertools.product(*ordered_value_lists)]
    # Apply combinations inclusion
    if "include" in workload_settings:
        patterns = workload_settings["include"]
        # Resize extra to be the same size as combinations
        extra = [{} for _ in range(len(combinations))]
        # List of extra combinations
        new = []
        for p in patterns:
            is_new = True
            for c, e in zip(combinations, extra):
                overlap_keys = p.keys() & c.keys()
                if all([c[k] == p[k] for k in overlap_keys]):
                    is_new = False
                    e.update({k:p[k] for k in p if k not in overlap_keys})
            if is_new:
                new.append(p)
        for c, e in zip(combinations, extra):
            c.update(e)
        combinations += new
    # Apply combinations exclusion
    if "exclude" in workload_settings:
        patterns = workload_settings["exclude"]
        combinations = [c for c in combinations if not any([c.items() >= p.items() for p in patterns])]
    # Populate common values and check for missing/unknown params
    params_set = set(params)
    for c in combinations:
        c.update(common_values)
        for k, v in default_params.items():
            if k not in c:
                c[k] = v
        missing = params_set - c.keys()
        if missing:
            raise KeyError(f"Missing required param(s) {missing} in {c}")
        unknown = c.keys() - params_set
        if unknown:
            raise KeyError(f"Unknown param(s) {unknown} in {c}")
    return combinations

def generate_config(settings: dict, template_path: str, orig_num_partitions: int, num_log_mangers: int):
    config = Configuration()
    with open(template_path, "r") as f:
        text_format.Parse(f.read(), config)
    regions_ids = {name: id for id, name in enumerate(settings["regions"])}
    num_partitions = orig_num_partitions
    for r in settings["regions"]:
        region = Region()
        public_ips = settings["servers_public"][r]
        private_ips = settings["servers_private"][r]
        # Normally we determine the number of partitions by the number of IPs given
        if num_partitions is None:
            num_partitions = len(public_ips)
        if len(public_ips) < num_partitions:
            raise RuntimeError(f"Not enough public ips for region '{r}' ({len(public_ips)} < {num_partitions})")
        if len(private_ips) < num_partitions:
            raise RuntimeError(f"Not enough private ips for region '{r}' ({len(private_ips)} < {num_partitions})")
        # Remove any excess IPs?? (If some region has more machines spawned?)
        servers_private = [addr.encode() for addr in private_ips[:num_partitions]]
        region.addresses.extend(servers_private)
        servers_public = [addr.encode() for addr in public_ips[:num_partitions]]
        region.public_addresses.extend(servers_public)
        clients = [addr.encode() for addr in settings["clients"][r]]
        region.client_addresses.extend(clients)
        distance_ranking = [str(other_r) if isinstance(other_r, int) else str(regions_ids[other_r]) for other_r in settings["distance_ranking"][r]]
        region.distance_ranking = ",".join(distance_ranking)
        if "num_replicas" in settings:
            region.num_replicas = settings["num_replicas"].get(r, 1)
        else:
            region.num_replicas = 1
        if "shrink_mh_orderer" in settings:
            region.shrink_mh_orderer = settings["shrink_mh_orderer"].get(r, False)
        region.sync_replication = settings.get("local_sync_replication", False)           
        config.regions.append(region)
    config.num_partitions = num_partitions
    if num_log_mangers is not None:
        config.num_log_managers = num_log_mangers
    # Quick hack to change the number of keys based on number of partitions for the scalability experiment
    if orig_num_partitions is not None:
        config.simple_partitioning.num_records = orig_num_partitions * 1000000
    config_filename, config_ext = os.path.splitext(os.path.basename(template_path))
    if orig_num_partitions is not None:
        config_filename += f"-{orig_num_partitions}"
    # Saving the config used in a file
    config_path = os.path.join(gettempdir(), f"{config_filename}{config_ext}")
    with open(config_path, "w") as f:
        text_format.PrintMessage(config, f)
    return config_path

def cleanup(username: str, config_path: str, image: str):
    LOG.info("STOP ANY RUNNING EXPERIMENT")
    cleanup_cmd = ["benchmark", config_path, "--user", username, "--image", image, "--cleanup", "--clients", "1", "--txns", "0"]
    LOG.info("Cleaning up with command %s", cleanup_cmd)
    admin.main(cleanup_cmd)

def start_server(username: str, config_path: str, image: str, binary="slog"):
    start_server_cmd = ["start", config_path, "--user", username, "--image", image, "--bin", binary]
    LOG.info("START SERVERS with command %s", start_server_cmd)
    admin.main(start_server_cmd)
    wait_for_servers_up_cmd = ["collect_server", config_path, "--user", username, "--image", image, "--flush-only", "--no-pull"]
    LOG.info("WAIT FOR ALL SERVERS TO BE ONLINE with command %s", wait_for_servers_up_cmd)
    admin.main(wait_for_servers_up_cmd)

def collect_client_data(username: str, config_path: str, out_dir: str, tag: str):
    collect_client_cmd = ["collect_client", config_path, tag, "--user", username, "--out-dir", out_dir]
    LOG.info("Collecting server data with command %s", collect_client_cmd)
    admin.main(collect_client_cmd)

def collect_server_data(username: str, config_path: str, image: str, out_dir: str, tag: str):
    # fmt: off
    # The image has already been pulled when starting the servers, so use "--no-pull"
    collect_server_cmd = ["collect_server", config_path, "--tag", tag, "--user", username, "--image", image,"--out-dir", out_dir, "--no-pull"]
    LOG.info("Collecting server data with command %s", collect_server_cmd)
    admin.main(collect_server_cmd)
    # fmt: on

def collect_data(username: str, config_path: str, image: str, out_dir: str, tag: str, no_client_data: bool, no_server_data: bool):
    collectors = []
    if not no_client_data:
        collectors.append(Process(target=collect_client_data, args=(username, config_path, out_dir, tag)))
    if not no_server_data:
        collectors.append(Process(target=collect_server_data, args=(username, config_path, image, out_dir, tag)))
    for p in collectors:
        p.start()
    for p in collectors:
        p.join()

def run_benchmark(args, image, settings, config_path, config_name, values):
    out_dir = os.path.join(args.out_dir, NAME)
    sample = settings.get("sample", 10)
    trials = settings.get("trials", 1)
    LOG.info(f'Running benchmark {NAME} with {trials} trials and sampling {sample}%')
    # For now we don't add tags
    for val in values:
        for t in range(trials):
            params = ",".join(f"{k}={val[k]}" for k in WORKLOAD_PARAMS)
            LOG.info("Params string: %s", params)
            # fmt: off
            tag = 'test'
            benchmark_args = [
                "benchmark",
                config_path,
                "--user", settings["username"],
                "--image", image,
                "--workload", workload_settings["workload"],
                "--clients", f"{val['clients']}",
                "--rate", f"{val['rate_limit']}",
                "--generators", f"{val['generators']}",
                "--txns", f"{val['txns']}",
                "--duration", f"{val['duration']}",
                "--sample", f"{sample}",
                "--seed", f"{args.seed}",
                "--params", params,
                "--tag", tag, # For now we ignore setting custom tags
                # The image has already been pulled in the cleanup step
                "--no-pull",
            ]
            LOG.info("Running benchmark command with config %s", benchmark_args)
            # fmt: on
            admin.main(benchmark_args)
            LOG.info("Collecting data")
            collect_data(settings["username"], config_path, image, out_dir, tag, False, False) # Always collect ALL data

with open(settings_path, "r") as f:
    settings = json.load(f)
LOG.info("================================================")
LOG.info(f"Running experiment at time {datetime.datetime.now()} with config from file {settings_path}")
LOG.info(settings)

workload_settings = settings[NAME]
params = OTHER_PARAMS + WORKLOAD_PARAMS

all_values = combine_parameters(params, DEFAULT_PARAMS, workload_settings)

# num_parts_to_values is probably useless???
num_parts_to_values = defaultdict(list)
for v in all_values:
    num_parts_to_values[v["num_partitions"]].append(v)

LOG.info("The tested combinations will be:")
LOG.info(all_values)

num_log_managers = workload_settings.get("num_log_managers", None)

# The DB systems that will the tried (e.g., Detock, SLOG, etc.). Iterate through the desiged systems and run the experiment on them
LOG.info('Will run the following DB configs: %s', workload_settings["servers"])
for server in workload_settings["servers"]:
    template_path = f'experiments/{server["config"]}'
    # Special config that contains all server IP addresses
    cleanup_config_path = generate_config(settings, template_path, None, num_log_managers)
    for num_partitions, values in num_parts_to_values.items():
        config_path = generate_config(settings, template_path, num_partitions, num_log_managers)
        LOG.info('============ GENERATED CONFIG "%s" ============', config_path)
        cleanup(settings["username"], cleanup_config_path, server["image"])
        # Spin up system Docker containers (if not already up)
        if not skip_starting_server:
            start_server(settings["username"], config_path, server["image"], server.get("binary", "slog"))
        LOG.info('Servers set up!')
        config_name = os.path.splitext(os.path.basename(server["config"]))[0]
        # Special case only for scalability experiment
        if num_partitions is not None:
            config_name += f"-sz{num_partitions}"
        run_benchmark([], server["image"], settings, config_path, config_name, values)





# Suggestion rather than using all this crap, just create a new script that does what we have in the Google Doc instructions (very last version)

# After we run all the experiments, collect results over scp


print("Done")