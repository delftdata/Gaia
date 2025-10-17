import os
from os.path import join, isdir
import sys
import numpy as np
import pandas as pd
import polars as pl
import argparse
import re
from datetime import datetime
import json

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize
from matplotlib.ticker import MaxNLocator
import seaborn as sns

'''
Script for decomposing the transactional latency into individual components and making a heatmap.
'''

# Conversion factor: nanoseconds to milliseconds
NANO_TO_MS = 1e-6

VALID_SCENARIOS = ['baseline', 'skew', 'scalability', 'network', 'packet_loss', 'sunflower', 'example']
VALID_WORKLOADS = ['ycsb', 'tpcc', 'movr', 'movie', 'pps', 'dsh', 'smallbank']

SYSNAME_MAP = {
    'janus': 'Janus',
    'ddr_ts': 'Detock',
    'calvin': 'Calvin',
    'slog': 'SLOG',
}

# Argument parser
parser = argparse.ArgumentParser(description="Decompose the latency of transactions into components and plot graph stacked bar chart.")
parser.add_argument('-df', '--data_folder', default='plots/raw_data/st/ycsb/lat_breakdown', help='Path to folder with raw data')
parser.add_argument('-w',  '--workload', default='ycsb', choices=VALID_WORKLOADS, help='Workload evaluated (default: ycsb)')
parser.add_argument('-o',  '--output_folder', default='plots/data/final/ycsb/latency_breakdown', help='Folder where to store the processed data')

args = parser.parse_args()
data_folder = args.data_folder
workload = args.workload
output_folder = args.output_folder

system_dirs = os.listdir(data_folder)
# This is just to order the DBs in the chronological order used in the rest of the paper
ordered_system_dirs = []
if 'calvin' in system_dirs:
    ordered_system_dirs.append('calvin')
    system_dirs.remove('calvin')
if 'slog' in system_dirs:
    ordered_system_dirs.append('slog')
    system_dirs.remove('slog')
if 'ddr_ts' in system_dirs:
    ordered_system_dirs.append('ddr_ts')
    system_dirs.remove('ddr_ts')
if 'detock' in system_dirs: # In case Detock is named differently
    ordered_system_dirs.append('detock')
    system_dirs.remove('detock')
if 'janus' in system_dirs:
    ordered_system_dirs.append('janus')
    system_dirs.remove('janus')
ordered_system_dirs.extend(system_dirs)

ordered_system_dirs = [system for system in ordered_system_dirs if '.' not in system]
ordered_system_dirs = [system for system in ordered_system_dirs if 'ddr_only' not in system]
summary_combined = pl.DataFrame(schema=['System'])
#summary_combined = pd.DataFrame(columns=['System'])

def extract_component_times(cur_events):
    NANO_TO_MS = 1e-6
    # Define state variables
    current_stage = None
    stage_start_time = None
    # Duration accumulators
    stage_durations = {
        "server": 0.0,
        "forwarder": 0.0,
        "mh_orderer": 0.0,
        "sequencer": 0.0,
        "log_manager": 0.0,
        "scheduler": 0.0,
        "lck_man": 0.0,
        "worker": 0.0,
        "idle": 0.0
    }
    # Define mapping of ENTER/EXIT events to stages
    stage_enter = {
        "ENTER_SERVER": "server",
        "RETURN_TO_SERVER": "server",
        "EXIT_SERVER_TO_FORWARDER": "forwarder", # Special case for Janus
        "ENTER_FORWARDER": "forwarder",
        "ENTER_MULTI_HOME_ORDERER": "mh_orderer",
        "ENTER_MULTI_HOME_ORDERER_IN_BATCH": "mh_orderer",
        "ENTER_SEQUENCER": "sequencer",
        "ENTER_SEQUENCER_IN_BATCH": "sequencer",
        "EXIT_SEQUENCER_IN_BATCH": "idle",
        "ENTER_LOG_MANAGER_IN_BATCH": "log_manager",
        "ENTER_LOG_MANAGER_ORDER": "log_manager",
        "ENTER_SCHEDULER": "scheduler",
        "ENTER_SCHEDULER_LO": "scheduler",
        "ENTER_LOCK_MANAGER": "lck_man",
        "ENTER_WORKER": "worker",
    }
    stage_exit = {
        "EXIT_SERVER_TO_CLIENT": "server",
        "EXIT_SERVER_TO_FORWARDER": "server",
        "EXIT_FORWARDER_TO_SEQUENCER": "forwarder",
        "ENTER_WORKER": "forwarder", # Special case for Janus
        "EXIT_FORWARDER_TO_MULTI_HOME_ORDERER": "forwarder",
        "EXIT_MULTI_HOME_ORDERER_IN_BATCH": "mh_orderer",
        "EXIT_MULTI_HOME_ORDERER": "mh_orderer",
        "EXIT_SEQUENCER_IN_BATCH": "sequencer",
        "ENTER_LOG_MANAGER_IN_BATCH": "idle",
        "EXIT_LOG_MANAGER": "log_manager",
        "DISPATCHED": "scheduler",
        "DISPATCHED_FAST": "scheduler",
        "DISPATCHED_SLOW": "scheduler",
        "ENTER_LOCK_MANAGER": "scheduler",
        "EXIT_WORKER": "worker",
    }
    for event_row in cur_events.iter_rows(named=True):
        event = event_row["event"]
        time = event_row["time"]
        # Special case for the lock manager
        if current_stage == "lck_man":
            duration = (time - stage_start_time) * NANO_TO_MS
            stage_durations[current_stage] += duration
            current_stage = None
            stage_start_time = None
        # If we're currently in a stage and this event marks its exit
        if current_stage and event in stage_exit and stage_exit[event] == current_stage:
            duration = (time - stage_start_time) * NANO_TO_MS
            stage_durations[current_stage] += duration
            current_stage = None
            stage_start_time = None
        # If this event marks entering a stage (and no other stage is active)
        if event in stage_enter:
            if current_stage is None:
                current_stage = stage_enter[event]
                stage_start_time = time
        elif event == "RETURN_TO_SERVER" and current_stage is None:
            current_stage = "idle"
            stage_start_time = time
    return (
        stage_durations["server"],
        stage_durations["forwarder"],
        stage_durations["mh_orderer"],
        stage_durations["sequencer"],
        stage_durations["log_manager"],
        stage_durations["scheduler"],
        stage_durations["lck_man"],
        stage_durations["worker"],
        stage_durations["idle"],
    )

for system in ordered_system_dirs:
    clients = [item for item in os.listdir(join(data_folder, system, "client")) if os.path.isdir(join(data_folder, system, "client", item))]
    txns_csvs = [join(data_folder, system, "client", client, "transactions.csv") for client in clients]
    events_csvs = [join(data_folder, system, "client", client, "txn_events.csv") for client in clients]
    # Merge the CSVs together
    txns_df = pl.DataFrame(schema=["txn_id","coordinator","regions","partitions","generator","restarts","global_log_pos","sent_at","received_at"])
    events_df = pl.DataFrame(schema=["txn_id","event","time","machine","home"])
    ### New optimized version with polars
    # Read and concatenate all transaction CSVs
    txns_df = pl.concat([pl.read_csv(path) for path in txns_csvs])
    # Read and concatenate all event CSVs
    events_df = pl.concat([pl.read_csv(path) for path in events_csvs])
    # Group by txn_id
    event_groups = events_df.group_by("txn_id")
    # Prepare list to collect results
    results = []
    txns_df = txns_df.tail(100) # For debugging purposes only, use the first 100 txns to speed up the script
    for txn in txns_df.iter_rows(named=True):
        txn_id = txn["txn_id"]
        sent_at = txn["sent_at"]
        received_at = txn["received_at"]
        duration_ms = (received_at - sent_at) * NANO_TO_MS
        is_mp = ';' in str(txn['partitions'])
        is_mh = ';' in str(txn['regions'])
        # Get event rows for this txn, if any
        txn_events = events_df.filter(pl.col("txn_id") == txn_id).sort("time")
        if txn_events.height > 0:
            server_ms, forwarder_ms, mh_orderer_ms, sequencer_ms, log_manager_ms, scheduler_ms, lck_man_ms, worker_ms, idle_ms = extract_component_times(txn_events)
            if system == 'janus':
                lck_man_ms += forwarder_ms
                forwarder_ms = 0.0
        other_duration = round(max(0, duration_ms - server_ms - forwarder_ms - mh_orderer_ms - sequencer_ms - log_manager_ms - scheduler_ms - lck_man_ms - worker_ms - idle_ms), 5)
        #if other_duration > 100:
        #    print("Warning: Lots of unattributed latency!")
        results.append({
            "Txn_ID": txn_id,
            "Is MP": is_mp,
            "Is MH": is_mh,
            "Start time": sent_at,
            "End time": received_at,
            "Duration (ms)": round(duration_ms, 5),
            "Server (ms)": round(server_ms, 5),
            "Fwd (ms)": round(forwarder_ms, 5),
            "MH orderer (ms)": round(mh_orderer_ms, 5),
            "Seq (ms)": round(sequencer_ms, 5),
            "Log man (ms)": round(log_manager_ms, 5),
            "Sched (ms)": round(scheduler_ms, 5),
            "Lck man (ms)": round(lck_man_ms, 5),
            "Worker (ms)": round(worker_ms, 5),
            "Wait (ms)": round(idle_ms, 5),
            "Other (ms)": other_duration,
        })
    latency_breakdown_df = pl.DataFrame(results)
    os.makedirs(output_folder, exist_ok=True)
    latency_breakdown_df.write_csv(os.path.join(output_folder, f"latency_breakdown_{system}.csv"))
    # Define txn subcategories
    sp_sh_df = latency_breakdown_df.filter(~pl.col("Is MP") & ~pl.col("Is MH"))
    mp_sh_df = latency_breakdown_df.filter(pl.col("Is MP") & ~pl.col("Is MH"))
    mp_mh_df = latency_breakdown_df.filter(pl.col("Is MP") & pl.col("Is MH"))
    # Columns to summarize
    cols = [
        "Duration (ms)", "Server (ms)", "Fwd (ms)", "Seq (ms)", "MH orderer (ms)",
        "Log man (ms)", "Sched (ms)", "Lck man (ms)", "Worker (ms)", "Wait (ms)", "Other (ms)"
    ]
    def summarize(df, prefix):
        mean_vals = df.select([pl.col(c).mean().alias(f"{prefix}Avg {c}") for c in cols])
        std_vals  = df.select([pl.col(c).std().alias(f"{prefix}Std {c}") for c in cols])
        mean_vals.hstack(std_vals, in_place=True)
        # Add % cols
        percent_vals = df.select([(pl.col(c).mean()).alias(f"{prefix}{c[:-3]}%)") for c in cols if 'Duration' not in c])
        mean_vals.hstack(percent_vals, in_place=True)
        return mean_vals
    # Get all summaries
    summary_all   = summarize(latency_breakdown_df, "All ")
    summary_sp_sh = summarize(sp_sh_df, "SP_SH ")
    summary_mp_sh = summarize(mp_sh_df, "MP_SH ")
    summary_mp_mh = summarize(mp_mh_df, "MP_MH ")
    # Combine into single row with 'System' column
    summary_row = pl.DataFrame({"System": [system]})
    # Append to full summary collection (if you're looping over systems)
    all_systems_summary = pl.concat([all_systems_summary, summary_combined]) if 'all_systems_summary' in locals() else summary_combined




    # Compute base stats
    summary_stats_all = latency_breakdown_df[[
        "Duration (ms)",
        "Server (ms)",
        "Fwd (ms)",
        "Seq (ms)",
        "MH orderer (ms)",
        "Log man (ms)",
        "Sched (ms)",
        "Lck man (ms)",
        "Worker (ms)",
        "Wait (ms)",
        "Other (ms)"
    ]].agg(['mean', 'std'])
    summary_stats_sp_sh = sp_sh_df[[
        "Duration (ms)",
        "Server (ms)",
        "Fwd (ms)",
        "Seq (ms)",
        "MH orderer (ms)",
        "Log man (ms)",
        "Sched (ms)",
        "Lck man (ms)",
        "Worker (ms)",
        "Wait (ms)",
        "Other (ms)"
    ]].agg(['mean', 'std'])
    summary_stats_mp_sh = mp_sh_df[[
        "Duration (ms)",
        "Server (ms)",
        "Fwd (ms)",
        "Seq (ms)",
        "MH orderer (ms)",
        "Log man (ms)",
        "Sched (ms)",
        "Lck man (ms)",
        "Worker (ms)",
        "Wait (ms)",
        "Other (ms)"
    ]].agg(['mean', 'std'])
    summary_stats_mp_mh = mp_mh_df[[
        "Duration (ms)",
        "Server (ms)",
        "Fwd (ms)",
        "Seq (ms)",
        "MH orderer (ms)",
        "Log man (ms)",
        "Sched (ms)",
        "Lck man (ms)",
        "Worker (ms)",
        "Wait (ms)",
        "Other (ms)"
    ]].agg(['mean', 'std'])

    # Flatten into one row
    summary_flat = pd.DataFrame([{
        "System": system,
        # All Txns
        "Avg Duration (ms)": round(summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Std Duration (ms)": round(summary_stats_all.loc['std', 'Duration (ms)'], 5), 
        "Avg Server (ms)": round(summary_stats_all.loc['mean', 'Server (ms)'], 5),
        "Std Server (ms)": round(summary_stats_all.loc['std', 'Server (ms)'], 5),
        "Server (%)": round(summary_stats_all.loc['mean', 'Server (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Fwd (ms)": round(summary_stats_all.loc['mean', 'Fwd (ms)'], 5),
        "Std Fwd (ms)": round(summary_stats_all.loc['std', 'Fwd (ms)'], 5),
        "Fwd (%)": round(summary_stats_all.loc['mean', 'Fwd (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Seq (ms)": round(summary_stats_all.loc['mean', 'Seq (ms)'], 5),
        "Std Seq (ms)": round(summary_stats_all.loc['std', 'Seq (ms)'], 5),
        "Seq (%)": round(summary_stats_all.loc['mean', 'Seq (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg MH orderer (ms)": round(summary_stats_all.loc['mean', 'MH orderer (ms)'], 5),
        "Std MH orderer (ms)": round(summary_stats_all.loc['std', 'MH orderer (ms)'], 5),
        "MH orderer (%)": round(summary_stats_all.loc['mean', 'MH orderer (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Log man (ms)": round(summary_stats_all.loc['mean', 'Log man (ms)'], 5),
        "Std Log man (ms)": round(summary_stats_all.loc['std', 'Log man (ms)'], 5),
        "Log man (%)": round(summary_stats_all.loc['mean', 'Log man (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Sched (ms)": round(summary_stats_all.loc['mean', 'Sched (ms)'], 5),
        "Std Sched (ms)": round(summary_stats_all.loc['std', 'Sched (ms)'], 5),
        "Sched (%)": round(summary_stats_all.loc['mean', 'Sched (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Lck man (ms)": round(summary_stats_all.loc['mean', 'Lck man (ms)'], 5),
        "Std Lck man (ms)": round(summary_stats_all.loc['std', 'Lck man (ms)'], 5),
        "Lck man (%)": round(summary_stats_all.loc['mean', 'Lck man (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Worker (ms)": round(summary_stats_all.loc['mean', 'Worker (ms)'], 5),
        "Std Worker (ms)": round(summary_stats_all.loc['std', 'Worker (ms)'], 5),
        "Worker (%)": round(summary_stats_all.loc['mean', 'Worker (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Wait (ms)": round(summary_stats_all.loc['mean', 'Wait (ms)'], 5),
        "Std Wait (ms)": round(summary_stats_all.loc['std', 'Wait (ms)'], 5),
        "Wait (%)": round(summary_stats_all.loc['mean', 'Wait (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Other (ms)": round(summary_stats_all.loc['mean', 'Other (ms)'], 5),
        "Std Other (ms)": round(summary_stats_all.loc['std', 'Other (ms)'], 5),
        "Other (%)": round(summary_stats_all.loc['mean', 'Other (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        # SP SH Txns
        "SP_SH Avg Duration (ms)": round(summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Std Duration (ms)": round(summary_stats_sp_sh.loc['std', 'Duration (ms)'], 5),
        "SP_SH Avg Server (ms)": round(summary_stats_sp_sh.loc['mean', 'Server (ms)'], 5),
        "SP_SH Std Server (ms)": round(summary_stats_sp_sh.loc['std', 'Server (ms)'], 5),
        "SP_SH Server (%)": round(summary_stats_sp_sh.loc['mean', 'Server (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Fwd (ms)": round(summary_stats_sp_sh.loc['mean', 'Fwd (ms)'], 5),
        "SP_SH Std Fwd (ms)": round(summary_stats_sp_sh.loc['std', 'Fwd (ms)'], 5),
        "SP_SH Fwd (%)": round(summary_stats_sp_sh.loc['mean', 'Fwd (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Seq (ms)": round(summary_stats_sp_sh.loc['mean', 'Seq (ms)'], 5),
        "SP_SH Std Seq (ms)": round(summary_stats_sp_sh.loc['std', 'Seq (ms)'], 5),
        "SP_SH Seq (%)": round(summary_stats_sp_sh.loc['mean', 'Seq (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg MH orderer (ms)": round(summary_stats_sp_sh.loc['mean', 'MH orderer (ms)'], 5),
        "SP_SH Std MH orderer (ms)": round(summary_stats_sp_sh.loc['std', 'MH orderer (ms)'], 5),
        "SP_SH MH orderer (%)": round(summary_stats_sp_sh.loc['mean', 'MH orderer (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Log man (ms)": round(summary_stats_sp_sh.loc['mean', 'Log man (ms)'], 5),
        "SP_SH Std Log man (ms)": round(summary_stats_sp_sh.loc['std', 'Log man (ms)'], 5),
        "SP_SH Log man (%)": round(summary_stats_sp_sh.loc['mean', 'Log man (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Sched (ms)": round(summary_stats_sp_sh.loc['mean', 'Sched (ms)'], 5),
        "SP_SH Std Sched (ms)": round(summary_stats_sp_sh.loc['std', 'Sched (ms)'], 5),
        "SP_SH Sched (%)": round(summary_stats_sp_sh.loc['mean', 'Sched (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Lck man (ms)": round(summary_stats_sp_sh.loc['mean', 'Lck man (ms)'], 5),
        "SP_SH Std Lck man (ms)": round(summary_stats_sp_sh.loc['std', 'Lck man (ms)'], 5),
        "SP_SH Lck man (%)": round(summary_stats_sp_sh.loc['mean', 'Lck man (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Worker (ms)": round(summary_stats_sp_sh.loc['mean', 'Worker (ms)'], 5),
        "SP_SH Std Worker (ms)": round(summary_stats_sp_sh.loc['std', 'Worker (ms)'], 5),
        "SP_SH Worker (%)": round(summary_stats_sp_sh.loc['mean', 'Worker (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Wait (ms)": round(summary_stats_sp_sh.loc['mean', 'Wait (ms)'], 5),
        "SP_SH Std Wait (ms)": round(summary_stats_sp_sh.loc['std', 'Wait (ms)'], 5),
        "SP_SH Wait (%)": round(summary_stats_sp_sh.loc['mean', 'Wait (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        "SP_SH Avg Other (ms)": round(summary_stats_sp_sh.loc['mean', 'Other (ms)'], 5),
        "SP_SH Std Other (ms)": round(summary_stats_sp_sh.loc['std', 'Other (ms)'], 5),
        "SP_SH Other (%)": round(summary_stats_sp_sh.loc['mean', 'Other (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
        # MP SH Txns
        "MP_SH Avg Duration (ms)": round(summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Std Duration (ms)": round(summary_stats_mp_sh.loc['std', 'Duration (ms)'], 5),
        "MP_SH Avg Server (ms)": round(summary_stats_mp_sh.loc['mean', 'Server (ms)'], 5),
        "MP_SH Std Server (ms)": round(summary_stats_mp_sh.loc['std', 'Server (ms)'], 5),
        "MP_SH Server (%)": round(summary_stats_mp_sh.loc['mean', 'Server (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Fwd (ms)": round(summary_stats_mp_sh.loc['mean', 'Fwd (ms)'], 5),
        "MP_SH Std Fwd (ms)": round(summary_stats_mp_sh.loc['std', 'Fwd (ms)'], 5),
        "MP_SH Fwd (%)": round(summary_stats_mp_sh.loc['mean', 'Fwd (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Seq (ms)": round(summary_stats_mp_sh.loc['mean', 'Seq (ms)'], 5),
        "MP_SH Std Seq (ms)": round(summary_stats_mp_sh.loc['std', 'Seq (ms)'], 5),
        "MP_SH Seq (%)": round(summary_stats_mp_sh.loc['mean', 'Seq (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg MH orderer (ms)": round(summary_stats_mp_sh.loc['mean', 'MH orderer (ms)'], 5),
        "MP_SH Std MH orderer (ms)": round(summary_stats_mp_sh.loc['std', 'MH orderer (ms)'], 5),
        "MP_SH MH orderer (%)": round(summary_stats_mp_sh.loc['mean', 'MH orderer (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Log man (ms)": round(summary_stats_mp_sh.loc['mean', 'Log man (ms)'], 5),
        "MP_SH Std Log man (ms)": round(summary_stats_mp_sh.loc['std', 'Log man (ms)'], 5),
        "MP_SH Log man (%)": round(summary_stats_mp_sh.loc['mean', 'Log man (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Sched (ms)": round(summary_stats_mp_sh.loc['mean', 'Sched (ms)'], 5),
        "MP_SH Std Sched (ms)": round(summary_stats_mp_sh.loc['std', 'Sched (ms)'], 5),
        "MP_SH Sched (%)": round(summary_stats_mp_sh.loc['mean', 'Sched (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Lck man (ms)": round(summary_stats_mp_sh.loc['mean', 'Lck man (ms)'], 5),
        "MP_SH Std Lck man (ms)": round(summary_stats_mp_sh.loc['std', 'Lck man (ms)'], 5),
        "MP_SH Lck man (%)": round(summary_stats_mp_sh.loc['mean', 'Lck man (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Worker (ms)": round(summary_stats_mp_sh.loc['mean', 'Worker (ms)'], 5),
        "MP_SH Std Worker (ms)": round(summary_stats_mp_sh.loc['std', 'Worker (ms)'], 5),
        "MP_SH Worker (%)": round(summary_stats_mp_sh.loc['mean', 'Worker (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Wait (ms)": round(summary_stats_mp_sh.loc['mean', 'Wait (ms)'], 5),
        "MP_SH Std Wait (ms)": round(summary_stats_mp_sh.loc['std', 'Wait (ms)'], 5),
        "MP_SH Wait (%)": round(summary_stats_mp_sh.loc['mean', 'Wait (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        "MP_SH Avg Other (ms)": round(summary_stats_mp_sh.loc['mean', 'Other (ms)'], 5),
        "MP_SH Std Other (ms)": round(summary_stats_mp_sh.loc['std', 'Other (ms)'], 5),
        "MP_SH Other (%)": round(summary_stats_mp_sh.loc['mean', 'Other (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
        # MP MH Txns
        "MP_MH Avg Duration (ms)": round(summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Std Duration (ms)": round(summary_stats_mp_mh.loc['std', 'Duration (ms)'], 5),
        "MP_MH Avg Server (ms)": round(summary_stats_mp_mh.loc['mean', 'Server (ms)'], 5),
        "MP_MH Std Server (ms)": round(summary_stats_mp_mh.loc['std', 'Server (ms)'], 5),
        "MP_MH Server (%)": round(summary_stats_mp_mh.loc['mean', 'Server (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Fwd (ms)": round(summary_stats_mp_mh.loc['mean', 'Fwd (ms)'], 5),
        "MP_MH Std Fwd (ms)": round(summary_stats_mp_mh.loc['std', 'Fwd (ms)'], 5),
        "MP_MH Fwd (%)": round(summary_stats_mp_mh.loc['mean', 'Fwd (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Seq (ms)": round(summary_stats_mp_mh.loc['mean', 'Seq (ms)'], 5),
        "MP_MH Std Seq (ms)": round(summary_stats_mp_mh.loc['std', 'Seq (ms)'], 5),
        "MP_MH Seq (%)": round(summary_stats_mp_mh.loc['mean', 'Seq (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg MH orderer (ms)": round(summary_stats_mp_mh.loc['mean', 'MH orderer (ms)'], 5),
        "MP_MH Std MH orderer (ms)": round(summary_stats_mp_mh.loc['std', 'MH orderer (ms)'], 5),
        "MP_MH MH orderer (%)": round(summary_stats_mp_mh.loc['mean', 'MH orderer (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Log man (ms)": round(summary_stats_mp_mh.loc['mean', 'Log man (ms)'], 5),
        "MP_MH Std Log man (ms)": round(summary_stats_mp_mh.loc['std', 'Log man (ms)'], 5),
        "MP_MH Log man (%)": round(summary_stats_mp_mh.loc['mean', 'Log man (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Sched (ms)": round(summary_stats_mp_mh.loc['mean', 'Sched (ms)'], 5),
        "MP_MH Std Sched (ms)": round(summary_stats_mp_mh.loc['std', 'Sched (ms)'], 5),
        "MP_MH Sched (%)": round(summary_stats_mp_mh.loc['mean', 'Sched (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Lck man (ms)": round(summary_stats_mp_mh.loc['mean', 'Lck man (ms)'], 5),
        "MP_MH Std Lck man (ms)": round(summary_stats_mp_mh.loc['std', 'Lck man (ms)'], 5),
        "MP_MH Lck man (%)": round(summary_stats_mp_mh.loc['mean', 'Lck man (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Worker (ms)": round(summary_stats_mp_mh.loc['mean', 'Worker (ms)'], 5),
        "MP_MH Std Worker (ms)": round(summary_stats_mp_mh.loc['std', 'Worker (ms)'], 5),
        "MP_MH Worker (%)": round(summary_stats_mp_mh.loc['mean', 'Worker (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Wait (ms)": round(summary_stats_mp_mh.loc['mean', 'Wait (ms)'], 5),
        "MP_MH Std Wait (ms)": round(summary_stats_mp_mh.loc['std', 'Wait (ms)'], 5),
        "MP_MH Wait (%)": round(summary_stats_mp_mh.loc['mean', 'Wait (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
        "MP_MH Avg Other (ms)": round(summary_stats_mp_mh.loc['mean', 'Other (ms)'], 5),
        "MP_MH Std Other (ms)": round(summary_stats_mp_mh.loc['std', 'Other (ms)'], 5),
        "MP_MH Other (%)": round(summary_stats_mp_mh.loc['mean', 'Other (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
    }])
    # Append row for current system to the combined DataFrame
    summary_combined = pd.concat([summary_combined, summary_flat], ignore_index=True)

# Save summary
summary_combined.to_csv(os.path.join(output_folder, "latency_summary.csv"), index=False)

print("\nSummary statistics:")
print(summary_combined.round(5))

print("Generating heatmap")
'''columns_to_include = [
    "Server (%)", "Fwd (%)", "Seq (%)", "MH orderer (%)", "Log man (%)", "Sched (%)", "Lck man (%)", "Worker (%)", "Wait (%)", "Other (%)",
    "SP_SH Server (%)", "SP_SH Fwd (%)", "SP_SH Seq (%)", "SP_SH MH orderer (%)", "SP_SH Log man (%)", "SP_SH Sched (%)", "SP_SH Lck man (%)", "SP_SH Worker (%)", "SP_SH Wait (%)", "SP_SH Other (%)",
    "MP_SH Server (%)", "MP_SH Fwd (%)", "MP_SH Seq (%)", "MP_SH MH orderer (%)", "MP_SH Log man (%)", "MP_SH Sched (%)", "MP_SH Lck man (%)", "MP_SH Worker (%)", "MP_SH Wait (%)", "MP_SH Other (%)",
    "MP_MH Server (%)", "MP_MH Fwd (%)", "MP_MH Seq (%)", "MP_MH MH orderer (%)", "MP_MH Log man (%)", "MP_MH Sched (%)", "MP_MH Lck man (%)", "MP_MH Worker (%)", "MP_MH Wait (%)", "MP_MH Other (%)",
]'''
columns_to_include_all = [
    "Server (%)", "Fwd (%)", "Seq (%)", "MH orderer (%)", "Log man (%)", "Sched (%)", "Lck man (%)", "Worker (%)", "Wait (%)", "Other (%)",
]
columns_to_include_sp_sh = [
    "SP_SH Server (%)", "SP_SH Fwd (%)", "SP_SH Seq (%)", "SP_SH MH orderer (%)", "SP_SH Log man (%)", "SP_SH Sched (%)", "SP_SH Lck man (%)", "SP_SH Worker (%)", "SP_SH Wait (%)", "SP_SH Other (%)",
]
columns_to_include_mp_sh = [
    "MP_SH Server (%)", "MP_SH Fwd (%)", "MP_SH Seq (%)", "MP_SH MH orderer (%)", "MP_SH Log man (%)", "MP_SH Sched (%)", "MP_SH Lck man (%)", "MP_SH Worker (%)", "MP_SH Wait (%)", "MP_SH Other (%)",
]
columns_to_include_mp_mh = [
    "MP_MH Server (%)", "MP_MH Fwd (%)", "MP_MH Seq (%)", "MP_MH MH orderer (%)", "MP_MH Log man (%)", "MP_MH Sched (%)", "MP_MH Lck man (%)", "MP_MH Worker (%)", "MP_MH Wait (%)", "MP_MH Other (%)",
]
column_super_categories = {
    'Server (%)': ['Server (%)', 'Fwd (%)', 'Worker (%)'],
    'Sequencer (%)': ['Seq (%)', 'MH orderer (%)', 'Log man (%)'],
    'Scheduler (%)': ['Sched (%)', 'Lck man (%)'],
    'Idle (%)': ['Wait (%)'],
    'Other (%)': ['Other (%)']
}
#columns_names = ["Server (%)", "Forwarder (%)", "Sequencer (%)", "MH orderer (%)", "Log manager (%)", "Scheduler (%)", "Lock manager (%)", "Worker (%)", "Other (%)"]
columns_names = ["Server", "Sequencer", "Scheduler", "Idle", "Other"]
cmap = plt.cm.get_cmap("OrRd")
cmap.set_bad("lightgrey")

fig, ax = plt.subplots(3,1, figsize=(5,7), constrained_layout=True)

# Heatmap for SP SH txns
summary_percentages = 100 * summary_combined[columns_to_include_sp_sh]
# New DataFrame to hold merged categories
merged_summary = pd.DataFrame()
# For each supercategory, find matching columns and sum them
for supercat, substrings in column_super_categories.items():
    matched_cols = [col for col in summary_percentages.columns if any(sub in col for sub in substrings)]
    merged_summary[supercat] = summary_percentages[matched_cols].sum(axis=1)
merged_summary.replace('NaN', np.nan, inplace=True)
merged_summary = merged_summary.to_numpy().round(5)
annot = np.where(np.isnan(merged_summary), 'N/A', merged_summary.astype(float)).astype(str)  # Annotation matrix
for row in range(len(annot)):
    for cell in range(len(annot[row])):
        if (float(annot[row][cell]) > 1):
            annot[row][cell] = str(annot[row][cell])[:4] + ' %'
        else:
            annot[row][cell] = str(annot[row][cell])[:6] + ' %'
merged_summary += 0.00000001 # Small hack to avoid problems with log of 0

h0 = sns.heatmap(
    data=merged_summary,
    annot=annot,                         # Custom annotation matrix
    fmt='',                              # Allow custom formatting
    cmap=cmap, 
    cbar=False,                          # Disable individual colorbars
    linewidths=0.5,
    cbar_kws={"shrink": 0.7}, 
    mask=np.isnan(merged_summary),  # Mask NaN values
    vmin=0,
    vmax=30,
    norm=LogNorm(vmin=0.1, vmax=30),
    annot_kws={"size": 8},
    ax=ax[0]
)
ax[0].xaxis.set_ticks_position('top')
ax[0].set_xticklabels(columns_names, rotation=90, fontsize=8) # Remove x-ticks for further subplots
sys_names = summary_combined["System"]
sys_names = [SYSNAME_MAP[system] if system in SYSNAME_MAP else system for system in sys_names]
ax[0].set_yticklabels(sys_names, rotation=0, fontsize=8)  # Keep region labels readable

# Heatmap for MP SH txns
summary_percentages = 100 * summary_combined[columns_to_include_mp_sh]
# New DataFrame to hold merged categories
merged_summary = pd.DataFrame()
# For each supercategory, find matching columns and sum them
for supercat, substrings in column_super_categories.items():
    matched_cols = [col for col in summary_percentages.columns if any(sub in col for sub in substrings)]
    merged_summary[supercat] = summary_percentages[matched_cols].sum(axis=1)
merged_summary.replace('NaN', np.nan, inplace=True)
merged_summary = merged_summary.to_numpy().round(5)
annot = np.where(np.isnan(merged_summary), 'N/A', merged_summary.astype(float)).astype(str)  # Annotation matrix
for row in range(len(annot)):
    for cell in range(len(annot[row])):
        if (float(annot[row][cell]) > 1):
            annot[row][cell] = str(annot[row][cell])[:4] + ' %'
        else:
            annot[row][cell] = str(annot[row][cell])[:6] + ' %'
merged_summary += 0.00000001 # Small hack to avoid problems with log of 0

h1 = sns.heatmap(
    data=merged_summary,
    annot=annot,                         # Custom annotation matrix
    fmt='',                              # Allow custom formatting
    cmap=cmap, 
    cbar=False,                          # Disable individual colorbars
    linewidths=0.5,
    cbar_kws={"shrink": 0.7}, 
    mask=np.isnan(merged_summary),  # Mask NaN values
    vmin=0,
    vmax=30,
    norm=LogNorm(vmin=0.1, vmax=30),
    annot_kws={"size": 8},
    ax=ax[1]
)
ax[1].set_xticks(ticks=[], labels=[]) # Remove x-ticks for further subplots
sys_names = summary_combined["System"]
sys_names = [SYSNAME_MAP[system] if system in SYSNAME_MAP else system for system in sys_names]
ax[1].set_yticklabels(sys_names, rotation=0, fontsize=8)  # Keep region labels readable

# Heatmap for MP MH txns
summary_percentages = 100 * summary_combined[columns_to_include_mp_mh]
# New DataFrame to hold merged categories
merged_summary = pd.DataFrame()
# For each supercategory, find matching columns and sum them
for supercat, substrings in column_super_categories.items():
    matched_cols = [col for col in summary_percentages.columns if any(sub in col for sub in substrings)]
    merged_summary[supercat] = summary_percentages[matched_cols].sum(axis=1)
merged_summary = merged_summary.apply(pd.to_numeric, errors='coerce')
merged_summary.iloc[0] = np.nan # Janus has no 'MH' txns
merged_summary.iloc[3] = np.nan # Calvin has no 'MH' txns
# Prepare annotation matrix
annot = merged_summary.copy()
for i in range(annot.shape[0]):
    for j in range(annot.shape[1]):
        val = annot.iat[i, j]
        if pd.isna(val):
            annot.iat[i, j] = "N/A"
        elif val > 1:
            annot.iat[i, j] = f"{val:.1f} %"
        else:
            annot.iat[i, j] = f"{val:.3f} %"

merged_summary = merged_summary.fillna(-1) # Only a hack for the correct color display
h2 = sns.heatmap(
    data=merged_summary,
    annot=annot.values,
    fmt='',
    cmap=cmap,
    cbar=False,
    linewidths=0.5,
    linecolor='white',
    vmin=0.1,
    vmax=30,
    norm=LogNorm(vmin=0.1, vmax=30),
    annot_kws={"size": 8},
    ax=ax[2],
)
ax[2].set_xticks(ticks=[], labels=[]) # Remove x-ticks for further subplots
sys_names = summary_combined["System"]
sys_names = [SYSNAME_MAP[system] if system in SYSNAME_MAP else system for system in sys_names]
ax[2].set_yticklabels(sys_names, rotation=0, fontsize=8)  # Keep region labels readable

# Create a single colorbar for all plots
cbar = fig.colorbar(
    h1.collections[0],         # Use the first heatmap's mappable
    ax=ax,
    orientation='vertical',
    shrink=0.7,
    pad=0.02,
    aspect=30
)
#plt.tight_layout() Doesn't work with subplots

# Save the plot
print("Saving heatmap")
output_path = f'plots/output/{workload}/latency_decomposition'
png_path = output_path + '.png'
jpg_path = output_path + '.jpg'
pdf_path = output_path + '.pdf'
plt.savefig(png_path, dpi=300, bbox_inches='tight')
plt.savefig(pdf_path, bbox_inches='tight')
plt.show()

print("Done")