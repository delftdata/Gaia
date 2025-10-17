import os
from os.path import join, isdir
import sys
import numpy as np
import pandas as pd
import argparse

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
VALID_ENVIRONMENTS = ['local', 'st', 'aws']

SYSNAME_MAP = {
    'janus':  'Janus',
    'Detock': 'Detock', # 2 ways of namins Detock (unfortunately inconsistent)
    'ddr_ts': 'Detock', 
    'calvin': 'Calvin',
    'slog':   'SLOG',
}

# Argument parser
parser = argparse.ArgumentParser(description="Decompose the latency of transactions into components and plot graph stacked bar chart.")
parser.add_argument('-w', '--workload', default='ycsb', choices=VALID_WORKLOADS, help='Workload evaluated (default: ycsb)')
parser.add_argument('-e', '--environment', default='aws', choices=VALID_ENVIRONMENTS, help='What type of machine the experiment was run on.')

args = parser.parse_args()
workload = args.workload
environment = args.environment

data_folder = f'plots/raw_data/{environment}/{workload}/lat_breakdown'
output_folder = f'plots/data/{environment}/{workload}/latency_breakdown'
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
if 'Detock' in system_dirs: # In case Detock is named differently
    ordered_system_dirs.append('Detock')
    system_dirs.remove('Detock')
if 'janus' in system_dirs:
    ordered_system_dirs.append('janus')
    system_dirs.remove('janus')
ordered_system_dirs.extend(system_dirs)

ordered_system_dirs = [system for system in ordered_system_dirs if '.' not in system]
ordered_system_dirs = [system for system in ordered_system_dirs if 'ddr_only' not in system]
summary_combined = pd.DataFrame(columns=['System'])

def extract_janus_component_times(cur_events):
    # Duration accumulators
    stage_durations = {
        "server": 0.0,
        "idle": 0.0,
        "other": 0.0
    }
    total_time = (txn_events['time'].iloc[-1] - txn_events['time'].iloc[0]) * NANO_TO_MS

    last_event = None
    last_time = -1
    for _, event_row in cur_events.iterrows():
        event = event_row["event"]
        time = event_row["time"]
        if last_event is None:
            if event != 'ENTER_SERVER':
                print('WARNING! Strange Start')
            last_time = time
        else:
            if event == 'ENTER_SERVER': # This is when we see a '2nd start' probably processing same txn on another worker
                if last_time < time:
                    print('WARNING! Strange Enter server')
                last_time = time
            elif time == last_time or event == last_event:
                pass
            elif event == 'EXIT_SERVER_TO_FORWARDER':
                if last_event != 'ENTER_SERVER':
                    print('WARNING! Strange Exit server to fwd')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_WORKER':
                if last_event != 'EXIT_SERVER_TO_FORWARDER':
                    print('WARNING! Strange Enter worker')
                stage_durations['idle'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'GOT_REMOTE_READS':
                if last_event != 'ENTER_WORKER':
                    print('WARNING! Strange Got remote reads')
                stage_durations['idle'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_WORKER':
                if last_event != 'ENTER_WORKER' and last_event != 'GOT_REMOTE_READS':
                    print('WARNING! Strange Got remote reads')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'RETURN_TO_SERVER':
                if last_event != 'EXIT_WORKER':
                    print('WARNING! Strange Return to server')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_SERVER_TO_CLIENT':
                if last_event != 'RETURN_TO_SERVER':
                    print('WARNING! Strange Exit server to client')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            else:
                print('WARNING! Encountered an uncovered chain of events!')
        last_event = event

    return stage_durations['server'], stage_durations['idle'], stage_durations['other']

def extract_calvin_component_times(cur_events):
    # Duration accumulators
    stage_durations = {
        "server": 0.0,
        "seq": 0.0,
        "sched": 0.0,
        "idle": 0.0,
        "other": 0.0
    }
    total_time = (txn_events['time'].iloc[-1] - txn_events['time'].iloc[0]) * NANO_TO_MS

    last_event = None
    last_time = -1
    for _, event_row in cur_events.iterrows():
        event = event_row["event"]
        time = event_row["time"]
        if last_event is None:
            if event != 'ENTER_SERVER':
                print('WARNING! Strange Start')
            last_time = time
        else:
            if event == 'ENTER_SERVER': # This is when we see a '2nd start' probably processing same txn on another worker
                if last_time < time:
                    print('WARNING! Strange Enter server')
                last_time = time
            elif time == last_time or event == last_event:
                pass
            elif event == 'EXIT_SERVER_TO_FORWARDER':
                if last_event != 'ENTER_SERVER':
                    print('WARNING! Strange Exit server to fwd')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_FORWARDER':
                if last_event != 'EXIT_SERVER_TO_FORWARDER':
                    print('WARNING! Strange Enter fwd')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_FORWARDER_TO_SEQUENCER':
                if last_event != 'ENTER_FORWARDER':
                    print('WARNING! Strange Exit fwd to sequencer')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_SEQUENCER':
                if last_event != 'EXIT_FORWARDER_TO_SEQUENCER':
                    print('WARNING! Strange Enter sequencer')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_LOCAL_BATCH':
                if last_event != 'ENTER_SEQUENCER':
                    print('WARNING! Strange Enter local batch')
                stage_durations['seq'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_SEQUENCER_IN_BATCH':
                if last_event != 'ENTER_LOCAL_BATCH':
                    print('WARNING! Strange Exit sequencer')
                stage_durations['seq'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_LOG_MANAGER_IN_BATCH':
                if last_event != 'EXIT_SEQUENCER_IN_BATCH':
                    print('WARNING! Strange Enter log man. in batch')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_LOG_MANAGER_ORDER':
                if last_event != 'ENTER_LOG_MANAGER_IN_BATCH':
                    print('WARNING! Strange Enter log man. order')
                stage_durations['idle'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_LOG_MANAGER':
                if last_event != 'ENTER_LOG_MANAGER_ORDER':
                    print('WARNING! Strange Exit log man.')
                stage_durations['idle'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_SCHEDULER':
                if last_event != 'EXIT_LOG_MANAGER':
                    print('WARNING! Strange Enter scheduler')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_LOCK_MANAGER':
                if last_event != 'ENTER_SCHEDULER':
                    print('WARNING! Strange Enter lock man.')
                stage_durations['sched'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'DISPATCHED_FAST' or event == 'DISPATCHED_SLOW':
                if last_event != 'ENTER_LOCK_MANAGER':
                    print('WARNING! Strange Dispatched fast/slow')
                stage_durations['idle'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'ENTER_WORKER':
                if last_event != 'DISPATCHED_FAST' and last_event != 'DISPATCHED_SLOW':
                    print('WARNING! Strange Enter worker')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'GOT_REMOTE_READS':
                if last_event != 'ENTER_WORKER':
                    print('WARNING! Strange Got remote reads')
                stage_durations['idle'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_WORKER':
                if last_event != 'ENTER_WORKER' and last_event != 'GOT_REMOTE_READS':
                    print('WARNING! Strange Got remote reads')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'RETURN_TO_SERVER':
                if last_event != 'EXIT_WORKER':
                    print('WARNING! Strange Return to server')
                stage_durations['other'] += (time - last_time) * NANO_TO_MS
                last_time = time
            elif event == 'EXIT_SERVER_TO_CLIENT':
                if last_event != 'RETURN_TO_SERVER':
                    print('WARNING! Strange Exit server to client')
                stage_durations['server'] += (time - last_time) * NANO_TO_MS
                last_time = time
            else:
                print('WARNING! Encountered an uncovered chain of events!')
        last_event = event

    return stage_durations['server'], stage_durations['seq'], stage_durations['sched'], stage_durations['idle'], stage_durations['other']

def extract_slog_component_times(cur_events):
    # Duration accumulators
    stage_durations = {
        "server": 0.0,
        "seq": 0.0,
        "sched": 0.0,
        "idle": 0.0,
        "other": 0.0
    }
    total_time = (txn_events['time'].iloc[-1] - txn_events['time'].iloc[0]) * NANO_TO_MS

    last_event = None
    last_time = -1
    for _, event_row in cur_events.iterrows():
        event = event_row["event"]
        time = event_row["time"]
        if last_event is None:
            if event != 'ENTER_SERVER':
                print('WARNING! Strange Start')
            last_time = time
        else:
            cur_stage_duration = (time - last_time) * NANO_TO_MS
            if cur_stage_duration < 0 and event != 'ENTER_SERVER':
                #print('WARNING: Negative stage duration')
                last_event = event
                last_time = time
                continue
            #if cur_stage_duration > 200:
            #    print('Long duration')
            if event == 'ENTER_SERVER': # This is when we see a '2nd start' probably processing same txn on another worker
                if last_time < time:
                    print('WARNING! Strange Enter server')
                last_time = time
            elif time == last_time or event == last_event:
                pass
            elif event == 'EXIT_SERVER_TO_FORWARDER':
                if last_event != 'ENTER_SERVER':
                    print('WARNING! Strange Exit server to fwd')
                stage_durations['server'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_FORWARDER':
                if last_event != 'EXIT_SERVER_TO_FORWARDER':
                    print('WARNING! Strange Enter fwd')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER' or event == 'EXIT_FORWARDER_TO_SEQUENCER':
                if last_event != 'ENTER_FORWARDER':
                    print('WARNING! Strange Exit fwd to MH orderer')
                stage_durations['server'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_MULTI_HOME_ORDERER':
                if last_event != 'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER':
                    print('WARNING! Strange Enter MH orderer')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_MULTI_HOME_ORDERER_IN_BATCH':
                if last_event != 'ENTER_MULTI_HOME_ORDERER' and last_event != 'GOT_REMOTE_READS' and last_event != 'ENTER_WORKER':
                    print('WARNING! Strange MH orderer in batch')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOG_MANAGER_ORDER':
                if last_event != 'ENTER_MULTI_HOME_ORDERER_IN_BATCH' and last_event != 'ENTER_LOG_MANAGER_IN_BATCH':
                    print('WARNING! Strange Enter log man. order')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_MULTI_HOME_ORDERER':
                if last_event != 'ENTER_LOG_MANAGER_ORDER':
                    print('WARNING! Strange Exit MH orderer')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_SEQUENCER':
                if last_event != 'EXIT_MULTI_HOME_ORDERER' and last_event != 'EXIT_FORWARDER_TO_SEQUENCER':
                    print('WARNING! Strange Enter sequencer')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOCAL_BATCH':
                if last_event != 'ENTER_SEQUENCER':
                    print('WARNING! Strange Enter local batch')
                stage_durations['seq'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_SEQUENCER_IN_BATCH':
                if last_event != 'ENTER_LOCAL_BATCH':
                    print('WARNING! Strange Exit sequencer in batch')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOG_MANAGER_IN_BATCH':
                if last_event != 'EXIT_SEQUENCER_IN_BATCH':
                    print('WARNING! Strange Enter log man. in batch')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_LOG_MANAGER':
                if last_event != 'ENTER_LOG_MANAGER_ORDER':
                    print('WARNING! Strange Exit log man.')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_SCHEDULER':
                if last_event != 'EXIT_LOG_MANAGER':
                    print('WARNING! Strange Enter Scheduler')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOCK_MANAGER':
                if last_event != 'ENTER_SCHEDULER' and last_event != 'EXIT_LOG_MANAGER':
                    print('WARNING! Strange Enter lock man.')
                if last_event == 'ENTER_SCHEDULER':
                    stage_durations['sched'] += cur_stage_duration
                else:
                    stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_SCHEDULER_LO':
                if last_event != 'ENTER_LOCK_MANAGER':
                    print('WARNING! Strange Enter scheduler LO')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'DISPATCHED_FAST' or event == 'DISPATCHED_SLOW':
                if last_event != 'ENTER_SCHEDULER_LO' and last_event != 'ENTER_LOCK_MANAGER':
                    print('WARNING! Strange Dispatched fast')
                if last_event == 'ENTER_SCHEDULER_LO':
                    stage_durations['sched'] += cur_stage_duration
                else:
                    stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_WORKER':
                if last_event != 'DISPATCHED_FAST' and last_event != 'DISPATCHED_SLOW':
                    print('WARNING! Strange Enter worker')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'GOT_REMOTE_READS':
                if last_event != 'ENTER_WORKER':
                    print('WARNING! Strange Got remote reads')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_WORKER':
                if last_event != 'ENTER_LOCK_MANAGER' and last_event != 'ENTER_WORKER' and last_event != 'GOT_REMOTE_READS':
                    print('WARNING! Strange Exit worker')
                if last_event == 'ENTER_LOCK_MANAGER':
                    stage_durations['idle'] += cur_stage_duration
                else:
                    stage_durations['server'] += cur_stage_duration
                last_time = time
            elif event == 'RETURN_TO_SERVER':
                if last_event != 'EXIT_WORKER':
                    print('WARNING! Strange Return to server')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_SERVER_TO_CLIENT':
                if last_event != 'RETURN_TO_SERVER':
                    print('WARNING! Strange Exit server to client')
                stage_durations['server'] += cur_stage_duration
                last_time = time
            else:
                print('WARNING! Encountered an uncovered chain of events!')
        last_event = event
    
    return stage_durations['server'], stage_durations['seq'], stage_durations['sched'], stage_durations['idle'], stage_durations['other']

def extract_detock_component_times(cur_events):
    # Duration accumulators
    stage_durations = {
        "server": 0.0,
        "seq": 0.0,
        "sched": 0.0,
        "idle": 0.0,
        "other": 0.0
    }
    total_time = (txn_events['time'].iloc[-1] - txn_events['time'].iloc[0]) * NANO_TO_MS

    last_event = None
    last_time = -1
    for _, event_row in cur_events.iterrows():
        event = event_row["event"]
        time = event_row["time"]
        if last_event is None:
            if event != 'ENTER_SERVER':
                print('WARNING! Strange Start')
            last_time = time
        else:
            cur_stage_duration = (time - last_time) * NANO_TO_MS
            if cur_stage_duration < 0 and event != 'ENTER_SERVER':
                #print('WARNING: Negative stage duration')
                last_event = event
                last_time = time
                continue
            #if cur_stage_duration > 200:
            #    print('Long duration')
            if event == 'ENTER_SERVER': # This is when we see a '2nd start' probably processing same txn on another worker
                if last_time < time:
                    print('WARNING! Strange Enter server')
                last_time = time
            elif time == last_time or event == last_event:
                pass
            elif event == 'EXIT_SERVER_TO_FORWARDER':
                if last_event != 'ENTER_SERVER':
                    print('WARNING! Strange Exit server to fwd')
                stage_durations['server'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_FORWARDER':
                if last_event != 'EXIT_SERVER_TO_FORWARDER':
                    print('WARNING! Strange Enter fwd')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER' or event == 'EXIT_FORWARDER_TO_SEQUENCER':
                if last_event != 'ENTER_FORWARDER':
                    print('WARNING! Strange Exit fwd to MH orderer')
                stage_durations['server'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_MULTI_HOME_ORDERER':
                if last_event != 'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER':
                    print('WARNING! Strange Enter MH orderer')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_MULTI_HOME_ORDERER_IN_BATCH':
                if last_event != 'ENTER_MULTI_HOME_ORDERER' and last_event != 'GOT_REMOTE_READS' and last_event != 'ENTER_WORKER':
                    print('WARNING! Strange MH orderer in batch')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOG_MANAGER_ORDER':
                if last_event != 'ENTER_MULTI_HOME_ORDERER_IN_BATCH' and last_event != 'ENTER_LOG_MANAGER_IN_BATCH':
                    print('WARNING! Strange Enter log man. order')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_MULTI_HOME_ORDERER':
                if last_event != 'ENTER_LOG_MANAGER_ORDER':
                    print('WARNING! Strange Exit MH orderer')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_SEQUENCER':
                if last_event != 'EXIT_MULTI_HOME_ORDERER' and last_event != 'EXIT_FORWARDER_TO_SEQUENCER' and last_event != 'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER':
                    print('WARNING! Strange Enter sequencer')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOCAL_BATCH':
                if last_event != 'ENTER_SEQUENCER':
                    print('WARNING! Strange Enter local batch')
                stage_durations['seq'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_SEQUENCER_IN_BATCH':
                if last_event != 'ENTER_LOCAL_BATCH':
                    print('WARNING! Strange Exit sequencer in batch')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOG_MANAGER_IN_BATCH':
                if last_event != 'EXIT_SEQUENCER_IN_BATCH':
                    print('WARNING! Strange Enter log man. in batch')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_LOG_MANAGER':
                if last_event != 'ENTER_LOG_MANAGER_ORDER':
                    print('WARNING! Strange Exit log man.')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_SCHEDULER':
                if last_event != 'EXIT_LOG_MANAGER':
                    print('WARNING! Strange Enter Scheduler')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_LOCK_MANAGER':
                if last_event != 'ENTER_SCHEDULER' and last_event != 'EXIT_LOG_MANAGER':
                    print('WARNING! Strange Enter lock man.')
                if last_event == 'ENTER_SCHEDULER':
                    stage_durations['sched'] += cur_stage_duration
                else:
                    stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_SCHEDULER_LO':
                if last_event != 'ENTER_LOCK_MANAGER':
                    print('WARNING! Strange Enter scheduler LO')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'DISPATCHED_FAST' or event == 'DISPATCHED_SLOW':
                if last_event != 'ENTER_SCHEDULER_LO' and last_event != 'ENTER_LOCK_MANAGER':
                    print('WARNING! Strange Dispatched fast')
                if last_event == 'ENTER_SCHEDULER_LO':
                    stage_durations['sched'] += cur_stage_duration
                else:
                    stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'ENTER_WORKER':
                if last_event != 'DISPATCHED_FAST' and last_event != 'DISPATCHED_SLOW':
                    print('WARNING! Strange Enter worker')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'GOT_REMOTE_READS':
                if last_event != 'ENTER_WORKER':
                    print('WARNING! Strange Got remote reads')
                stage_durations['idle'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_WORKER':
                if last_event != 'ENTER_LOCK_MANAGER' and last_event != 'ENTER_WORKER' and last_event != 'GOT_REMOTE_READS':
                    print('WARNING! Strange Exit worker')
                if last_event == 'ENTER_LOCK_MANAGER':
                    stage_durations['idle'] += cur_stage_duration
                else:
                    stage_durations['server'] += cur_stage_duration
                last_time = time
            elif event == 'RETURN_TO_SERVER':
                if last_event != 'EXIT_WORKER':
                    print('WARNING! Strange Return to server')
                stage_durations['other'] += cur_stage_duration
                last_time = time
            elif event == 'EXIT_SERVER_TO_CLIENT':
                if last_event != 'RETURN_TO_SERVER':
                    print('WARNING! Strange Exit server to client')
                stage_durations['server'] += cur_stage_duration
                last_time = time
            else:
                print('WARNING! Encountered an uncovered chain of events!')
        last_event = event
    
    return stage_durations['server'], stage_durations['seq'], stage_durations['sched'], stage_durations['idle'], stage_durations['other']

def extract_component_times(cur_events):
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
    for _, event_row in cur_events.iterrows():
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
        0 # For 'other_ms'. To be compatible with new methodology
    )

for system in ordered_system_dirs:
    print(f"Analyzing system: {system}")
    #if system != 'Detock':
    #if system != 'janus' and system != 'calvin' and system != 'slog':
    #    continue
    clients = [item for item in os.listdir(join(data_folder, system, "client")) if os.path.isdir(join(data_folder, system, "client", item))]
    txns_csvs = [join(data_folder, system, "client", client, "transactions.csv") for client in clients]
    events_csvs = [join(data_folder, system, "client", client, "txn_events.csv") for client in clients]
    # Merge the CSVs together
    txns_csv = pd.DataFrame(columns=["txn_id","coordinator","regions","partitions","generator","restarts","global_log_pos","sent_at","received_at"])
    events_csv = pd.DataFrame(columns=["txn_id","event","time","machine","home"])
    for i in range(len(clients)):
        cur_txns_csv = pd.read_csv(txns_csvs[i])
        txns_csv = pd.concat([txns_csv, cur_txns_csv], ignore_index=True)
        cur_events_csv = pd.read_csv(events_csvs[i])
        events_csv = pd.concat([events_csv, cur_events_csv], ignore_index=True)
    # Group events by txn_id for fast access
    event_groups = events_csv.groupby("txn_id")
    # Prepare list to collect results
    results = []
    #txns_csv = txns_csv.tail(100) # For debugging purposes only, use the last 100 txns to speed up the script
    print(f"Total length: {len(txns_csv)} rows")
    for idx, txn in txns_csv.iterrows():
        if (idx % 10000) == 0:
            print(f"Cur row: {idx} of {len(txns_csv)}")
        txn_id = txn["txn_id"]
        sent_at = txn["sent_at"]
        received_at = txn["received_at"]
        duration_ms = (received_at - sent_at) * NANO_TO_MS
        is_mp = ';' in str(txn['partitions'])
        is_mh = ';' in str(txn['regions'])
        # Get event rows for this txn, if any
        if txn_id in event_groups.groups:
            #txn_events = event_groups.get_group(txn_id).sort_values("time")
            txn_events = event_groups.get_group(txn_id)
            if system == 'janus':
                server_ms, idle_ms, other_ms = extract_janus_component_times(txn_events)
            elif system == 'calvin':
                server_ms, seq_ms, sched_ms, idle_ms, other_ms = extract_calvin_component_times(txn_events)
            elif system == 'slog':
                server_ms, seq_ms, sched_ms, idle_ms, other_ms = extract_slog_component_times(txn_events)
            elif system == 'Detock':
                server_ms, seq_ms, sched_ms, idle_ms, other_ms = extract_detock_component_times(txn_events)
            else:
                server_ms, forwarder_ms, mh_orderer_ms, sequencer_ms, log_manager_ms, scheduler_ms, lck_man_ms, worker_ms, idle_ms, other_ms = extract_component_times(txn_events)
            #if system == 'janus':
            #    lck_man_ms += forwarder_ms
            #    forwarder_ms = 0.0
        if system == 'janus':
            # Other duration will already be computed directly
            results.append({
                "Txn_ID": txn_id,
                "Is MP": is_mp,
                "Is MH": is_mh,
                "Start time": sent_at,
                "End time": received_at,
                "Duration (ms)": round(duration_ms, 5),
                "Server (ms)": round(server_ms, 5),
                "Idle (ms)": round(idle_ms, 5),
                "Other (ms)": round(other_ms, 5),
                "Fwd (ms)": 0.0,
                "MH orderer (ms)": 0.0,
                "Seq (ms)": 0.0,
                "Log man (ms)": 0.0,
                "Sched (ms)": 0.0,
                "Lck man (ms)": 0.0,
                "Worker (ms)": 0.0,
            })
        elif system == 'calvin':
            results.append({
                "Txn_ID": txn_id,
                "Is MP": is_mp,
                "Is MH": is_mh,
                "Start time": sent_at,
                "End time": received_at,
                "Duration (ms)": round(duration_ms, 5),
                "Server (ms)": round(server_ms, 5),
                "Idle (ms)": round(idle_ms, 5),
                "Other (ms)": round(other_ms, 5),
                "Fwd (ms)": 0.0,
                "MH orderer (ms)": 0.0,
                "Seq (ms)": round(seq_ms, 5),
                "Log man (ms)": 0.0,
                "Sched (ms)": round(sched_ms, 5),
                "Lck man (ms)": 0.0,
                "Worker (ms)": 0.0,
            })
        elif system == 'slog':
            results.append({
                "Txn_ID": txn_id,
                "Is MP": is_mp,
                "Is MH": is_mh,
                "Start time": sent_at,
                "End time": received_at,
                "Duration (ms)": round(duration_ms, 5),
                "Server (ms)": round(server_ms, 5),
                "Idle (ms)": round(idle_ms, 5),
                "Other (ms)": round(other_ms, 5),
                "Fwd (ms)": 0.0,
                "MH orderer (ms)": 0.0,
                "Seq (ms)": round(seq_ms, 5),
                "Log man (ms)": 0.0,
                "Sched (ms)": round(sched_ms, 5),
                "Lck man (ms)": 0.0,
                "Worker (ms)": 0.0,
            })
        elif system == 'Detock':
            results.append({
                "Txn_ID": txn_id,
                "Is MP": is_mp,
                "Is MH": is_mh,
                "Start time": sent_at,
                "End time": received_at,
                "Duration (ms)": round(duration_ms, 5),
                "Server (ms)": round(server_ms, 5),
                "Idle (ms)": round(idle_ms, 5),
                "Other (ms)": round(other_ms, 5),
                "Fwd (ms)": 0.0,
                "MH orderer (ms)": 0.0,
                "Seq (ms)": round(seq_ms, 5),
                "Log man (ms)": 0.0,
                "Sched (ms)": round(sched_ms, 5),
                "Lck man (ms)": 0.0,
                "Worker (ms)": 0.0,
            })
        else:
            other_duration = round(max(0, duration_ms - server_ms - forwarder_ms - mh_orderer_ms - sequencer_ms - log_manager_ms - scheduler_ms - lck_man_ms - worker_ms - idle_ms - other_ms), 5)
            if other_duration > 100:
                print(f"Warning: Lots of unattributed latency in transaction {txn_id}!")
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
                "Idle (ms)": round(idle_ms, 5),
                "Other (ms)": other_duration,
            })
    latency_breakdown_df = pd.DataFrame(results)
    os.makedirs(output_folder, exist_ok=True)
    latency_breakdown_df.to_csv(os.path.join(output_folder, f"latency_breakdown_{system}.csv"), index=False)
    # Define txn subcategories
    sp_sh_df = latency_breakdown_df[(~latency_breakdown_df["Is MP"]) & (~latency_breakdown_df["Is MH"])]
    mp_sh_df = latency_breakdown_df[(latency_breakdown_df["Is MP"]) & (~latency_breakdown_df["Is MH"])]
    mp_mh_df = latency_breakdown_df[(latency_breakdown_df["Is MP"]) & (latency_breakdown_df["Is MH"])]
    # Compute base stats
    '''summary_stats_all = latency_breakdown_df[[
        "Duration (ms)",
        "Server (ms)",
        "Fwd (ms)",
        "Seq (ms)",
        "MH orderer (ms)",
        "Log man (ms)",
        "Sched (ms)",
        "Lck man (ms)",
        "Worker (ms)",
        "Idle (ms)",
        "Other (ms)"
    ]].agg(['mean', 'std'])'''
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
        "Idle (ms)",
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
        "Idle (ms)",
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
        "Idle (ms)",
        "Other (ms)"
    ]].agg(['mean', 'std'])

    # Flatten into one row
    summary_flat = pd.DataFrame([{
        "System": system,
        # All Txns
        '''"Avg Duration (ms)": round(summary_stats_all.loc['mean', 'Duration (ms)'], 5),
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
        "Avg Idle (ms)": round(summary_stats_all.loc['mean', 'Idle (ms)'], 5),
        "Std Idle (ms)": round(summary_stats_all.loc['std', 'Idle (ms)'], 5),
        "Idle (%)": round(summary_stats_all.loc['mean', 'Idle (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),
        "Avg Other (ms)": round(summary_stats_all.loc['mean', 'Other (ms)'], 5),
        "Std Other (ms)": round(summary_stats_all.loc['std', 'Other (ms)'], 5),
        "Other (%)": round(summary_stats_all.loc['mean', 'Other (ms)'] / summary_stats_all.loc['mean', 'Duration (ms)'], 5),'''
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
        "SP_SH Avg Idle (ms)": round(summary_stats_sp_sh.loc['mean', 'Idle (ms)'], 5),
        "SP_SH Std Idle (ms)": round(summary_stats_sp_sh.loc['std', 'Idle (ms)'], 5),
        "SP_SH Idle (%)": round(summary_stats_sp_sh.loc['mean', 'Idle (ms)'] / summary_stats_sp_sh.loc['mean', 'Duration (ms)'], 5),
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
        "MP_SH Avg Idle (ms)": round(summary_stats_mp_sh.loc['mean', 'Idle (ms)'], 5),
        "MP_SH Std Idle (ms)": round(summary_stats_mp_sh.loc['std', 'Idle (ms)'], 5),
        "MP_SH Idle (%)": round(summary_stats_mp_sh.loc['mean', 'Idle (ms)'] / summary_stats_mp_sh.loc['mean', 'Duration (ms)'], 5),
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
        "MP_MH Avg Idle (ms)": round(summary_stats_mp_mh.loc['mean', 'Idle (ms)'], 5),
        "MP_MH Std Idle (ms)": round(summary_stats_mp_mh.loc['std', 'Idle (ms)'], 5),
        "MP_MH Idle (%)": round(summary_stats_mp_mh.loc['mean', 'Idle (ms)'] / summary_stats_mp_mh.loc['mean', 'Duration (ms)'], 5),
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
    "Server (%)", "Fwd (%)", "Seq (%)", "MH orderer (%)", "Log man (%)", "Sched (%)", "Lck man (%)", "Worker (%)", "Idle (%)", "Other (%)",
    "SP_SH Server (%)", "SP_SH Fwd (%)", "SP_SH Seq (%)", "SP_SH MH orderer (%)", "SP_SH Log man (%)", "SP_SH Sched (%)", "SP_SH Lck man (%)", "SP_SH Worker (%)", "SP_SH Idle (%)", "SP_SH Other (%)",
    "MP_SH Server (%)", "MP_SH Fwd (%)", "MP_SH Seq (%)", "MP_SH MH orderer (%)", "MP_SH Log man (%)", "MP_SH Sched (%)", "MP_SH Lck man (%)", "MP_SH Worker (%)", "MP_SH Idle (%)", "MP_SH Other (%)",
    "MP_MH Server (%)", "MP_MH Fwd (%)", "MP_MH Seq (%)", "MP_MH MH orderer (%)", "MP_MH Log man (%)", "MP_MH Sched (%)", "MP_MH Lck man (%)", "MP_MH Worker (%)", "MP_MH Idle (%)", "MP_MH Other (%)",
]'''
columns_to_include_all = [
    "Server (%)", "Fwd (%)", "Seq (%)", "MH orderer (%)", "Log man (%)", "Sched (%)", "Lck man (%)", "Worker (%)", "Idle (%)", "Other (%)",
]
columns_to_include_sp_sh = [
    "SP_SH Server (%)", "SP_SH Fwd (%)", "SP_SH Seq (%)", "SP_SH MH orderer (%)", "SP_SH Log man (%)", "SP_SH Sched (%)", "SP_SH Lck man (%)", "SP_SH Worker (%)", "SP_SH Idle (%)", "SP_SH Other (%)",
]
columns_to_include_mp_sh = [
    "MP_SH Server (%)", "MP_SH Fwd (%)", "MP_SH Seq (%)", "MP_SH MH orderer (%)", "MP_SH Log man (%)", "MP_SH Sched (%)", "MP_SH Lck man (%)", "MP_SH Worker (%)", "MP_SH Idle (%)", "MP_SH Other (%)",
]
columns_to_include_mp_mh = [
    "MP_MH Server (%)", "MP_MH Fwd (%)", "MP_MH Seq (%)", "MP_MH MH orderer (%)", "MP_MH Log man (%)", "MP_MH Sched (%)", "MP_MH Lck man (%)", "MP_MH Worker (%)", "MP_MH Idle (%)", "MP_MH Other (%)",
]
column_super_categories = {
    'Server (%)': ['Server (%)', 'Fwd (%)', 'Worker (%)'],
    'Sequencer (%)': ['Seq (%)', 'MH orderer (%)', 'Log man (%)'],
    'Scheduler (%)': ['Sched (%)', 'Lck man (%)'],
    'Idle (%)': ['Idle (%)'],
    'Other (%)': ['Other (%)']
}
#columns_names = ["Server (%)", "Forwarder (%)", "Sequencer (%)", "MH orderer (%)", "Log manager (%)", "Scheduler (%)", "Lock manager (%)", "Worker (%)", "Other (%)"]
columns_names = ["Server", "Sequencer", "Scheduler", "Idle", "Other"]
cmap = plt.cm.get_cmap("OrRd")
cmap.set_bad("lightgrey")

fig, ax = plt.subplots(3,1, figsize=(5,4), constrained_layout=True)

# Heatmap for SP SH txns
summary_percentages = summary_combined[columns_to_include_sp_sh]
summary_percentages = summary_percentages.div(summary_percentages.sum(axis=1), axis=0) * 100
# New DataFrame to hold merged categories
merged_summary = pd.DataFrame()
# For each supercategory, find matching columns and sum them
for supercat, substrings in column_super_categories.items():
    matched_cols = [col for col in summary_percentages.columns if any(sub in col for sub in substrings)]
    merged_summary[supercat] = summary_percentages[matched_cols].sum(axis=1)
merged_summary.replace('NaN', np.nan, inplace=True)
merged_summary = merged_summary.to_numpy().round(5)
annot = np.where(np.isnan(merged_summary), 'N/A', merged_summary.astype(float)).astype(str) # Annotation matrix
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
plotted_columns_names = ['Server', 'Sequen-\ncer', 'Sched-\nuler', 'Idle', 'Other']
ax[0].set_xticklabels(plotted_columns_names, rotation=0, fontsize=8) # Remove x-ticks for further subplots
sys_names = summary_combined["System"]
sys_names = [SYSNAME_MAP[system] if system in SYSNAME_MAP else system for system in sys_names]
ax[0].set_yticklabels(sys_names, rotation=0, fontsize=8) # Keep region labels readable
ax[0].set_ylabel("LSH", fontsize=15, rotation=90, labelpad=10, va="center")

# Heatmap for MP SH txns
summary_percentages = summary_combined[columns_to_include_mp_sh]
summary_percentages = summary_percentages.div(summary_percentages.sum(axis=1), axis=0) * 100
# New DataFrame to hold merged categories
merged_summary = pd.DataFrame()
# For each supercategory, find matching columns and sum them
for supercat, substrings in column_super_categories.items():
    matched_cols = [col for col in summary_percentages.columns if any(sub in col for sub in substrings)]
    merged_summary[supercat] = summary_percentages[matched_cols].sum(axis=1)
merged_summary.replace('NaN', np.nan, inplace=True)
merged_summary = merged_summary.to_numpy().round(5)
annot = np.where(np.isnan(merged_summary), 'N/A', merged_summary.astype(float)).astype(str) # Annotation matrix
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
ax[1].set_yticklabels(sys_names, rotation=0, fontsize=8) # Keep region labels readable
ax[1].set_ylabel("FSH", fontsize=15, rotation=90, labelpad=10, va="center")

# Heatmap for MP MH txns
summary_percentages = summary_combined[columns_to_include_mp_mh]
summary_percentages = summary_percentages.div(summary_percentages.sum(axis=1), axis=0) * 100
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
ax[2].set_yticklabels(sys_names, rotation=0, fontsize=8) # Keep region labels readable
ax[2].set_ylabel("MH", fontsize=15, rotation=90, labelpad=10, va="center")

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
output_path = f'plots/output/{environment}/{workload}/latency_decomposition_{workload}'
png_path = output_path + '.png'
pdf_path = output_path + '.pdf'
plt.savefig(png_path, dpi=300, bbox_inches='tight')
plt.savefig(pdf_path, bbox_inches='tight')
plt.show()

print("Done")
