import os, csv
import pandas as pd

COST_MATRIX = pd.read_csv('aws/data_transfer_cost_matrix.csv', index_col=0)
EXP_REGIONS = ['eu-west-1','eu-west-2','us-west-1','us-west-2','us-east-1','us-east-2','ap-norteast-1','ap-norteast-2']

DEFAULT_AWS_REGION = 'eu-west-2'

def count_bytes_and_cost(input_file, start, end):
    # Contains: Time,From,To,FromBytes,ToBytes
    net_data = pd.read_csv(input_file, index_col=0)
    if '_' in input_file and input_file != 'aws/iftop_eg.csv':
        src_aws_region = input_file.split('_')[1]
    else:
        src_aws_region = DEFAULT_AWS_REGION
    print(f"Input region is: {src_aws_region}")

    # Filter for timesteps at relevant data points
    net_data = net_data.loc[net_data.index >= start]
    net_data = net_data.loc[net_data.index <= end]

    # Step 1: Calculate total bytes for each destination region
    start_bytes_per_destination = {}
    final_bytes_per_destination = {}
    total_bytes = 0
    for row in net_data.iterrows():
        dest = row[1]['To']
        if dest in start_bytes_per_destination.keys():
            final_bytes_per_destination[dest] = row[1]['ToBytes'] - start_bytes_per_destination[dest]
        else:
            start_bytes_per_destination[dest] = row[1]['ToBytes']
    for key in final_bytes_per_destination.keys():
        for region in EXP_REGIONS:
            if region in key:
                total_bytes += final_bytes_per_destination[key]
    print(f'Total bytes transfered: {total_bytes}')

    # Step 2: Calculate cost
    cur_region_cost = COST_MATRIX.loc[src_aws_region]
    total_cost = 0
    # Collect only transfers between AWS VMs (ignore e.g. SSH-ed connection)
    for key in final_bytes_per_destination.keys():
        for region in EXP_REGIONS:
            if region in key:
                total_cost += cur_region_cost[region] * final_bytes_per_destination[key] / 1000000000
    print(f'Total cost of data transfers: {total_cost}')

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Plot monitoring data.")
    parser.add_argument("-i", "--input_file", default='aws/iftop_eg.csv.csv', required=True, help="Path to the csv with network util data.")
    parser.add_argument("-s", "--start_time", default=1735663237187, type=int, required=True, help="Start time of the experiment.")
    parser.add_argument("-e", "--end_time", default=1735663242307, type=int, required=True, help="End time of the experiment.")
    args = parser.parse_args()    

    count_bytes_and_cost(args.input_file, args.start_time, args.end_time)

    print("Done")