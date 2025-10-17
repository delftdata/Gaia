import pandas as pd
import os
import argparse

# Constants
NANO_TO_MS = 1e-6  # Convert nanoseconds to milliseconds

# Argument parser
parser = argparse.ArgumentParser(description="Calculate the duration of individual txn phases.")
parser.add_argument('-if', '--input_file', default='plots/raw_data/st/ycsb/lat_breakdown/slog/client/0-0/txn_events.csv', help='Path to file with raw txn event data')
parser.add_argument('-of', '--output_file', default='plots/raw_data/txn_events_duration.csv', help='Path to file where to store the calculated txn event durations')

args = parser.parse_args()
input_file = args.input_file
output_file = args.output_file

# Load data
df = pd.read_csv(input_file)
df = df.sort_values(by=["txn_id", "time"])  # Sort for consistent deltas

# Prepare list to hold deltas
rows = []

# Group by txn_id and compute deltas
for txn_id, group in df.groupby("txn_id"):
    sorted_group = group.sort_values("time")
    prev_row = None
    for _, row in sorted_group.iterrows():
        if prev_row is not None:
            delta_ns = row["time"] - prev_row["time"]
            delta_ms = delta_ns * NANO_TO_MS
            rows.append({
                "txn_id": txn_id,
                "prev_event": prev_row["event"],
                "prev_time": prev_row["time"],
                "curr_event": row["event"],
                "curr_time": row["time"],
                "delta_ms": round(delta_ms, 3)
            })
        prev_row = row

# Create a DataFrame of deltas
delta_df = pd.DataFrame(rows)

# Filter: keep only suspicious deltas
filtered_df = delta_df[(delta_df["delta_ms"] < -50) | (delta_df["delta_ms"] > 100)]
filtered_df = filtered_df[(filtered_df["prev_event"] != 'ENTER_LOG_MANAGER_IN_BATCH')]
filtered_df = filtered_df[(filtered_df["prev_event"] != 'EXIT_SEQUENCER_IN_BATCH')]

# Save to CSV
filtered_df.to_csv(output_file, index=False)

#print(f"Saved filtered delta events to {output_path}")

print("Done")