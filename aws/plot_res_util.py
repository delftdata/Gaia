import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_res_monitoring_data(csv_path, output_path):
    # Contains: Timestamp,Elapsed_time,CPU_util,Mem_util,Net_util,Disk_util
    data = pd.read_csv(csv_path)
    plt.figure(figsize=(12, 8))

    # Plot CPU usage
    plt.subplot(2, 2, 1)
    plt.plot(data['Elapsed_time'], data['CPU_util'], label='CPU Usage (%)')
    plt.ylim(0, 100)
    plt.xlabel('Time')
    plt.ylabel('CPU (%)')
    plt.title('CPU Usage')
    plt.legend()

    # Plot Memory usage
    plt.subplot(2, 2, 2)
    plt.plot(data['Elapsed_time'], data['Mem_util'], label='Memory Usage (%)')
    plt.ylim(0, 100)
    plt.xlabel('Time')
    plt.ylabel('Memory (%)')
    plt.title('Memory Usage')
    plt.legend()

    # Plot Network usage
    plt.subplot(2, 2, 3)
    plt.plot(data['Elapsed_time'], data['Net_util'], label='Network Usage (KB/s)')
    plt.xlabel('Time')
    plt.ylabel('Network (MB/s)')
    plt.title('Network Usage')
    plt.legend()

    # Plot Disk usage
    plt.subplot(2, 2, 4)
    plt.plot(data['Elapsed_time'], data['Disk_util'], label='Disk Usage (KB/s)')
    plt.xlabel('Time')
    plt.ylabel('Disk (MB/s)')
    plt.title('Disk Usage')
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"{output_path}.pdf")
    plt.show()
    plt.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Plot monitoring data.")
    parser.add_argument("-i", "--input_file", default='aws/res_util_eg.csv', required=True, help="Path to the csv with resource util data.")
    parser.add_argument("-o", "--output_file", default='aws/res_util_eg', required=True, help="Path for the output plot of resource util data.")
    args = parser.parse_args()    

    plot_res_monitoring_data(args.input_file, args.output_file)

    print("Done")