import pandas as pd
import matplotlib.pyplot as plt
import os
import argparse

def make_plot(csv_path, workload = 'ycsb'):

    # Read the data from the CSV file
    data = pd.read_csv(csv_path)

    data['Database'] = data['Database'].str[:4]

    # Define configurations for each subplot
    categories = [
        {"title": "Single-partition", "columns": ["Sequencer_SH", "Scheduler_SH", "Batcher_SH", "Forwarder_SH", "Idle_SH", "Total_SH"]},
        {"title": "Single-home (multi-partition)", "columns": ["Sequencer_SHMP", "Scheduler_SHMP", "Batcher_SHMP", "Forwarder_SHMP", "Idle_SHMP", "Total_SHMP"]},
        {"title": "Foreign single-home", "columns": ["Sequencer_FSH", "Scheduler_FSH", "Batcher_FSH", "Forwarder_FSH", "Idle_FSH", "Total_FSH"]},
        {"title": "Multi-home", "columns": ["Sequencer_MH", "Scheduler_MH", "Batcher_MH", "Forwarder_MH", "Idle_MH", "Total_MH"]},
    ]

    components = ["Sequencer", "Scheduler", "Batcher", "Forwarder", "Idle", "Other"]
    #hatch_patterns = ['/', '\\', '|', '-', '+', 'x']  # Hatching for components
    hatch_patterns = ['', '', '', '', '', '']  # No hatching

    # Create a plot with 4 subplots in 1 row
    fig, axes = plt.subplots(1, 4, figsize=(12, 3))  # Adjust the width as needed for paper formatting

    for idx, ax in enumerate(axes):
        # Extract data for the current category
        category = categories[idx]
        columns = category["columns"]
        latency_data = data[columns].copy()
        latency_data["Other"] = latency_data[columns[-1]] - latency_data[columns[:-1]].sum(axis=1)

        # Normalize the data
        for component, col in zip(components[:-1], columns[:-1]):
            latency_data[component] = latency_data[col] / latency_data[columns[-1]]
        latency_data["Other"] = latency_data["Other"] / latency_data[columns[-1]]

        # Plot the data
        bottom = [0] * len(data)
        x = range(len(data['Database']))
        for i, component in enumerate(components):
            ax.bar(
                x, 
                latency_data[component], 
                bottom=bottom, 
                label=component, 
                hatch=hatch_patterns[i], 
                edgecolor='black',  # Add thin black borders
                linewidth=0.5  # Set the border width to be very thin
            )
            bottom = [b + latency_data[component].iloc[j] for j, b in enumerate(bottom)]

        # Format the subplot
        ax.set_xticks(x)
        ax.set_xticklabels(data['Database'], fontsize=9)
        if idx == 0:
            ax.set_yticks([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
            ax.set_yticklabels([f"{ytick:.1f}" for ytick in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]], fontsize=8)  # Decrease font size for y-ticks
        else:
            ax.set_yticks([-1.0]) # Remove yticks for the 2nd, 3rd, 4th subplot
        ax.set_ylim(0, 1)
        ax.set_title(category["title"], fontsize=10)
        if idx == 0:
            ax.set_ylabel("Normalized Latency", fontsize=10)

    # Add a single legend above all subplots
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', ncol=len(components), fontsize=9, frameon=False)
    plt.subplots_adjust(top=0.8, wspace=0.3)  # Adjust space for the legend and between subplots

    # Save the plot
    plt.tight_layout(rect=[0, 0, 1, 0.95])  # Leave space for the legend
    output_path = 'plots/output/latency_breakdown'
    jpg_path = output_path + '.jpg'
    pdf_path = output_path + '.pdf'
    plt.savefig(jpg_path, dpi=300, bbox_inches='tight')
    plt.savefig(pdf_path, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="System Evaluation Script")
    parser.add_argument("-p",  "--csv_path", default="plots/data/examples/latency_data.csv", help="The name of the experiment we want to plot.")
    parser.add_argument("-w",  "--workload", default="ycsb", choices=["ycsb", "tpcc"], help="The workload that was evaluated.")
    args = parser.parse_args()

    make_plot(csv_path=args.csv_path, workload=args.workload)

    print("Done")
