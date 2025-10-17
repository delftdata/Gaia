import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np
import os
import argparse

VALID_SCENARIOS = ['baseline', 'skew', 'scalability', 'network', 'packet_loss', 'sunflower']
VALID_WORKLOADS = ['ycsb', 'tpcc', 'movr', 'movie', 'pps', 'dsh', 'smallbank']
VALID_ENVIRONMENTS = ['local', 'st', 'aws']
DEFAULT_LAT_PERCENTILES = "50;95;99" # Extracted data will contain p50, p90, p95, p99. For the plots we will use p50 p95 p99
TXN_TYPES = ['lsh', 'fsh', 'mh']
WORKLOAD_CAPITALIZATION = {'ycsb': 'YCSB', 'tpcc': 'TPC-C'}

def darken_color(color, factor):
    """Darkens a color toward black. Factor ∈ [0, 1], where 1 = original color, 0 = black."""
    rgb = mcolors.to_rgb(color)
    return tuple(c * factor for c in rgb)

def lighten_color(color, factor):
    """Lightens a color toward white. Factor ∈ [0, 1], where 1 = original color, 0 = white."""
    rgb = mcolors.to_rgb(color)
    return tuple(1 - (1 - c) * factor for c in rgb)

def make_plot(plot='baseline', workload='ycsb', env='st', latency_percentiles=[50, 95, 99], skip_aborts=False, separate_latencies=False, log_latencies=True, costs_per_txn=True):

    if workload == "pps":
        if plot == 'baseline':
            x_lab = 'OrderProduct Multi-Home (%)'
        elif plot == 'skew':
            x_lab = 'Skew Factor (HOT)'
        elif plot == 'scalability':
            x_lab = 'Input Throughput (txn/s)'
        elif plot == 'network':
            x_lab = 'Extra delay (ms)'
        elif plot == 'packet_loss':
            x_lab = 'Packets lost (%)'
        elif plot == 'example':
            x_lab = 'Example x-axis'
    else:
        if plot == 'baseline':
            x_lab = 'Geo-distribution (%)'
        elif plot == 'skew':
            x_lab = 'Skew factor (Theta)'
        elif plot == 'scalability':
            x_lab = 'Input Throughput (txn/s)'
        elif plot == 'network':
            x_lab = 'Extra delay (ms)'
        elif plot == 'packet_loss':
            x_lab = 'Packets lost (%)'
        elif plot == 'sunflower':
            x_lab = 'Sunflower falloff'
        elif plot == 'example':
            x_lab = 'Example x-axis'

    # Read data from CSV
    csv_path = f'plots/data/{env}/{workload}/{plot}.csv'
    data = pd.read_csv(csv_path)

    # Extract data
    xaxis_points = data['x_var']
    # For some experiments, we have to adjust the x_values
    if workload == 'tpcc' and plot == 'baseline':
        #xaxis_points = [0, 4, 8, 15, 20, 25, 29]
        xaxis_points = [0, 4, 8, 15, 20, 25, 29, 32, 34, 36, 38, 39]
    if workload == 'smallbank' and plot == 'baseline':
        #xaxis_points = [0, 4, 8, 15, 20, 25, 29]
        xaxis_points = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 49.15]
    #elif workload == 'tpcc' and plot == 'skew':
    #    xaxis_points = [250 - point for point in xaxis_points]
    elif workload == 'movr' and plot == 'baseline':
        xaxis_points = [x * 0.35 for x in xaxis_points]
    elif workload == 'dsh' and plot == 'baseline':
        xaxis_points = [x * 100 for x in xaxis_points]

    if not skip_aborts:
        metrics = ['throughput', 'latency', 'aborts', 'bytes', 'cost']
        y_labels = [
            'Throughput (txn/s)',
            'Latency (ms)',
            'Aborts (%)',
            'Data transfers (GB/s)',
            'Hourly Cost ($/h)'
        ]
        if separate_latencies:
            subplot_titles = ['Throughput', 'Latency (by txn type)', 'Aborts', 'Data transfers', 'Cost']
        else:
            subplot_titles = ['Throughput', 'Latency (log scale)', 'Aborts', 'Data transfers', 'Cost']
    else:
        metrics = ['throughput', 'latency', 'bytes', 'cost']
        y_labels = [
            'Throughput (txn/s)',
            'Latency (ms)',
            'Data transfers (GB/s)',
            'Hourly Cost ($/h)'
        ]
        if separate_latencies:
            subplot_titles = ['Throughput', 'Latency (by txn type)', 'Data transfers', 'Cost']
        else:
            subplot_titles = ['Throughput', 'Latency (log scale)', 'Data transfers', 'Cost']
    
    if costs_per_txn:
        subplot_titles[-1] = 'Cost per 10k txns'
        y_labels[-1] = 'Cost per 10k txns (¢)'
    if workload == 'tpcc':
        subplot_titles = ['' for title in subplot_titles] # We don't want to duplicate and clutter the plot

    databases = ['Calvin', 'SLOG', 'Detock', 'Janus']
    line_styles = ['-', '--', '-.', ':']
    colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red']

    # Configure Matplotlib global font size
    plt.rcParams.update({
        'font.size': 12,        # Increase font size for better readability
        'axes.titlesize': 14,
        'axes.labelsize': 12,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10
    })

    # Create figure and subplots
    if workload != 'tpcc':
        fig, axes = plt.subplots(1, len(metrics), figsize=(15, 2.5), sharex=True)
    else:
        fig, axes = plt.subplots(1, len(metrics), figsize=(15, 2.4), sharex=True)

    for ax, metric, y_label, subplot_title in zip(axes, metrics, y_labels, subplot_titles):
        min_latency = 1_000_000_000
        for db, color, style in zip(databases, colors, line_styles):
            if plot == 'scalability':
                column_name = f'{db}_input_throughput'
                if column_name in data.columns:
                    xaxis_points = data[column_name]
                    #xaxis_points = data[data[column_name].notnull()][column_name]
                elif column_name.lower() in data.columns:
                    #xaxis_points = data[data[column_name.lower()].notnull()][column_name.lower()]
                    xaxis_points = data[column_name.lower()]
            if metric != 'latency':
                column_name = f'{db}_{metric}'
                if column_name in data.columns:  # Plot only if the column exists in the CSV
                    if metric == 'bytes':
                        data[column_name] = data[column_name] / 1_000_000_000
                    ax.plot(xaxis_points, data[column_name], label=db, color=color, linestyle=style)
                elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                    if metric == 'bytes':
                        data[column_name.lower()] = data[column_name.lower()] / 1_000_000_000
                    ax.plot(xaxis_points, data[column_name.lower()],label=db, color=color, linestyle=style)
            else:
                cur_colors = [lighten_color(color=color, factor=0.5), mcolors.to_rgb(color), darken_color(color=color, factor=0.5)]
                if separate_latencies:
                    fixed_percentile = '95'
                    for txn_type, cur_color in zip(TXN_TYPES, cur_colors):
                        if db.lower() == 'calvin' or db.lower() == 'janus':
                            cur_color = cur_colors[1] # Janus & Calvin do not differentiate between txn types
                        column_name = f'{db}_{txn_type}_p{fixed_percentile}'
                        if column_name in data.columns:  # Plot only if the column exists in the CSV
                            ax.plot(xaxis_points, data[column_name], label=db, color=cur_color, linestyle=style)
                            min_latency = min(min_latency, min(data[column_name]))
                        elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                            ax.plot(xaxis_points, data[column_name.lower()], label=db, color=cur_color, linestyle=style)
                            min_latency = min(min_latency, min(data[column_name.lower()]))
                else:
                    for percentile, cur_color in zip(latency_percentiles, cur_colors):
                        column_name = f'{db}_p{percentile}'
                        if column_name in data.columns:  # Plot only if the column exists in the CSV
                            ax.plot(xaxis_points, data[column_name], label=db, color=cur_color, linestyle=style)
                            min_latency = min(min_latency, min(data[column_name]))
                        elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                            ax.plot(xaxis_points, data[column_name.lower()], label=db, color=cur_color, linestyle=style)
                            min_latency = min(min_latency, min(data[column_name.lower()]))
                if log_latencies:
                    ax.set_yscale('log')
                    
        ax.set_title(subplot_title)
        ax.set_ylabel(y_label)
        ax.set_xlabel(x_lab)
        ax.grid(True)
        if workload == 'pps':
            if plot == 'baseline':
                ax.set_xticks(np.linspace(0, 100, 6))  # 0%, 20%, ..., 100%
                ax.set_xlim(0, 100)
            elif plot == 'skew':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
                ax.set_xscale('log')
            elif plot == 'scalability':
                ax.set_xlim(left=0)
                #ax.set_xscale('log')
            elif plot == 'network':
                ax.set_xlim(left=0, right=1000)
            elif plot == 'packet_loss':
                ax.set_xlim(0, 10)
        elif workload == 'ycsb':
            if plot == 'baseline':
                ax.set_xticks(np.linspace(0, 100, 6))  # 0%, 20%, ..., 100%
                ax.set_xlim(0, 100)
            elif plot == 'skew':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
            elif plot == 'scalability':
                ax.set_xlim(left=0, right=200_000)
                #ax.set_xscale('log')
                if metric == 'throughput':
                    ax.set_ylim(top=75_000)
                elif metric == 'latency':
                    ax.set_ylim(top=100000)
                elif metric == 'bytes':
                    ax.set_ylim(top=3)
            elif plot == 'network':
                ax.set_xlim(left=0, right=1000)
            elif plot == 'packet_loss':
                ax.set_xlim(0, 10)
            elif plot == 'sunflower':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
        elif workload == 'tpcc':
            if plot == 'baseline':
                ax.set_xticks(np.linspace(0, 40, 5))  # 0%, 20%, ..., 100%
                ax.set_xlim(0, 40)
            elif plot == 'skew':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
            elif plot == 'scalability':
                ax.set_xlim(left=0, right=50_000)
                #ax.set_xscale('log')
                #if metric == 'latency':
                #    ax.set_ylim(top=60)
            elif plot == 'network':
                ax.set_xlim(left=0, right=1000)
            elif plot == 'packet_loss':
                ax.set_xlim(0, 10)
            elif plot == 'sunflower':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
        else:
            if plot == 'baseline':
                if workload == 'movr':
                    ax.set_xticks(np.linspace(0, 35, 6))  # 0%, 7.5%, ..., 35%
                    ax.set_xlim(0, 35)
                else:
                    ax.set_xticks(np.linspace(0, 100, 6))  # 0%, 20%, ..., 100%
                    ax.set_xlim(0, 100)
            elif plot == 'skew':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
            elif plot == 'scalability':
                ax.set_xlim(left=0)
                #ax.set_xscale('log')
            elif plot == 'network':
                ax.set_xlim(left=0, right=1000)
            elif plot == 'packet_loss':
                ax.set_xlim(0, 10)
            elif plot == 'sunflower':
                ax.set_xticks(np.linspace(0.0, 1.0, 6))  # 0.0, 0.2, ..., 1.0
                ax.set_xlim(0, 1)
        if not log_latencies or metric != 'latency':
            ax.set_ylim(bottom=0)  # Remove extra whitespace below y=0
        else:
            ax.set_ylim(bottom=1)
            #if min_latency > 0.01:
            #    ax.set_ylim(bottom=0.01)
            #else:
            #    ax.set_ylim(bottom=0.001)
        if metric == 'cost' and plot == 'scalability':
            ax.set_ylim(0,10)
            if workload == 'ycsb':
                ax.set_ylim(0,5)
        elif metric == 'throughput':
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x/1000:.0f}k'))
        if plot == 'scalability':
            ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x/1000:.0f}k'))

    # Add legend and adjust layout
    handles, labels = axes[-1].get_legend_handles_labels()
    labels = [l[:1].capitalize()+l[1:] for l in labels]
    if workload != 'tpcc':
        fig.legend(handles, labels, loc='upper center', ncol=len(databases), bbox_to_anchor=(0.5, 1.1))
    workload_type = 'default'
    if plot == 'skew':
        workload_type = 'skew'
    elif plot == 'baseline':
        workload_type = 'access\npatterns'
    fig.text(
            0.02, 0.5,                # (x, y) in figure coordinates — x=0.02 pushes it to the left
            f'{WORKLOAD_CAPITALIZATION[workload]} ({workload_type})',        # the label text
            va='center', ha='center', # center vertically
            rotation='vertical',      # vertical orientation
            fontsize=16, fontweight='bold'
        )
    plt.tight_layout(rect=[0.04, 0, 1, 1])  # Further reduce whitespace

    # Save figures
    output_path = f'plots/output/{env}/{workload}/{plot}_{workload}'
    png_path = output_path + '.png'
    jpg_path = output_path + '.jpg'
    pdf_path = output_path + '.pdf'
    os.makedirs('/'.join(output_path.split('/')[:-1]), exist_ok=True)
    plt.savefig(png_path, dpi=300, bbox_inches='tight')
    plt.savefig(pdf_path, bbox_inches='tight')
    plt.show()

def make_hw_plot(workload='ycsb', env='st', latency_percentiles=[50, 99], skip_aborts=True, separate_latencies=False, log_latencies=True, costs_per_txn=True):

    x_lab = 'VM Type'
    plot = 'vary_hw'

    # Read data from CSV
    csv_path = f'plots/data/{env}/{workload}/{plot}.csv'
    data = pd.read_csv(csv_path)

    # Extract data
    xaxis_points = data['x_var']

    if not skip_aborts:
        metrics = ['throughput', 'latency', 'aborts', 'bytes', 'cost']
        y_labels = [
            'Throughput (txn/s)',
            'Latency (ms)',
            'Aborts (%)',
            'Data transfers (GB/s)',
            'Hourly Cost ($/h)'
        ]

        subplot_titles = ['Throughput', 'Latency (log scale)', 'Aborts', 'Data transfers', 'Cost']
    else:
        metrics = ['throughput', 'latency', 'bytes', 'cost']
        y_labels = [
            'Throughput (txn/s)',
            'Latency (ms)',
            'Data transfers (GB/s)',
            'Hourly Cost ($/h)'
        ]
        subplot_titles = ['Throughput', 'Latency (log scale)', 'Data transfers', 'Cost']
    
    if costs_per_txn:
        subplot_titles[-1] = 'Cost per 10k txns'
        y_labels[-1] = 'Cost per 10k txns (¢)'
    if workload == 'tpcc':
        subplot_titles = ['' for title in subplot_titles] # We don't want to duplicate and clutter the plot

    databases = ['Calvin', 'SLOG', 'Detock', 'Janus']
    line_styles = ['-', '--', '-.', ':']
    colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red']

    # Configure Matplotlib global font size
    plt.rcParams.update({
        'font.size': 12,        # Increase font size for better readability
        'axes.titlesize': 14,
        'axes.labelsize': 12,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'legend.fontsize': 10
    })

    # Create figure and subplots
    if workload != 'tpcc':
        fig, axes = plt.subplots(1, len(metrics), figsize=(15, 2.5), sharex=True)
    else:
        fig, axes = plt.subplots(1, len(metrics), figsize=(15, 2.4), sharex=True)

    width = 0.2  # the width of the bars
    fs = 10

    default_xaxis_points = [-1.5*width + x for x in range(len(xaxis_points))]
    for ax, metric, y_label, subplot_title in zip(axes, metrics, y_labels, subplot_titles):
        min_latency = 1_000_000_000 # Small hack to get a 100% overboard value
        for db, color, style, i in zip(databases, colors, line_styles, range(len(databases))):
            cur_x_axis_points = [x+i*width for x in default_xaxis_points]
            if metric != 'latency' and metric != 'cost':
                column_name = f'{db}_{metric}'
                if column_name in data.columns:  # Plot only if the column exists in the CSV
                    if metric == 'bytes':
                        data[column_name] = data[column_name] / 1_000_000_000
                    ax.bar(cur_x_axis_points, data[column_name], width=width, label=db, color=color, edgecolor='#000000')
                elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                    if metric == 'bytes':
                        data[column_name.lower()] = data[column_name.lower()] / 1_000_000_000
                    ax.bar(cur_x_axis_points, data[column_name.lower()], width=width, label=db, color=color, edgecolor='#000000')
            elif metric == 'cost':
                cur_colors = [mcolors.to_rgb(color), lighten_color(color=color, factor=0.5)]
                column_name = f'{db}_fixed_cost'
                if column_name in data.columns:  # Plot only if the column exists in the CSV
                    ax.bar(cur_x_axis_points, data[column_name], width=width, label=db, color=cur_colors[0], edgecolor='#000000')
                    next_column_name = f'{db}_cost'
                    ax.bar(cur_x_axis_points, data[next_column_name]-data[column_name], bottom=data[column_name], width=width, color=cur_colors[1], edgecolor='#000000')
                elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                    ax.bar(cur_x_axis_points, data[column_name.lower()], width=width, label=db, color=cur_colors[0], edgecolor='#000000')
                    min_latency = min(min_latency, min(data[column_name.lower()]))
                    next_column_name = f'{db}_cost'
                    ax.bar(cur_x_axis_points, data[next_column_name.lower()]-data[column_name.lower()], bottom=data[column_name.lower()], width=width, color=cur_colors[1], edgecolor='#000000')
                pass
            else:
                cur_colors = [mcolors.to_rgb(color), lighten_color(color=color, factor=0.5)]
                if separate_latencies:
                    fixed_percentile = '95'
                    for txn_type, cur_color in zip(TXN_TYPES, cur_colors):
                        if db.lower() == 'calvin' or db.lower() == 'janus':
                            cur_color = cur_colors[1] # Janus & Calvin do not differentiate between txn types
                        column_name = f'{db}_{txn_type}_p{fixed_percentile}'
                        if column_name in data.columns:  # Plot only if the column exists in the CSV
                            ax.bar(cur_x_axis_points, data[column_name], width=width, label=db, color=cur_color, edgecolor='#000000')
                            min_latency = min(min_latency, min(data[column_name]))
                        elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                            ax.bar(cur_x_axis_points, data[column_name.lower()], width=width, label=db, color=cur_color, edgecolor='#000000')
                            min_latency = min(min_latency, min(data[column_name.lower()]))
                else:
                    # For bar plot we only want 2 stacked bars anyway, so we just hardcode this
                    column_name = f'{db}_p{latency_percentiles[0]}'
                    if column_name in data.columns:  # Plot only if the column exists in the CSV
                        ax.bar(cur_x_axis_points, data[column_name], width=width, label=db, color=cur_colors[0], edgecolor='#000000')
                        min_latency = min(min_latency, min(data[column_name]))
                        next_column_name = f'{db}_p{latency_percentiles[1]}'
                        ax.bar(cur_x_axis_points, data[next_column_name]-data[column_name], bottom=data[column_name], width=width, color=cur_colors[1], edgecolor='#000000')
                    elif column_name.lower() in data.columns:  # Plot only if the column exists in the CSV
                        ax.bar(cur_x_axis_points, data[column_name.lower()], width=width, label=db, color=cur_colors[0], edgecolor='#000000')
                        min_latency = min(min_latency, min(data[column_name.lower()]))
                        next_column_name = f'{db}_p{latency_percentiles[1]}'
                        ax.bar(cur_x_axis_points, data[next_column_name.lower()]-data[column_name.lower()], bottom=data[column_name.lower()], width=width, color=cur_colors[1], edgecolor='#000000')
                if log_latencies and metric == 'latency':
                    ax.set_yscale('log')
                    
        ax.set_title(subplot_title)
        ax.set_ylabel(y_label)
        ax.set_xlabel(x_lab)
        x = np.arange(len(xaxis_points))
        xaxis_points_short = [x[:-5] for x in xaxis_points]
        ax.set_xticks(x, xaxis_points_short)
        ax.grid(True)
        
        if not log_latencies or metric != 'latency':
            ax.set_ylim(bottom=0)  # Remove extra whitespace below y=0
        else:
            ax.set_ylim(bottom=1)
            #if min_latency > 0.01:
            #    ax.set_ylim(bottom=0.01)
            #lse:
            #   ax.set_ylim(bottom=0.001)
        if metric == 'throughput':
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x/1000:.0f}k'))

    # Add legend and adjust layout
    handles, labels = axes[-1].get_legend_handles_labels()
    labels = [l[:1].capitalize()+l[1:] for l in labels]
    if workload != 'tpcc':
        fig.legend(handles, labels, loc='upper center', ncol=len(databases), bbox_to_anchor=(0.5, 1.1))
    workload_type = 'default'
    if plot == 'skew':
        workload_type = 'skew'
    elif plot == 'baseline':
        workload_type = 'access\npatterns'
    fig.text(
            0.02, 0.5,                # (x, y) in figure coordinates — x=0.02 pushes it to the left
            f'{WORKLOAD_CAPITALIZATION[workload]} ({workload_type})',        # the label text
            va='center', ha='center', # center vertically
            rotation='vertical',      # vertical orientation
            fontsize=16, fontweight='bold'
        )
    plt.tight_layout(rect=[0.04, 0, 1, 1])  # Further reduce whitespace

    # Save figures
    output_path = f'plots/output/{env}/{workload}/{plot}_{workload}'
    png_path = output_path + '.png'
    jpg_path = output_path + '.jpg'
    pdf_path = output_path + '.pdf'
    os.makedirs('/'.join(output_path.split('/')[:-1]), exist_ok=True)
    plt.savefig(png_path, dpi=300, bbox_inches='tight')
    plt.savefig(pdf_path, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="System Evaluation Script")
    parser.add_argument("-p",  "--plot", default="baseline", choices=VALID_SCENARIOS, help="The name of the experiment we want to plot.")
    parser.add_argument("-w",  "--workload", default="ycsb", choices=VALID_WORKLOADS, help="The workload that was evaluated.")
    parser.add_argument('-e',  '--environment', default='aws', choices=VALID_ENVIRONMENTS, help='What type of machine the experiment was run on.')
    parser.add_argument("-sa", "--skip_aborts", default=False, help="Whether or not to plot the aborts (since many workloads don't have any).")
    parser.add_argument("-lp", "--latency_percentiles", default=DEFAULT_LAT_PERCENTILES, help="The latency percentiles to plot")
    parser.add_argument("-sl", "--separate_latencies", default=True, help="Whether or not to separate latencies by txn type.")
    parser.add_argument("-ll", "--log_latencies", default=True, help="Whether or not to plot the latency on a log scale.")
    parser.add_argument("-ct", "--costs_per_txn", default=True, help="Whether or not to plot the cost per transaction.")

    args = parser.parse_args()
    plot = args.plot
    workload = args.workload
    environment = args.environment
    skip_aborts = args.skip_aborts
    latency_percentiles = args.latency_percentiles
    separate_latencies = args.separate_latencies
    log_latencies = args.log_latencies
    costs_per_txn = args.costs_per_txn

    latencies = [int(latency) for latency in latency_percentiles.split(';')]

    make_plot(plot=plot,
              workload=workload,
              env=environment,
              latency_percentiles=latencies,
              skip_aborts=skip_aborts,
              separate_latencies=separate_latencies,
              log_latencies=log_latencies,
              costs_per_txn=costs_per_txn)

    print("Done")
