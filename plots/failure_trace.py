import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

# Read data from CSV
csv_path = 'plots/data/failure_trace.csv'  # Adjust this path
data = pd.read_csv(csv_path)

# Extract data
percent_multi_home = data['Time']
metrics = ['Latency', 'Throughput', 'BytesTransferred', 'Aborts', 'Cost']
y_labels = [
    'Latency (ms)',
    'Throughput (txn/s)',
    'Bytes Transferred (MB)',
    'Aborts (#)',
    'Cost ($)'
]
failure_points = [5, 31, 72] # TODO: Replace with a randomly generated set of points
databases = ['Calvin', 'SLOG', 'Detock', 'Mencius', 'Atomic Multicast', 'Caerus']
line_styles = ['-', '--', '-.', ':', '-', '--']
colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple', 'tab:brown']

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
fig, axes = plt.subplots(1, 5, figsize=(15, 3), sharex=True)

for ax, metric, y_label in zip(axes, metrics, y_labels):
    for db, color, style in zip(databases, colors, line_styles):
        column_name = f'{metric}_{db}'
        if column_name in data.columns:  # Plot only if the column exists in the CSV
            ax.plot(
                percent_multi_home,
                data[column_name],
                label=db,
                color=color,
                linestyle=style
            )
    ax.set_title(metric)
    ax.set_ylabel(y_label)
    ax.set_xlabel('Time (s)')
    ax.grid(True)
    ax.set_xlim(0, 100)
    ax.set_xticks(np.linspace(0, 100, 6))  # 0%, 20%, ..., 100%
    ax.set_ylim(bottom=0)  # Remove extra whitespace below y=0

    # Vertical lines to show where failures happen
    for p in failure_points:
        ax.axvline(x=p, color='black')

# Add legend and adjust layout
handles, labels = axes[-1].get_legend_handles_labels()
fig.legend(handles, labels, loc='upper center', ncol=len(databases), bbox_to_anchor=(0.5, 1.1))
plt.tight_layout(rect=[0, 0, 1, 1])  # Further reduce whitespace

# Save figures
output_path = 'plots/output/failure_trace'
jpg_path = output_path + '.jpg'
pdf_path = output_path + '.pdf'
plt.savefig(jpg_path, dpi=300, bbox_inches='tight')
plt.savefig(pdf_path, bbox_inches='tight')
plt.show()

print("Done")