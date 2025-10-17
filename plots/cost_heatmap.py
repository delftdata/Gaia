import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import argparse

VALID_WORKLOADS = ['ycsb', 'tpcc', 'movr', 'movie', 'pps', 'dsh', 'smallbank']

# Argument parser
parser = argparse.ArgumentParser(description="Plot cost heatmap for a given scenario.")
parser.add_argument('-w', '--workload', default='ycsb', choices=VALID_WORKLOADS, help='Workload to plot (default: ycsb)')

args = parser.parse_args()
workload = args.workload

# Read the data from the provided CSV file
csv_path = f'plots/data/aws/{workload}/costs.csv'

data = pd.read_csv(csv_path, index_col=0)

# Plot the heatmap with adjustments
plt.figure(figsize=(5, 1.5))

# Create the heatmap - prepare both data and annotations
data_plot = data.copy()  # Create a copy for plotting
data_plot = data_plot.fillna(-1) 

annot = pd.DataFrame(data_plot)

for row in range(annot.shape[0]):
    for cell in range(annot.shape[1]):
        val = annot.iat[row, cell]
        # Special case, e.g., when TPC-C fails on particular hardware due to insufficient memory
        if val == -1:
            annot.iat[row, cell] = "N/A"
        else:
            annot.iat[row, cell] = '$'+str(int(round(float(val), 0)))

cmap = plt.cm.get_cmap("OrRd")
cmap.set_bad("lightgrey")
# Calculate the actual data range excluding sentinel values
valid_data = data_plot[data_plot != -1]
vmin = valid_data.min().min()
vmax = valid_data.max().max()

# sns.heatmap(data_plot,  # Use the modified data with NaNs
#             annot=annot.values,
#             fmt="s",
#             cmap=cmap,
#             cbar=True,
#             linewidths=0.5, 
#             cbar_kws={"shrink": 1})

sns.heatmap(
    data=data_plot,
    annot=annot.values,
    fmt='',
    cmap=cmap,
    cbar=True,
    linewidths=0.5,
    linecolor='white',
    vmin=vmin,  # Set the minimum to exclude sentinel values
    vmax=vmax   # Set the maximum based on actual data
    )

# Manually color the -1 cells lightgrey
ax = plt.gca()
for i in range(data_plot.shape[0]):
    for j in range(data_plot.shape[1]):
        if data_plot.iloc[i, j] == -1:
            ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=True, color='lightgrey', 
                                     linewidth=1, edgecolor='white'))

# Move the x-axis to the top
plt.gca().xaxis.set_ticks_position('top')
plt.xticks(fontsize=10)
plt.yticks(rotation=0, fontsize=10)  # Keep hardware labels readable

# Set the labels for axes
plt.ylabel("AWS VM Type", fontsize=12)

plt.tight_layout()

# Save the plot
output_path = f'plots/output/aws/{workload}/cost_{workload}_heatmap'
jpg_path = output_path + '.jpg'
pdf_path = output_path + '.pdf'
plt.savefig(jpg_path, dpi=300, bbox_inches='tight')
plt.savefig(pdf_path, bbox_inches='tight')
plt.show()

print("Done")