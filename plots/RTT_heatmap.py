import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import os
from os.path import join

AVG_RTT_CSV_PATH = 'plots/data/rtt_matrix_regions.csv'
STD_RTT_CSV_PATH = 'plots/data/rtt_std_matrix_regions.csv'

SOURCE_CSV_FOLDER_PATH = 'plots/data/final/rtts'

# Desired order: us-west, us-east, eu-west, ap-northeast
ordered_regions = ['usw1', 'usw2', 'use1', 'use2', 'euw1', 'euw2', 'apne1', 'apne2']

# Map long region names to abbreviations
short_region_map = {
    "ap-northeast-1": "apne1",
    "ap-northeast-2": "apne2",
    "eu-west-1": "euw1",
    "eu-west-2": "euw2",
    "us-east-1": "use1",
    "us-east-2": "use2",
    "us-west-1": "usw1",
    "us-west-2": "usw2"
}

data_dfs = []
for file in os.listdir(SOURCE_CSV_FOLDER_PATH):
    data_dfs.append(pd.read_csv(join(SOURCE_CSV_FOLDER_PATH, file), index_col=0))
# Convert list of DataFrames to 3D NumPy array (shape: num_files x rows x cols)
data_array = np.stack([df.to_numpy(dtype=float) for df in data_dfs])
# Compute element-wise mean and std, ignoring NaNs
full_data = np.nanmean(data_array, axis=0)
full_data_std = np.nanstd(data_array, axis=0)
# Convert back to DataFrames with correct labels
index = data_dfs[0].index
columns = data_dfs[0].columns
data = pd.DataFrame(full_data, index=index, columns=columns)
full_data_std = pd.DataFrame(full_data_std, index=index, columns=columns)

# Replace 'N/A' with np.nan for numeric calculations
data.replace('N/A', np.nan, inplace=True)

# Convert numeric columns to float
data = data.apply(pd.to_numeric, errors='coerce')
np_data = data.to_numpy()

# Round numeric values and replace NaN with placeholders for display
rounded_data = data.round().to_numpy()  # Convert to numpy for easier handling
full_data_std.columns = [short_region_map[name] for name in full_data_std.columns]
full_data_std.index = [short_region_map[name] for name in full_data_std.index]
full_data_std = full_data_std.reindex(index=ordered_regions, columns=ordered_regions)

annot = np.empty_like(data, dtype=object)
for i in range(len(data)):
    for j in range(len(data.columns)):
        mean_val = data.iloc[i, j]
        std_val = full_data_std.iloc[i, j]
        if np.isnan(mean_val):
            annot[i, j] = 'N/A'
        elif i == j:
            # Diagonal: show mean with 2 decimal places, no std
            annot[i, j] = f"{mean_val:.2f}\n±{std_val:.2f}"
        else:
            # Off-diagonal: rounded mean ± rounded std
            annot[i, j] = f"{int(round(mean_val))}\n±{int(round(std_val))}"

data.columns = [short_region_map[name] for name in data.columns]
data.index = [short_region_map[name] for name in data.index]

# Reorder both rows and columns
data = data.reindex(index=ordered_regions, columns=ordered_regions)

# Plot the heatmap
plt.figure(figsize=(5, 3))
ax = sns.heatmap(
    data=data,  # Plot the rounded numpy data
    #annot=annot,        # Custom annotation matrix
    fmt='',             # Allow custom formatting
    cmap="coolwarm", 
    cbar=True, 
    linewidths=0.5,
    cbar_kws={"shrink": 0.7}, 
    mask=np.isnan(rounded_data),  # Mask NaN values
    vmin=0,
    vmax=250
)

# Get the colormap and normalization used in the heatmap
cmap = plt.get_cmap("coolwarm")
norm = plt.Normalize(vmin=0, vmax=250)

# Overlay custom cell annotations
for i in range(data.shape[0]):
    for j in range(data.shape[1]):
        mean_val = data.iloc[i, j]
        std_val = full_data_std.iloc[i, j]
        if not np.isnan(mean_val):# Get background color of the cell
            # Small hack to get the text white vs. black depending on the cell color
            color = cmap(norm(mean_val))
            r, g, b = color[:3]
            # Compute luminance to decide text color
            luminance = 0.299 * r + 0.587 * g + 0.114 * b
            text_color = 'black' if luminance > 0.5 else 'white'
            # FInding the text position to annotate
            x = j + 0.5
            y = i + 0.5
            # Mean in large font
            ax.text(
                x, y - 0.1,
                f"{mean_val:.2f}" if i == j else f"{int(round(mean_val))}",
                ha='center', va='center',
                fontsize=9, fontweight='bold', color=text_color
            )
            # Std dev in smaller font below
            ax.text(
                x, y + 0.2,
                f"±{std_val:.2f}" if i == j else f"±{int(round(std_val))}",
                ha='center', va='center',
                fontsize=6, color=text_color
            )

# Move the x-axis to the top
plt.gca().xaxis.set_ticks_position('top')
plt.gca().set_xticklabels(data.columns, fontsize=9)
plt.gca().set_yticklabels(data.index, rotation=0, fontsize=9)  # Keep region labels readable

plt.tight_layout()

# Save the AVG and STD matricies
data.to_csv(AVG_RTT_CSV_PATH)
full_data_std.to_csv(STD_RTT_CSV_PATH)

# Save the plot
output_path = 'plots/output/RTT_heatmap'
jpg_path = output_path + '.jpg'
pdf_path = output_path + '.pdf'
plt.savefig(jpg_path, dpi=300, bbox_inches='tight')
plt.savefig(pdf_path, bbox_inches='tight')
plt.show()

print("Done")
