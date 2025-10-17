import pandas as pd
import numpy as np
import matplotlib.colors as mcolors
import matplotlib.cm as cm
from matplotlib import colors
import re

csv_input_file = 'latex_generators/data/vm_specs.csv'
tex_output_file = 'latex_generators/output/vm_specs_table.tex'

def rescale(values):
    """Rescale values to range [0,1] for colormap scaling."""
    min_v, max_v = min(values), max(values)
    return [(v - min_v) / (max_v - min_v) for v in values]

def get_color(value, cmap, vmin=0.2, vmax=0.8):
    """Return a LaTeX-compatible color from a colormap, restricting extremes for readability."""
    norm = colors.Normalize(vmin=vmin, vmax=vmax)  # Avoid extremes
    norm_value = norm(np.clip(value, 0, 1))
    rgb = cmap(norm_value)[:3]
    
    # Blend slightly with white for lighter colors
    blend_factor = 0.2  # Increase for even lighter colors
    rgb = [(1 - blend_factor) * c + blend_factor for c in rgb]
    
    return "{:.2f},{:.2f},{:.2f}".format(*rgb)

def extract_network_speed(value, max_speed=None):
    """Extracts numeric network speed from a string."""
    match = re.search(r"(\d+)\s*Gigabit", value)
    if match:
        return int(match.group(1))
    if "High" in value and max_speed is not None:
        return max_speed  # Map 'High' to max value found
    return 0  # Default to 0 if unrecognized

def generate_latex_table(csv_file):
    df = pd.read_csv(csv_file)
    df.columns = [col.replace(" (", "\\\\(") for col in df.columns]
    df.columns = [col.replace(")", "\\\\)") for col in df.columns]
    col_headers = df.columns
    
    # Convert cost to numerical values
    df.iloc[:, -1] = df.iloc[:, -1].str.replace('$', '').astype(float)
    
    red_blue_cmap = cm.RdBu_r # Use Red-to-Blue for CPU, Memory, Network (Higher = Better)
    blue_red_cmap = cm.RdBu   # Use Blue-to-Red for Cost (Lower = Better)

    # Normalize values for colormaps
    vcpu_scaled = rescale(df[col_headers[1]])
    memory_scaled = rescale(df[col_headers[2]])
    cost_scaled = rescale(df[col_headers[4]])

    # Special handling for network speed strings
    #net_scaled = rescale(df.index)  # Dummy scalings based on row index
    numeric_speeds = [extract_network_speed(val) for val in df[col_headers[3]] if "High" not in val]
    max_speed = max(numeric_speeds) if numeric_speeds else 0
    df["Network Speed (Gbps)"] = df[col_headers[3]].apply(lambda x: extract_network_speed(x, max_speed))
    net_scaled = rescale(df["Network Speed (Gbps)"])

    latex_code = """\\begin{table}[t]
\centering
\caption{Resource parameters and per hour pricing of various AWS instance types.}
\label{tab:vm_instance_pricing}
\\resizebox{\columnwidth}{!}{
\\begin{tabular}{|l|r|r|r|r|}
\\hline
\multicolumn{1}{|c|}{\\textbf{VM Type}} & 
\multicolumn{1}{c|}{\\textbf{vCPUs}} & 
\multicolumn{1}{c|}{\\textbf{\\begin{tabular}[c]{@{}c@{}}Memory\\\\(GiB)\\end{tabular}}} & 
\multicolumn{1}{c|}{\\textbf{\\begin{tabular}[c]{@{}c@{}}Network\\\\Performance\\end{tabular}}} & 
\multicolumn{1}{c|}{\\textbf{\\begin{tabular}[c]{@{}c@{}}Cost per Hour\\\\(us-east-1)\\end{tabular}}} \\\\ 
\hline
"""
    
    for i, row in df.iterrows():
        vcpu_color = get_color(vcpu_scaled[i], blue_red_cmap)
        memory_color = get_color(memory_scaled[i], blue_red_cmap)
        net_color = get_color(net_scaled[i], blue_red_cmap)
        cost_color = get_color(cost_scaled[i], red_blue_cmap)
        
        latex_code += (
            f"{row['VM Type']} & "
            f"\cellcolor[rgb]{{{vcpu_color}}} {row[col_headers[1]]} & "
            f"\cellcolor[rgb]{{{memory_color}}} {row[col_headers[2]]} & "
            f"\cellcolor[rgb]{{{net_color}}} {row[col_headers[3]]} & "
            f"\cellcolor[rgb]{{{cost_color}}} \\$ {row[col_headers[4]]:.3f} \\\\ \\hline\n"
        )
    
    latex_code += """\end{tabular}
}
\end{table}"""
    
    with open(tex_output_file, "w") as f:
        f.write(latex_code)

print("Creating Latex code for table")
generate_latex_table(csv_input_file)

print("LaTeX table generated as table.tex")