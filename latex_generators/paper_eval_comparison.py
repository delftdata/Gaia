import pandas as pd

# Load CSV
csv_path = 'latex_generators/data/paper_eval_comparison.csv'
tex_output_file = 'latex_generators/output/paper_eval_comparison.tex'

df = pd.read_csv(csv_path)

# Define column renaming for LaTeX headers
column_rename = {
    'System': 'System / Publication',
    'PacketLoss': 'Packet Loss',
    'VaryHW': 'Varying HW',
    'DataTransfers': 'Data Transfers'
}

# Rename columns for display
df = df.rename(columns=column_rename)

# Generate LaTeX code
latex_lines = []
latex_lines.append("""\\begin{table}[t]""")
latex_lines.append("""\\centering""")
latex_lines.append("""\\caption{A comparison of the evaluation scenarios and aspects used in the evaluations of XXX deterministic database publications.}""")
latex_lines.append("""\\label{tab:evaluation_comparison}""")
latex_lines.append("""\\resizebox{\\columnwidth}{!}{%""")
column_headers_line = """\\begin{tabular}{|l|""" + 'c|' * (df.shape[1] - 1) + '}'
latex_lines.append(column_headers_line)
latex_lines.append("""\\hline""")

# Header rows
latex_lines.append("""\multicolumn{1}{|c|}{\multirow{2}{*}{\\textbf{\\begin{tabular}[c]{@{}c@{}}System /\\\\ Publication\end{tabular}}}} & \multicolumn{5}{c|}{\\textbf{Scenario}} & \multicolumn{2}{c|}{\\textbf{Metric}} \\\\ \cline{2-8}""")
latex_lines.append("""\multicolumn{1}{|c|}{} & \multicolumn{1}{c|}{Skew} & \multicolumn{1}{c|}{Scalability} & \multicolumn{1}{c|}{Network} & \multicolumn{1}{c|}{\\begin{tabular}[c]{@{}c@{}}Packet\\\\ loss\end{tabular}} & \multicolumn{1}{c|}{\\begin{tabular}[c]{@{}c@{}}Varying\\\\ HW\end{tabular}} & \multicolumn{1}{c|}{\\begin{tabular}[c]{@{}c@{}}Data\\\\ transfers\end{tabular}} & \multicolumn{1}{c|}{Cost} \\\\ \hline""")

headers = list(df.columns)

# Rows
for _, row in df.iterrows():
   row_values = [str(row[col]) for col in headers]
   if '(Ours)' in row_values[0]:
       row_values[0] = """\\textbf{""" + row_values[0] + """}"""
   latex_lines.append(' & '.join(row_values) + """ \\\\ \\hline""")

latex_lines.append("""\end{tabular}""")
latex_lines.append("""}""")
latex_lines.append("""\end{table}""")

# Write LaTeX code to a file
with open(tex_output_file, 'w') as f:
    for line in latex_lines:
        f.write(line + '\n')

print("LaTeX table written to 'eval_comparison_table.tex'")