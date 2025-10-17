import os
import shutil

scenario = "network"
source_dir = f"data/pps/{scenario}/final-no-3"
destination_dir = f"plots/raw_data/pps/{scenario}"
experiments_per_case = 1

os.makedirs(destination_dir, exist_ok=True)

db_name = {"detock": "Detock", "slog": "SLOG", "calvin": "Calvin", "janus": "janus"}

for db in os.listdir(source_dir):
    full_path = os.path.join(source_dir, db)
    for experiment in os.listdir(full_path):
        index = int(experiment.split('-')[0])
        case = experiment.split('-')[1]
        if index % experiments_per_case == 0:
            source_path = os.path.join(full_path, experiment)
            dest_path = os.path.join(destination_dir, db_name[db.lower()], case)
            shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
            print(f"Copied: {source_path} -> {dest_path}")
