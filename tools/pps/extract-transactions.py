import os
import shutil

scenario = "network"
base_dir = f"plots/raw_data/pps/{scenario}"

for db in os.listdir(base_dir):
    db_path = os.path.join(base_dir, db)
    if os.path.isdir(db_path):
        for experiment in os.listdir(db_path):
            experiment_path = os.path.join(db_path, experiment)
            if os.path.isdir(experiment_path):
                client_path = os.path.join(experiment_path, "client")
                for archives in os.listdir(client_path):
                    archive_path = os.path.join(client_path, archives)
                    if archive_path.endswith('.tar.gz'):
                        archive_name = archives.split('.')[0]
                        dest_path = os.path.join(client_path, archive_name)
                        shutil.unpack_archive(archive_path, dest_path)
                        print(f"Extracted: {archive_path} -> {dest_path}")