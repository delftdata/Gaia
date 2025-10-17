# DeathStar Hotels

All scenarios were run using basic bash scripts which can be found in the scripts folder. Copy the desired script to the home directory of st1 (can be changed but scripts would need to be changed too), ensure the `build_detock` folder is set up (for the Python venv), and `~/tools/run_config_on_remote.py` should be there as well. Finally, copy the dsh config files (the ones with tu-cluster in the name) into `~/examples/dsh/`. 


By default, these scripts use the docker image I created (this would need to be manually changed if you wanted a different one).

### Scenarios
Scenarios can be run using the following command. (i.e. this tests a scenario for all databases)
```
./scenario.sh [name] [user]
```

Valid choices for the name are `baseline`, `skew`, `packet_loss`, `network`, `scalability`, `subflower`, and `lat_breakdown`. 


### Single experiment

A single experiment for a single database can be run using the following command

```
./only_one.sh [name] [db] [user]
```

Valid choices for the name are the same as above, valid choices for the db are `Detock`, `calvin`, `slog`, and `janus`

### Latency breakdown

Latency breakdown can be run using the following command.

```
./lat_breakdown.sh [user]
```

The way the extraction script works means the contents of the folder `.../lat_breakdown/database_name/0.25/` needs to be moved into the parent directory, i.e. `.../lat_breakdown/database_name/`.


### Modified experimental parameters

Most of the relevant experimental parameters can be found in `execution/dsh/utils.h`. Size of the database can be modified in the `dsh_partitioning` tag in the config files. For relevant benchmark parameters, look in `workload/dsh.cpp`

### A couple relevant facts

Skew is implemented by the `sample_` and `sample_once` functions in `workload/dsh.cpp`. Sunflower scenario requires the number of pre-generated transactions and additionally the file containing CSV values. A more detailed explanation of the sunfloer files can be found in `workload/dsh.cpp` near the function `load_sunflower`.