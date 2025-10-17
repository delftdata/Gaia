#!/bin/bash

if [[ $# < 3 ]]; then
	echo "usage: $0 [scenario] [db] [user]"
	exit 1
fi

source ./build_detock/bin/activate





if [[ "$2" =~ ^(Detock|Calvin|SLOG)$ ]]; then
    BIN="slog"
else
    BIN="janus"
fi

if [[ "$2" = "Detock" ]]; then
    CONF="./examples/dsh/tu-cluster-dsh-detock.conf"
else if [[ "$2" = "calvin" ]]; then
    CONF="./examples/dsh/tu-cluster-dsh-calvin.conf"
else if [[ "$2" = "slog" ]]; then
    CONF="./examples/dsh/tu-cluster-dsh-slog.conf"
else if [[ "$2" = "janus" ]]; then
    CONF="./examples/dsh/tu-cluster-dsh-janus.conf"
else 
    echo "Invalid db choice $2"
    exit 1
fi

python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $3 -e GLOG_v=1 --bin $BIN
python3 tools/run_config_on_remote.py -s $1 -w dsh -u $3 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database $2 --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $3
