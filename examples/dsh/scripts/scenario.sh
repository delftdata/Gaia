#!/bin/bash

if [[ $# < 2 ]]; then
	echo "usage: $0 [scenario] [user]"
	exit 1
fi

source ./build_detock/bin/activate


CONF="./examples/dsh/tu-cluster-dsh-detock.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $2 -e GLOG_v=1 --bin slog
python3 tools/run_config_on_remote.py -s $1 -w dsh -u $2 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database Detock --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $2

CONF="./examples/dsh/tu-cluster-dsh-janus.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $2 -e GLOG_v=1 --bin janus
python3 tools/run_config_on_remote.py -s $1 -w dsh -u $2 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database janus --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $2

CONF="./examples/dsh/tu-cluster-dsh-slog.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $2 -e GLOG_v=1 --bin slog
python3 tools/run_config_on_remote.py -s $1 -w dsh -u $2 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database slog --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $2

CONF="./examples/dsh/tu-cluster-dsh-calvin.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $2 -e GLOG_v=1 --bin slog
python3 tools/run_config_on_remote.py -s $1 -w dsh -u $2 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database calvin --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $2
