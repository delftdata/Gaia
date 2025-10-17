#!/bin/bash

if [[ $# < 1 ]]; then
	echo "usage: $0 [user]"
	exit 1
fi

source ./build_detock/bin/activate



CONF="./examples/dsh/latency/tu-cluster-dsh-detock-lat.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $1 -e GLOG_v=1 --bin slog
python3 tools/run_config_on_remote.py -s lat_breakdown -w dsh -u $1 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database Detock --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $1

CONF="./examples/dsh/latency/tu-cluster-dsh-janus-lat.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $1 -e GLOG_v=1 --bin janus
python3 tools/run_config_on_remote.py -s lat_breakdown -w dsh -u $1 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database janus --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $1

CONF="./examples/dsh/latency/tu-cluster-dsh-slog-lat.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $1 -e GLOG_v=1 --bin slog
python3 tools/run_config_on_remote.py -s lat_breakdown -w dsh -u $1 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database slog --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $1

CONF="./examples/dsh/latency/tu-cluster-dsh-calvin-lat.conf"
python3 tools/admin.py start --image aidaneickhoff/detock:latest $CONF -u $1 -e GLOG_v=1 --bin slog
python3 tools/run_config_on_remote.py -s lat_breakdown -w dsh -u $1 --conf $CONF --machine st1 -d 60 -b aidan_benchmark --database calvin --image aidaneickhoff/detock:latest
python3 tools/admin.py stop --image aidaneickhoff/detock:latest $CONF -u $1
