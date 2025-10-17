# Running experiments on AWS

For the 'real' data, we need to run our experiments on a true cloud environment, such as AWS. This folder contains some scripts for launching and setting up an AWS cluster.

1. Spawn the cluster (from your own machine) `python3 aws/launch_cluster.py -a start -rc aws/aws.json -n 2 -vm m4.2xlarge`
    1. Choose the cluster architecture in the `.json` file.
    2. Use the `-n` param for the number of servers per region.
    3. Use the `-vm` param to choose the VM type.
2. SSH into one of your clients, e.g., `ssh -i keys/my_aws_key_us-west-1.pem -o StrictHostKeyChecking=no ubuntu@52.53.190.91`
3. Activate environment `source build_detock/bin/activate && cd Detock/`
4. Spawn the DB service (from within one of your AWS client VMs): `python3 tools/admin.py start --image omraz/seq_eval:latest aws/conf_files/tpcc/aws_tpcc_slog.conf -u ubuntu -e GLOG_v=1 --bin slog`
5. Test DB service status `python3 tools/admin.py status --image omraz/seq_eval:latest aws/conf_files/tpcc/aws_tpcc_slog.conf -u ubuntu`
6. Run a single experiment `python3 tools/admin.py benchmark --image omraz/seq_eval:latest aws/conf_files/ycsb/aws_ycsb_ddr_ts.conf -u ubuntu --txns 2000000 --seed 1 --clients 3000 --duration 60 --generators 2 -wl basic --param "mh=50,mp=50" 2>&1 | tee benchmark_cmd.log` OR `python3 tools/admin.py benchmark --image omraz/seq_eval:latest aws/conf_files/tpcc/aws_tpcc_ddr_ts.conf -u ubuntu --txns 2000000 --seed 1 --clients 250 --duration 60 --generators 2 -wl tpcc --param "rem_item_prob=0.01,rem_payment_prob=0.01" 2>&1 | tee benchmark_cmd.log`
7. Run a whole scenario `python3 tools/run_config_on_remote.py -i omraz/seq_eval:latest -m ubuntu@52.53.244.50 -s skew -w tpcc -c aws/conf_files/tpcc/aws_tpcc_slog.conf -u ubuntu -bl True -db slog 2>&1 | tee scenario_$(date +"%d-%m-%y_%H-%M-%S").log`
8. Run all systems `python3 tools/run_all_systems_on_remote.py -i omraz/seq_eval:latest -m ubuntu@52.53.244.50 -s scalability -w ycsb -cf aws/conf_files/ycsb -u ubuntu`
8. Stop a DB service `python3 tools/admin.py stop --image omraz/seq_eval:latest aws/conf_files/ycsb/aws_ycsb_ddr_ts.conf -u ubuntu`
9. When you are finished with the experiments, tear down the AWS VMs (from your own machine) `python3 aws/launch_cluster.py -a stop -rc aws/aws.json`

## AWS Cloud Console setup

Note, for AWS you need to set up a VPC, Subnet, Internet Gateway, Route Table and edit the default Security Group. See https://docs.google.com/document/d/1HXoccRjVSvRAXVXmRqj1_kl92spvj4EjNQXJ4Q4Zqf0/edit?pli=1&tab=t.0#heading=h.kxzmtij7uv1q for a full list of instructions.

TODO: Read missing conf files from here https://github.com/delftdata/Detock/commit/c1e982051992c2a6eb53c1bc7c635e6e09e29b26#diff-056a7651d50cab9050bf5a2d74006385257c7a796618b0c001868079ca545fd1L33-L34