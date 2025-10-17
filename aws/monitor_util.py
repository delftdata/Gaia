import os
import subprocess as sp
from subprocess import Popen, PIPE
import shlex
import psutil
import time
import csv
from datetime import datetime
import threading

aws_region = ""
try:
    get_aws_region_command = '''TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s)
    curl -s -H "X-aws-ec2-metadata-token: $TOKEN" "http://169.254.169.254/latest/meta-data/placement/region"
    '''
    aws_region = "_" + sp.run(get_aws_region_command, shell=True, timeout=1, stdout=sp.PIPE, stderr=sp.PIPE).stdout.decode("utf-8")
except:
    print('Unable to fetch AWS region')
resource_output_file = f"utilization{aws_region}.csv"
network_output_file = f"iftop{aws_region}.csv"

start_time = int(time.time())
start_timestamp = str(datetime.utcfromtimestamp(start_time)).replace(' ', '_').replace(':','_')[:19]

# Create the CSV file with the header
with open(resource_output_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Timestamp", "Elapsed_time", "CPU_util", "Mem_util", "Net_util", "Disk_util"])

# Monitoring function
def monitor_res_util():
    net_prev = psutil.net_io_counters()
    disk_prev = psutil.disk_io_counters()

    while True:
        cur_time = int(time.time())
        elapsed_time = int(cur_time - start_time)

        # Collect utilization data
        cpu_util = psutil.cpu_percent(interval=1)
        mem_util = psutil.virtual_memory().percent
        net_curr = psutil.net_io_counters()
        net_util = (net_curr.bytes_sent + net_curr.bytes_recv - net_prev.bytes_sent - net_prev.bytes_recv) / 1_000_000  # In MB/s
        disk_curr = psutil.disk_io_counters()
        disk_util = (disk_curr.read_bytes + disk_curr.write_bytes - disk_prev.read_bytes - disk_prev.write_bytes) / 1_000_000  # In MB/s

        net_prev = net_curr
        disk_prev = disk_curr

        # Write data to CSV
        with open(resource_output_file, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([cur_time, elapsed_time, round(cpu_util, 2), round(mem_util, 2), round(net_util, 2), round(disk_util, 2)])

def monitor_net_util():
    #proc = Popen('python3 monitor_network.py >> net_raw_output.log', stdout=PIPE, stderr=PIPE)
    #proc = Popen('python3 monitor_network.py', stdout=PIPE, stderr=PIPE)
    #sp.run('python3 aws/monitor_network.py', shell=True)

    sp.run(f'sudo ./iftop -i enX0 -t -B -w {network_output_file} -x -r 1000', shell=True)

if __name__ == "__main__":
    print(f"Monitoring started. Resource util will be saved to '{resource_output_file}'. \
          Network util will be saved to '{network_output_file}'. Press Ctrl+C to stop.")
    res_util_thread = threading.Thread(target=monitor_res_util, daemon=True)
    res_util_thread.start()

    net_util_thread = threading.Thread(target=monitor_net_util, daemon=True)
    net_util_thread.start()

    while True:
        time.sleep(1)
