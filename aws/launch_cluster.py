import boto3
from botocore.exceptions import ClientError
import os
from os.path import join
import glob
import sys
import time
from datetime import datetime
import json
import argparse
import paramiko
import subprocess as sp
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ['PYDEVD_WARN_EVALUATION_TIMEOUT'] = "15"

USER = 'ubuntu'
DEFAULT_AWS_REGION = 'us-west-1'
MAX_RETRIES = 1000

IPS_FILE = 'aws/ips.json'

KEY_FOLDER = 'keys'
LOGGING_FILE = 'aws/VM_launch_logging.log'

server_instances = []
client_instances = []
all_instances = []

# Helper functions
def load_config(config_file):
    """
    Load configuration from the JSON file.
    """
    with open(config_file, "r") as f:
        return json.load(f)

def load_region_ips_from_file():
    with open(IPS_FILE) as file:
        region_ips = json.load(file)
    return region_ips

def execute_remote_command(ssh_client, command):
    """
    Execute a command on a remote server over SSH.
    """
    _, stdout, stderr = ssh_client.exec_command(command)
    print(stdout.read().decode())
    print(stderr.read().decode())

instances = []

def ensure_key_pair(region, key_folder):
    """
    Ensures a key pair named 'my_aws_key_<region>' exists in the specified region.
    If it doesn't exist, creates it and saves the private key in the keys folder.
    """
    key_name = f"my_aws_key_{region}"
    client = boto3.client("ec2", region_name=region)
    private_key_file = os.path.join(key_folder, f"{key_name}.pem")

    try:
        # Check if the key pair already exists
        client.describe_key_pairs(KeyNames=[key_name])
        print(f"Key pair '{key_name}' already exists in {region}.")
    except client.exceptions.ClientError as e:
        if "InvalidKeyPair.NotFound" in str(e):
            # Create the key pair
            print(f"Key pair '{key_name}' not found in {region}. Creating...")
            response = client.create_key_pair(KeyName=key_name)
            key_material = response["KeyMaterial"]

            # Save the private key to the keys folder
            os.makedirs(key_folder, exist_ok=True)
            with open(private_key_file, "w") as f:
                f.write(key_material)
            os.chmod(private_key_file, 0o400)
            print(f"Key pair '{key_name}' created. Private key saved to '{private_key_file}'.")
        else:
            raise
    return key_name

def launch_instances(config, key_folder, num_servers, num_clients, single_region):
    """
    Launches one EC2 instance in each region specified in the configuration.
    """
    for region, _ in ec2_clients.items():
        if single_region:
            region_config = REGIONS[DEFAULT_AWS_REGION]
            key_name = f"my_aws_key_{DEFAULT_AWS_REGION}"
        else:
            region_config = REGIONS[region]
            key_name = f"my_aws_key_{region}"
        ensure_key_pair(region, key_folder)  # Ensure the key pair exists

        print(f"Launching instances in {region}...")
        ec2_session = ec2_sessions[region]
        attempt = 0
        while attempt < MAX_RETRIES:
            try:
                instances = ec2_session.create_instances(
                    ImageId=region_config["ami_id"],
                    InstanceType=config["server_vm_type"],
                    KeyName=key_name,
                    MaxCount=num_servers,
                    MinCount=num_servers,
                    SubnetId=region_config["subnet_id"],
                    SecurityGroupIds=[region_config["sg_id"]],
                    TagSpecifications=[
                        {
                            'ResourceType': 'instance',
                            'Tags': [{'Key': 'Name', 'Value': f'DetockVM_{region}'}],
                        }
                    ],
                )
                instances.extend( # Final VM is for the client
                    ec2_session.create_instances(
                        ImageId=region_config["ami_id"],
                        InstanceType=config["client_vm_type"],
                        KeyName=key_name,
                        MaxCount=num_clients, # Last instance is the client
                        MinCount=num_clients,
                        SubnetId=region_config["subnet_id"],
                        SecurityGroupIds=[region_config["sg_id"]],
                        TagSpecifications=[
                            {
                                'ResourceType': 'instance',
                                'Tags': [{'Key': 'Name', 'Value': f'DetockVM_{region}'}],
                            }
                        ],
                    )
                )
                break
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code in ("InsufficientInstanceCapacity", "RequestLimitExceeded", "InternalError"):
                    wait_time = 2 ** attempt
                    print(f"⚠️ Attempt {attempt + 1} failed with code: {error_code} — retrying in {wait_time}s")
                    time.sleep(wait_time)
                    attempt += 1
                else:
                    raise  # Don't retry on other errors
        if attempt == MAX_RETRIES:
            Exception('Unable to create enough EC2 instances!')
        # Rename instances in same region with index to distinuish between them
        for index, instance in enumerate(instances[:-1], start=1):
            unique_name = f'DetockVM_{region}_{index}'
            instance.create_tags(
                Tags=[{'Key': 'Name', 'Value': unique_name}]
            )
            server_instances.append({"InstanceId": instance.id, "Region": region, "Name": unique_name})
        # Special name for the client VMs
        client_name = f'ClientVM_{region}'
        instances[-1].create_tags(
            Tags=[{'Key': 'Name', 'Value': client_name}]
        )
        client_instances.append({"InstanceId": instances[-1].id, "Region": region, "Name": client_name})

def wait_for_instances(all_instances):
    """
    Waits until all instances are running and retrieves their public IPs.
    """
    public_ips = []
    private_ips = []
    region_ips = {}
    for region in REGIONS:
        region_ips[region] = []
    for instance in all_instances:
        region = instance["Region"]
        client = ec2_clients[region]
        instance_id = instance["InstanceId"]

        print(f"Waiting for instance {instance_id} in {region} to be running...")
        waiter = client.get_waiter("instance_running")
        waiter.wait(InstanceIds=[instance_id])

        response = client.describe_instances(InstanceIds=[instance_id])
        instance_info = response["Reservations"][0]["Instances"][0]
        public_ip = instance_info.get("PublicIpAddress")
        private_ip = instance_info.get("PrivateIpAddress")
        print(f"Instance {instance_id} in {region} is running with IP: {public_ip}")
        instance["PublicIp"] = public_ip
        instance["PrivateIp"] = private_ip
        public_ips.append(public_ip)
        private_ips.append(private_ip)
        region_ips[region].append({"ip": public_ip, 'private_ip': private_ip, "instance_id": instance_id, "server": 'DetockVM_' in instance["Name"]})
    
    with open(IPS_FILE, 'w') as fp:
        json.dump(region_ips, fp, indent=4)

    return public_ips, private_ips, region_ips

def setup_vm(public_ip, key_path, github_credentials, server_vm_type='r5.4xlarge'):
    """
    Clone the repository and execute the setup script on a remote VM.
    """
    print(f"Setting up VM at {public_ip}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname=public_ip, username="ubuntu", key_filename=key_path)

        # Transfer GitHub credentials
        sftp = ssh.open_sftp()
        sftp.put("aws/github_credentials.json", "/home/ubuntu/github_credentials.json")
        sftp.close()
        print("GitHub credentials transferred.")

        # Clone Detock repository
        clone_command = """
        export GIT_ASKPASS=/bin/echo &&
        echo {} > /tmp/token &&
        git clone https://{}:{}@github.com/delftdata/Detock.git
        """.format(github_credentials["token"], github_credentials["username"], github_credentials["token"])
        execute_remote_command(ssh, clone_command)
        print("Detock Repository cloned.")

        # Clone iftop repo
        # For now let's just estimate the average cost
        '''clone_command = """
        export GIT_ASKPASS=/bin/echo &&
        echo {} > /tmp/token &&
        git clone https://{}:{}@github.com/delftdata/iftop.git
        """.format(github_credentials["token"], github_credentials["username"], github_credentials["token"])
        execute_remote_command(ssh, clone_command)
        print("Iftop Repository cloned.")'''

        # Run the setup script
        setup_command = "bash /home/ubuntu/Detock/aws/setup.sh"
        execute_remote_command(ssh, setup_command)
        print("Setup script executed.")

    except Exception as e:
        print(f"⚠️ Error setting up VM {public_ip}: {e}")
    finally:
        ssh.close()

def setup_vms(all_instances, single_region, server_vm_type='r5.4xlarge'):
    # Load GitHub credentials
    with open("aws/github_credentials.json", "r") as f:
        github_credentials = json.load(f)

    def setup_task(instance):
        if single_region:
            key_path = os.path.join(KEY_FOLDER, f"my_aws_key_{DEFAULT_AWS_REGION}.pem")
        else:
            key_path = os.path.join(KEY_FOLDER, f"my_aws_key_{instance['Region']}.pem")
        setup_vm(instance["PublicIp"], key_path, github_credentials, server_vm_type=server_vm_type)

    # Setup all VMs concurrently
    with ThreadPoolExecutor() as executor:
        executor.map(setup_task, all_instances)

def stop_cluster():
    """
    Terminates all instances launched during this session.
    """
    region_ips = load_region_ips_from_file()

    for region in list(region_ips.keys()):
        region_instance_ids = []
        for region_instance in region_ips[region]:
            region_instance_ids.append(region_instance["instance_id"])

        print(f"Terminating instances {str(region_instance_ids)} in {region}...")
        ec2_clients[region].terminate_instances(InstanceIds=region_instance_ids)
        print(f"Instances {str(region_instance_ids)} in {region} terminated.")

def test_connectivity_between_regions(region_ips, username='ubuntu', single_region=False):
    """
    Tests connectivity between instances in different regions by SSHing into them
    and pinging other instances. Saves the round-trip time (RTT) as a matrix CSV.
    
    Args:
        region_ips (dict): Dictionary of regions with instance public IPs and IDs.
        key_file (str): Path to the private key file for SSH.
        username (str): SSH username (e.g., "ubuntu").
    """
    print("Testing connectivity between VMs across regions...")

    # Prepare a blank RTT matrix with region names as headers
    regions = list(region_ips.keys())
    rtt_matrix = [[""] + regions]  # First row header

    for src_region in regions:
        # Just use the 1st VM in each region to test ping latencies
        src_ip = region_ips[src_region][0]["ip"]
        row = [src_region]  # First column header

        # SSH into the source instance
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if single_region:
                ssh_client.connect(hostname=src_ip, username=username, key_filename=os.path.join(KEY_FOLDER, f"my_aws_key_{DEFAULT_AWS_REGION}.pem"))
            else:
                ssh_client.connect(hostname=src_ip, username=username, key_filename=os.path.join(KEY_FOLDER, f"my_aws_key_{src_region}.pem"))
            print(f"Connected to {src_region} ({src_ip})")

            # Test connectivity to other instances
            for dest_region in regions:
                dest_ip = region_ips[dest_region][0]["private_ip"]
                # Execute ping command on the remote VM
                _, stdout, _ = ssh_client.exec_command(f"ping -c 1 {dest_ip}")
                ping_output = stdout.read().decode()
                rtt_time = "N/A"
                if "time=" in ping_output:
                    for line in ping_output.splitlines():
                        if "time=" in line:
                            rtt_time = line.split("time=")[1].split(" ")[0]  # Extract RTT
                            break
                row.append(rtt_time)
                print(f"RTT from {src_region} to {dest_region}: {rtt_time} ms")

            ssh_client.close()
        except Exception as e:
            print(f"⚠️ Error connecting to {src_region} ({src_ip}): {e}")
            row += ["Error"] * len(regions)
        rtt_matrix.append(row)

    # Save RTT matrix as CSV
    if not single_region:
        cur_time = int(time.time())
        cur_timestamp = str(datetime.utcfromtimestamp(cur_time)).replace(' ', '_').replace(':','_')[:19]
        output_file = f"plots/data/aws/rtts/rtt_matrix_regions_{cur_timestamp}.csv"
        with open(output_file, mode="w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(rtt_matrix)
        print(f"RTT matrix saved to {output_file}")

def get_ssh_cmd(region_ips):
    ip = None
    for vm in region_ips[DEFAULT_AWS_REGION]:
        if not vm['server']:
            ip = vm['ip']
    ssh_cmd = f"ssh -i keys/my_aws_key_{DEFAULT_AWS_REGION}.pem -o StrictHostKeyChecking=no {USER}@{ip}" 
    print(f"You can now ssh into your client using: {ssh_cmd}")

def update_conf_file_ips(database_configs="aws/conf_files"):
    conf_files = []
    for dirpath, _, filenames in os.walk(database_configs):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            conf_files.append(full_path)
    print(f"Updating IPs in all .conf files inside {database_configs}")

    for conf_file in conf_files:
        is_calvin = False
        if 'calvin' in conf_file:
            is_calvin = True

        # 1. Collect IPs from JSON
        with open(IPS_FILE, "r") as f:
            ips_data = json.load(f)

        regions = list(ips_data)
        regions_ip_lines = []
        if not is_calvin:
            for region in regions:
                cur_ips = ips_data[region]

                client_address = ''
                private_addresses = []
                public_addresses = []
                current_region_ip_lines = ['regions: {']
                for ip in cur_ips:
                    if ip['server']:
                        pub_ip = ip['ip']
                        priv_ip = ip['private_ip']
                        private_addresses.append(priv_ip)
                        public_addresses.append(pub_ip)
                        #current_region_ip_lines.append(f'    addresses: "{priv_ip}",')
                        #current_region_ip_lines.append(f'    public_addresses: "{pub_ip}",')
                    else:
                        client_address = ip['ip']
                for priv_ip in private_addresses:
                    current_region_ip_lines.append(f'    addresses: "{priv_ip}",')
                for pub_ip in public_addresses:
                    current_region_ip_lines.append(f'    public_addresses: "{pub_ip}",')
                # Append the lines for the client and replicas (hard-coded at the moment)
                current_region_ip_lines.append(f'    client_addresses: "{client_address}",')
                current_region_ip_lines.append('    num_replicas: 1,')
                current_region_ip_lines.append('}')
                regions_ip_lines.extend(current_region_ip_lines)
        else: # Special case for Calvin. All machines go into one single region
            regions_ip_lines.append('regions: {')
            client_addresses = []
            private_addresses = []
            public_addresses = []
            for region in regions:
                cur_ips = ips_data[region]
                for ip in cur_ips:
                    pub_ip = ip['ip']
                    priv_ip = ip['private_ip']
                    if ip['server']:
                        private_addresses.append(f'    addresses: "{priv_ip}",')
                        public_addresses.append(f'    public_addresses: "{pub_ip}",')
                    else:
                        client_addresses.append(f'    client_addresses: "{pub_ip}",')
            regions_ip_lines.extend(private_addresses)
            regions_ip_lines.extend(public_addresses)
            regions_ip_lines.extend(client_addresses)
            regions_ip_lines.append(f'    num_replicas: {len(regions)}')
            regions_ip_lines.append('}')

        # 2. Populate .conf with IPs
        with open(conf_file) as file:
            conf_lines = [line.rstrip() for line in file]

        new_conf_file_lines = []
        addresses_section = False
        addresses_section_reached = False
        for line in conf_lines:
            if 'regions: {' in line:
                addresses_section = True
                if not addresses_section_reached:
                    new_conf_file_lines = new_conf_file_lines + regions_ip_lines
                addresses_section_reached = True
            else:
                if not addresses_section:
                    if 'num_partitions: ' in line:
                        single_regions_ips_len = ips_data[list(ips_data.keys())[0]]
                        num_partitions = 0
                        for ip in single_regions_ips_len:
                            if ip['server']:
                                num_partitions += 1
                        new_conf_file_lines.append(f'num_partitions: {num_partitions}')
                    else:
                        new_conf_file_lines.append(line)
                if addresses_section and '}' in line:
                    addresses_section = False

        # 3. Write new IPs back to file
        with open(conf_file, 'w') as f:
            for line in new_conf_file_lines:
                f.write(f"{line}\n")
        print(f"Conf file {conf_file} updated with new IPs")
    print(f"All IPs in all .conf files inside {database_configs} updated!")

def copy_conf_files_to_client(database_configs, region_ips):
    # Get the IP of the 1st client (the one in us-west-1)
    ip = None
    for vm in region_ips[DEFAULT_AWS_REGION]:
        if not vm['server']:
            ip = vm['ip']
    pem_path=f'keys/my_aws_key_{DEFAULT_AWS_REGION}.pem'
    remote_path_basic = f'{USER}@{ip}:Detock/aws/'
    copy_conf_files_cmd = ['scp', '-i', pem_path, '-o', 'StrictHostKeyChecking=no', '-r', database_configs, remote_path_basic]
    print(f"Copying over updated conf files with command: {copy_conf_files_cmd}")
    result = sp.run(copy_conf_files_cmd, check=True, capture_output=True)
    if result.returncode != 0:
        print(f"⚠️ Failed to copy conf files with error: {result.stderr}")
    else:
        print("✅ Copied over conf files sucessfully!")
    # Copy IPs JSON over
    copy_ips_file_cmd = ['scp', '-i', pem_path, '-o', 'StrictHostKeyChecking=no', '-r', IPS_FILE, remote_path_basic]
    print(f"Copying over updated IPs file with command: {copy_ips_file_cmd}")
    result = sp.run(copy_ips_file_cmd, check=True, capture_output=True)
    if result.returncode != 0:
        print(f"⚠️ Failed to copy IPs file with error: {result.stderr}")
    else:
        print("✅ Copied over IPs file sucessfully!")
    # Copy over latency breakdown conf_files
    remote_ycsb_path_lat_breakdown = f'{USER}@{ip}:Detock/aws/conf_files/ycsb'
    copy_ycsb_lat_breakdown_conf_files_cmd = ['scp', '-i', pem_path, '-o', 'StrictHostKeyChecking=no', '-r', f'{database_configs}/ycsb/lat', remote_ycsb_path_lat_breakdown]
    print(f"Copying over updated latency breakdown conf files with command: {copy_ycsb_lat_breakdown_conf_files_cmd}")
    result = sp.run(copy_ycsb_lat_breakdown_conf_files_cmd, check=True, capture_output=True)
    if result.returncode != 0:
        print(f"⚠️ Failed to copy ycsb latency breakdown conf files with error: {result.stderr}")
    else:
        print("✅ Copied over ycsb latency breakdown conf files sucessfully!")
    remote_tpcc_path_lat_breakdown = f'{USER}@{ip}:Detock/aws/conf_files/tpcc'
    copy_tpcc_lat_breakdown_conf_files_cmd = ['scp', '-i', pem_path, '-o', 'StrictHostKeyChecking=no', '-r', f'{database_configs}/tpcc/lat', remote_tpcc_path_lat_breakdown]
    print(f"Copying over updated latency breakdown conf files with command: {copy_tpcc_lat_breakdown_conf_files_cmd}")
    result = sp.run(copy_tpcc_lat_breakdown_conf_files_cmd, check=True, capture_output=True)
    if result.returncode != 0:
        print(f"⚠️ Failed to copy tpcc latency breakdown conf files with error: {result.stderr}")
    else:
        print("✅ Copied over tpcc latency breakdown conf files sucessfully!")

def generate_ssh_config(region_ips, single_region, ssh_config_file_path='keys/config', key_dir='~/.ssh'):
    lines = []
    for region, nodes in region_ips.items():
        if single_region:
            key_path = f"{key_dir}/my_aws_key_{DEFAULT_AWS_REGION}.pem"
        else:
            key_path = f"{key_dir}/my_aws_key_{region}.pem"
        for i, node in enumerate(nodes):
            ip = node['ip']
            private_ip = node['private_ip']
            role = 'server' if node['server'] else 'client'
            alias = f"{region}-{role}-{i}"
            # Host alias entry
            lines.append(f"Host {alias}")
            lines.append(f"    HostName {ip}")
            lines.append(f"    User ubuntu")
            lines.append(f"    IdentityFile {key_path}")
            lines.append(f"    StrictHostKeyChecking no")
            lines.append(f"    UserKnownHostsFile /dev/null")
            lines.append("")
            # Direct IP entry
            lines.append(f"Host {ip}")
            lines.append(f"    User ubuntu")
            lines.append(f"    IdentityFile {key_path}")
            lines.append(f"    StrictHostKeyChecking no")
            lines.append(f"    UserKnownHostsFile /dev/null")
            lines.append("")
            # Private IP entry
            lines.append(f"Host {private_ip}")
            lines.append(f"    User ubuntu")
            lines.append(f"    IdentityFile {key_path}")
            lines.append(f"    StrictHostKeyChecking no")
            lines.append(f"    UserKnownHostsFile /dev/null")
            lines.append("")
    # Write to file
    with open(ssh_config_file_path, 'w') as f:
        f.write('\n'.join(lines))
    print(f"✅ SSH config written to {ssh_config_file_path}")

def copy_keys_to_all_vms(region_ips, single_region, key_base_dir='keys/', remote_key_dir='~/.ssh', max_workers=32):
    """
    Copies all PEM key files from local key_base_dir to each VM's ~/.ssh directory.
    """
    # Get list of all key files inside the 'keys' folder
    key_files = glob.glob(os.path.join(key_base_dir, '*.pem'))
    key_files.append('keys/config')
    if not key_files:
        print("⚠️ No .pem files found in", key_base_dir)
        return
    def copy_to_node(ip, region):
        if single_region:
            pem_file = f"{key_base_dir}my_aws_key_{DEFAULT_AWS_REGION}.pem"
        else:
            pem_file = f"{key_base_dir}my_aws_key_{region}.pem"
        results = []
        for key_path in key_files:
            try:
                sp.run(['scp', '-i', pem_file, '-o', 'StrictHostKeyChecking=no', key_path, f'ubuntu@{ip}:{remote_key_dir}/'], check=True)
                results.append((ip, key_path, True))
            except sp.CalledProcessError as e:
                results.append((ip, key_path, False, str(e)))
        return results
    # Paralellize copying across all nodes in the cluster
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit batch job
        for region, nodes in region_ips.items():
            for node in nodes:
                ip = node['ip']
                futures.append(executor.submit(copy_to_node, ip, region))
        # Report sucess/failure of batch job
        for future in as_completed(futures):
            for result in future.result():
                ip, key_path, success, *err = result
                if not success:
                    print(f"⚠️ Failed to copy {key_path} to {ip}: {err[0]}")
            print(f"✅ Copied all keys to {ip}")

def spawn_db_service(workload='YCSB', image='omraz/seq_eval:latest'):
    spawn_db_service_cmd = "python3.8 tools/admin.py start --image {} examples/{}.conf -u ubuntu -e GLOG_v=1"
    if workload == 'YCSB':
        print("Spawning YCSB-T DB service")
        conf_file = 'aws_cluster_ycsb'
        spawn_db_service_cmd = spawn_db_service_cmd.format(image, conf_file)
    elif workload == 'TPCC':
        print("Spawning YCSB-T DB service")
        conf_file = 'aws_cluster_tpcc'
        spawn_db_service_cmd = spawn_db_service_cmd.format(image, conf_file)
    else:
        print("Invalid workload selected")
    result = sp.run(spawn_db_service_cmd, shell=True, capture_output=True, text=True)
    if hasattr(result, "returncode") and result.returncode != 0:
        print(f"Spawning DB service failed with exit code {result.returncode}")

if __name__ == "__main__":
    AWS_ACTIONS = ["start", "status", "setup_db", "stop"]
    #                     Won't run!  8C 32GB HighB  8C 32GB 10B   8C 64GB 10B   8C 64GB 10B    16C 64GB 10B  16C 128GB 10B  16C 64GB 15B   32C 128GB 12.5B
    SUPPORTED_VM_TYPES = ['t2.micro', 'm4.2xlarge',  'm5.2xlarge', 'r5.2xlarge', 'r5a.2xlarge', 'm5.4xlarge', 'r5.4xlarge',  'm8g.4xlarge', 'm6i.8xlarge']
    # By default we use r5.4xlarge (following Detock's setup)
    DEFAULT_VM_TYPE = 'r5.4xlarge'

    parser = argparse.ArgumentParser(description="AWS Cluster Management Script")
    parser.add_argument("-a",  "--action", default="stop", choices=AWS_ACTIONS, help="Action to perform: start or stop the cluster.")
    parser.add_argument("-rc", "--regions_config", default="aws/aws_large.json", help="Path to the AWS regions config file.")
    parser.add_argument("-dc", "--database_configs", default="aws/conf_files", help="Path to the folder with all conf files for the experiment.")
    parser.add_argument("-sc", "--system_config", default="aws/conf_files/ycsb/tu_cluster_ycsb_ddr_ts.conf", help="Path to the conf file for the db system to launch.")
    parser.add_argument("-i",  "--image", default="omraz/seq_eval:latest", help="Docker image to use.")
    parser.add_argument("-ns", "--num_servers", default=2, type=int, help="No. of server instances to spawn per region.")
    parser.add_argument("-nc", "--num_clients", default=1, type=int, help="No. of client instances to spawn per region.")
    parser.add_argument("-sm", "--server_vm_type", default=DEFAULT_VM_TYPE, choices=SUPPORTED_VM_TYPES, help="AWS VM type to use for the experiment.")
    parser.add_argument("-cv", "--client_vm_type", default="m4.2xlarge", choices=SUPPORTED_VM_TYPES, help="AWS VM type to use for the experiment.")
    parser.add_argument("-sr", "--single_region", default=True, help="Whether to spwan all the homes within the same AWS region (for networking purposes).")

    args = parser.parse_args()

    action = args.action
    regions_config_file = args.regions_config
    database_configs = args.database_configs
    system_config = args.system_config
    image = args.image
    num_servers = args.num_servers
    server_vm_type = args.server_vm_type
    client_vm_type = args.client_vm_type
    single_region = args.single_region
    num_clients = args.num_clients

    regions_config = load_config(regions_config_file)
    REGIONS = regions_config["regions"]
    regions_config["server_vm_type"] = server_vm_type
    regions_config["client_vm_type"] = client_vm_type

    # Initialize AWS clients for each region
    if single_region:
        ec2_clients = {region: boto3.client("ec2", region_name=DEFAULT_AWS_REGION) for region in REGIONS.keys()}
    else:
        ec2_clients = {region: boto3.client("ec2", region_name=region) for region in REGIONS.keys()}

    # Initialize ec2 Sessions
    if single_region:
        ec2_sessions = {region: boto3.Session(profile_name='default', region_name=DEFAULT_AWS_REGION).resource('ec2') for region in REGIONS.keys()}
    else:
        ec2_sessions = {region: boto3.Session(profile_name='default', region_name=region).resource('ec2') for region in REGIONS.keys()}

    if action == "start":
        launch_instances(regions_config, KEY_FOLDER, num_servers, num_clients, single_region)
        all_instances += server_instances
        all_instances += client_instances
        public_ips, private_ips, region_ips = wait_for_instances(all_instances)
        setup_vms(all_instances, single_region, server_vm_type=server_vm_type)
        test_connectivity_between_regions(region_ips, single_region=single_region)
        update_conf_file_ips(database_configs=database_configs)
        copy_conf_files_to_client(database_configs, region_ips)
        generate_ssh_config(region_ips=region_ips, single_region=single_region)
        copy_keys_to_all_vms(region_ips=region_ips, single_region=single_region)
        get_ssh_cmd(region_ips)
        print("VMs launched and set up!")
    elif action == "status":
        region_ips = load_region_ips_from_file()
        public_ips = []
        for reg in region_ips.keys():
            for instance in region_ips[reg]:
                public_ips.append(instance["ip"])
        test_connectivity_between_regions(region_ips, single_region=single_region)
        get_ssh_cmd(region_ips)
    # TODO: Check whether this branch is even still needed if we now spawn the DBs directly from inside a client
    elif action == "setup_db":
        region_ips = load_region_ips_from_file()
        #update_conf_file_ips(database_configs=database_configs)
        #copy_conf_files_to_client(database_configs, region_ips)
        generate_ssh_config(region_ips=region_ips, single_region=single_region)
        copy_keys_to_all_vms(region_ips=region_ips, single_region=single_region)
        print("Database setup prepared!")
    elif action == "stop":
        stop_cluster()
        print("Cluster stopped!")
