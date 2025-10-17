import subprocess
import sys

# === Configuration ===
user = "fcirtog"  # SSH user
dry_run = False   # Set to True to print commands without executing
DELAY_INTER_REGION = "50ms"
BASIC_IFTOP_CMD = "iftop -n -t -s 1 2>&1 | grep interface"

# Define regions and machines
region1 = {"st1": "131.180.125.40", "st2": "131.180.125.41"}
region2 = {"st3": "131.180.125.42", "st5": "131.180.125.57"}
regions = [region1, region2]
all_nodes = {**region1, **region2}

# IP → interface mapping
interfaces = {}

# === Helpers ===
def run_subprocess(cmd, dry=False):
    print(f"[CMD] {cmd}")
    if dry:
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def get_network_interfaces(ips_used):
    global interfaces
    try:
        result = run_subprocess(BASIC_IFTOP_CMD, dry_run)
        interface = result.stdout.split('\n')[0].split('interface: ')[1].strip()
        local_ip = subprocess.run("hostname -I", shell=True, capture_output=True, text=True).stdout.split()[0]
        interfaces[local_ip] = interface
        print(f"[Local] IP {local_ip} uses interface {interface}")
    except Exception as e:
        print(f"[Local] Could not detect interface: {e}")

    for ip in ips_used:
        try:
            ssh_target = f"{user}@{ip}"
            ssh_cmd = f"ssh {ssh_target} '{BASIC_IFTOP_CMD}'"
            result = run_subprocess(ssh_cmd, dry_run)
            cur_interface = result.stdout.split('\n')[0].split('interface: ')[1].strip()
            interfaces[ip] = cur_interface
            print(f"[Remote] IP {ip} uses interface {cur_interface}")
        except Exception as e:
            print(f"[Remote] Unable to detect interface for {ip}: {e}")

# === Apply delay between regions ===
def apply_inter_region_delay(src_ip, dst_ip):
    src_iface = interfaces.get(src_ip)
    if not src_iface:
        print(f"[WARN] No interface for {src_ip}, skipping.")
        return

    ssh = f"ssh {user}@{src_ip}"

    # Compose a compound SSH command that:
    cmd = (
        f"{ssh} '"  # SSH into the source machine
        # 1. Add a prio queuing discipline to the network interface (3 bands: 1:1, 1:2, 1:3)
        f"sudo tc qdisc add dev {src_iface} root handle 1: prio; "

        # 2. Add a netem (network emulator) delay to the 3rd band (1:3) of the prio queue
        f"sudo tc qdisc add dev {src_iface} parent 1:3 handle 30: netem delay {DELAY_INTER_REGION}; "

        # 3. Add a traffic filter to redirect packets destined for dst_ip into the delayed band (1:3)
        f"sudo tc filter add dev {src_iface} protocol ip parent 1:0 prio 3 "
        f"u32 match ip dst {dst_ip} flowid 1:3 || true'"  # Prevent crash if already exists
    )
    run_subprocess(cmd, dry_run)


# === Remove delay inside a region ===
def remove_intra_region_delay(src_ip, dst_ip):
    iface = interfaces.get(src_ip)
    if not iface:
        print(f"[WARN] No interface for {src_ip}, skipping.")
        return

    ssh = f"ssh {user}@{src_ip}"
    
    # Delete filter and qdisc to remove delay
    filter_cmd = (
        f"{ssh} 'sudo tc filter delete dev {iface} protocol ip parent 1:0 prio 3 "
        f"u32 match ip dst {dst_ip} flowid 1:3 || true'"
    )
    qdisc_cmd = (
        f"{ssh} 'sudo tc qdisc del dev {iface} root || true'"
    )
    
    # Print the commands for debugging
    print(f"[DEBUG] Deleting filter with command: {filter_cmd}")
    print(f"[DEBUG] Deleting qdisc with command: {qdisc_cmd}")
    
    run_subprocess(filter_cmd, dry_run)
    run_subprocess(qdisc_cmd, dry_run)


# === Main ===
if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Usage: inter-region.py [apply|remove]")
        sys.exit(1)

    all_ips = list(all_nodes.values())
    get_network_interfaces(all_ips)

    if sys.argv[1] == 'apply':
        print("\n[INFO] Applying inter-region delays...")
        for src_name, src_ip in region1.items():
            for dst_name, dst_ip in region2.items():
                apply_inter_region_delay(src_ip, dst_ip)
                apply_inter_region_delay(dst_ip, src_ip)
        print("\n✅ Done: Inter-region delays applied.")
    
    if sys.argv[1] == 'remove':
        print("\n[INFO] Removing inter-region delays...")
        for region in regions:
            ips = list(region.values())
            for i in range(len(ips)):
                for j in range(len(ips)):
                    if i != j:
                        remove_intra_region_delay(ips[i], ips[j])
        print("\n✅ Done: Inter-region delays removed.")
