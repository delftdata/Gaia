import subprocess

# === Configuration ===
dry_run = False   # Set to True to print commands without executing
# Round-trip time = 2 * delay
DELAY_INTER_REGION = "50ms"
# DELAY_INTRA_REGION = "1ms"

# === Define the regions ===
machines_to_regions = {
    "131.180.125.40": 0,
    "131.180.125.41": 0,
    "131.180.125.42": 1,
    "131.180.125.57": 1,
}


# === Helpers ===
def run_subprocess(cmd, dry=False):
    print(f"[CMD] {cmd}")
    if dry:
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


# === Apply all delays, both inter-region and intra-region ===
def apply_all_delays(user, interfaces):
    # Build tc commands for all machines
    tc_cmds = {ip: [] for ip in machines_to_regions.keys()}

    for machine, region in machines_to_regions.items():
        iface = interfaces.get(machine)
        if not iface:
            print(f"[WARNING] No interface for {machine}, skipping.")
            continue

        # Wipe any previous qdisc
        tc_cmds[machine].append(f"sudo tc qdisc del dev {iface} root || true")
        # Create prio root with two bands
        # tc_cmds[machine].append(f"sudo tc qdisc add dev {iface} root handle 1: prio bands 4")
        tc_cmds[machine].append(f"sudo tc qdisc add dev {iface} root handle 1: prio bands 3")
        # Attach netem qdiscs for the two bands
        # tc_cmds[machine].append(f"sudo tc qdisc add dev {iface} parent 1:3 handle 30: netem delay {DELAY_INTRA_REGION}")
        # tc_cmds[machine].append(f"sudo tc qdisc add dev {iface} parent 1:4 handle 40: netem delay {DELAY_INTER_REGION}")
        tc_cmds[machine].append(f"sudo tc qdisc add dev {iface} parent 1:3 handle 30: netem delay {DELAY_INTER_REGION}")

        # Add the filters
        for other_machine, other_region in machines_to_regions.items():
            if other_machine == machine or other_region == region:
                continue
            # flowid = "1:3" if region == other_region else "1:4"
            flowid = "1:3"
            tc_cmds[machine].append(
                f"sudo tc filter add dev {iface} protocol ip parent 1:0 prio 1 "
                f"u32 match ip dst {other_machine}/32 flowid {flowid}"
            )
        
    # Execute the commands
    for machine, cmds in tc_cmds.items():
        if not cmds:
            continue
        ssh_cmd = f"ssh {user}@{machine} '" + "; ".join(cmds) + "'"
        run_subprocess(ssh_cmd, dry_run)
    print("\n✅ Done: All delays applied.")


# === Remove all delays, both inter-region and intra-region ===
def remove_all_delays(user, interfaces):
    for machine, iface in interfaces.items():
        if not iface:
            print(f"[WARNING] No interface for {machine}, skipping.")
            continue
        ssh_cmd = f"ssh {user}@{machine} 'sudo tc qdisc del dev {iface} root || true'"
        run_subprocess(ssh_cmd, dry_run)
    print("\n✅ Done: All delays removed.")