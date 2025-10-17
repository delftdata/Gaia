import subprocess as sp
import time

def apply_netem(delay="100ms", jitter="10ms", loss="0%", ips=None, user=None):
    """
    Applies network emulation (netem) settings on the given interface, locally or over SSH.
    :param delay: Base delay (e.g., "100ms").
    :param jitter: Jitter value (e.g., "20ms").
    :param loss: Packet loss percentage (e.g., "0%").
    :param ips: Dict of IP addresses and their interfaces to apply netem remotely.
    :param user: SSH username to use for remote access.
    """
    if ips:
        print("Applying netem on remote machines.")
        for ip in list(ips.keys()):
            netem_cmd = f"sudo tc qdisc add dev {ips[ip]} root netem delay {delay} {jitter} loss {loss}"
            ssh_target = f"{user}@{ip}" if user else ip
            print(f"Applying netem to {ssh_target} with command: {netem_cmd}")
            ssh_cmd = f"ssh {ssh_target} '{netem_cmd}'"
            result = sp.run(ssh_cmd, shell=True)
            if result.returncode != 0:
                print(f"⚠️ Failed to apply netem on {ip}")
            else:
                print(f"✅ Netem applied on {ip}")
    else:
        interface = sp.run('iftop 2>&1', shell=True, capture_output=True, text=True).stdout.split('\n')[0].split('interface: ')[1]
        print(f"Applying netem locally on {interface}...")
        netem_cmd = [
                "sudo", "tc", "qdisc", "add", "dev", interface, "root",
                "netem", "delay", delay, jitter, "loss", loss
            ]
        sp.run(netem_cmd, check=True)
        print(f"✅ Netem applied locally with delay={delay}, jitter={jitter}, loss={loss}")

def remove_netem(ips=None, user=None):
    """
    Removes netem settings from the given network interface, locally or via SSH.
    :param ips: Dict of IP addresses and their interfaces to remove netem remotely.
    :param user: SSH username to use for remote access.
    """
    if ips:
        print("Removing netem on remote machines.")
        for ip in ips:
            netem_cmd = f"sudo tc qdisc del dev {ips[ip]} root"
            ssh_target = f"{user}@{ip}" if user else ip
            print(f"Removing netem from {ssh_target}...")
            ssh_cmd = f"ssh {ssh_target} '{netem_cmd}'"
            result = sp.run(ssh_cmd, shell=True)
            if result.returncode != 0:
                print(f"⚠️ Failed to remove netem on {ip}")
            else:
                print(f"✅ Netem removed from {ip}")
    else:
        interface = sp.run('iftop 2>&1', shell=True, capture_output=True, text=True).stdout.split('\n')[0].split('interface: ')[1]
        print(f"Removing current netem configuration on local machine on interface {interface}...")
        netem_cmd = f"sudo tc qdisc del dev {interface} root"
        result = sp.run(netem_cmd.split(), check=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Netem removed locally from {interface}")
        else:
            print(f"⚠️ Failed to remove netem on {interface}")

def netem_status(ips=None, user=None):
    """
    Checks current netem settings from the given network interface, locally or via SSH.
    :param ips: Dict of IP addresses and their interfaces to check netem settings remotely.
    :param user: SSH username to use for remote access.
    """
    status_outputs = []
    if ips:
        print("Checking current netem status on remote machines...")
        for ip in list(ips.keys()):
            netem_cmd = f"tc qdisc show dev {ips[ip]}"
            ssh_target = f"{user}@{ip}" if user else ip
            ssh_cmd = f"ssh {ssh_target} '{netem_cmd}'"
            result = sp.run(ssh_cmd, shell=True, capture_output=True, text=True)
            print(f"Netem status at {ip}: {result.stdout}")
            status_outputs.append(result.stdout)
    else:
        interface = sp.run('iftop 2>&1', shell=True, capture_output=True, text=True).stdout.split('\n')[0].split('interface: ')[1]
        print(f"Checking current netem status on local machine {interface}...")
        netem_cmd = f"tc qdisc show dev {interface}"
        result = sp.run(netem_cmd.split(), check=True, capture_output=True, text=True)
        print(f"Netem status at on local node: {result.stdout}")
        status_outputs.append(result.stdout)
    return status_outputs
