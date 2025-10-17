import docker
import time
from datetime import datetime

# Initialize Docker client
client = docker.from_env()

# Define log file
log_file = "container_traffic_log.csv"

# Function to get current network statistics for all containers
def get_network_stats(retries=5, delay=1):
    """
    Fetches network statistics for all containers with retries to ensure data completeness.
    """
    for attempt in range(retries):
        stats = {}
        incomplete = False
        # Debug: List all containers
        containers = client.containers.list()
        print("Detected containers:", [c.name for c in containers])
        for container in containers:
            try:
                print(f"Fetching stats for container: {container.name}")
                container_stats = container.stats(stream=False)
                # Check for 'networks'
                networks = container_stats.get("networks", {})
                if not networks:
                    incomplete = True
                    print(f"Warning: 'networks' field missing for container {container.name}. Retrying...")
                    continue
                # Extract stats for each network interface
                for iface, iface_stats in networks.items():
                    stats[(container.name, iface)] = iface_stats.get("tx_bytes", 0)
            except Exception as e:
                print(f"Error fetching stats for container {container.name}: {e}")
                incomplete = True
        if not incomplete:
            return stats  # Return complete stats
        print(f"Retrying... ({attempt + 1}/{retries})")
        time.sleep(delay)
    print("Warning: Metrics incomplete after retries.")
    return stats  # Return the best available stats

# Function to log traffic between containers
def log_traffic():
    prev_stats = get_network_stats()

    with open(log_file, "w") as log:
        log.write("Time stamp,Origin,Destination,Bytes sent since last measurement\n")

    while True:
        time.sleep(5)  # Increase interval to 5 seconds
        timestamp = int(time.time())
        current_stats = get_network_stats()

        with open(log_file, "a") as log:
            # Debug: Log raw stats
            log.write(f"DEBUG: {timestamp} - {current_stats}\n")
            for (origin, iface), bytes_sent in current_stats.items():
                prev_bytes = prev_stats.get((origin, iface), 0)
                bytes_diff = bytes_sent - prev_bytes
                if bytes_diff > 0:
                    log.write(f"{timestamp},{origin},{iface},{bytes_diff}\n")

        prev_stats = current_stats

if __name__ == "__main__":
    print("Monitoring Docker container traffic... Logs will be saved in 'container_traffic_log.csv'")
    log_traffic()
