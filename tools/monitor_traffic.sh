#!/bin/bash

LOG_FILE="container_traffic.log"

# Clear previous iptables rules
sudo iptables -Z

# Initialize logging for all containers
for container in $(docker ps -q); do
    # Get container name and IP address
    container_name=$(docker inspect -f '{{.Name}}' "$container" | sed 's#/##')
    container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container")
    # Add iptables rule to track egress traffic for the container's IP
    sudo iptables -A OUTPUT -s "$container_ip" -j ACCEPT
    # Log initialization
    echo "$(date +%s) INIT $container_name ($container_ip)" >> "$LOG_FILE"
done

# Monitor traffic continuously
while true; do
    sleep 5  # Measurement interval in seconds

    for container in $(docker ps -q); do
        # Get container name and IP address
        container_name=$(docker inspect -f '{{.Name}}' "$container" | sed 's#/##')
        container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container")

        # Get total bytes sent by this container since the last reset
        bytes_sent=$(sudo iptables -L OUTPUT -v -x -n | grep "$container_ip" | awk '{print $2}')

        # Log the traffic
        echo "$(date +%s) $container_name ($container_ip) SENT $bytes_sent" >> "$LOG_FILE"
    done
done
