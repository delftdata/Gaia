import pty
import os
import time
import subprocess

# Command to run iftop
cmd = ['sudo', 'iftop', '-t', '-B', "-i", "enX0"]

out_file = 'org_out.log'

# Create a pseudo-terminal
master, slave = pty.openpty()

# Run the command with the slave end of the pseudo-terminal as its stdout
popen = subprocess.Popen(cmd, stdout=slave, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True, close_fds=True)

# Close the slave end in the parent process
os.close(slave)

with open(out_file, "a") as f:
    with os.fdopen(master, 'r') as stdout:
        for line in stdout:
            f.write(str(time.time()).split('.')[0] + ':' + line)
            #print(str(time.time()).split('.')[0], ':', line.rstrip())    

popen.wait()
