#!/usr/local/bin/python2.7

import subprocess
import sys

server_dns = ['ec2-34-207-252-255.compute-1.amazonaws.com', 'ec2-107-23-178-46.compute-1.amazonaws.com', 'ec2-34-235-87-54.compute-1.amazonaws.com', 'ec2-54-198-98-220.compute-1.amazonaws.com']
server_ips = ['172.31.5.104', '172.31.14.200', '172.31.6.234', '172.31.3.204']
sequencer = 'ec2-34-235-154-206.compute-1.amazonaws.com'
seq_ip = '172.31.9.12'
procs = []

# server_dns = ['ec2-52-72-66-109.compute-1.amazonaws.com', 'ec2-34-227-206-53.compute-1.amazonaws.com', 'ec2-34-229-205-4.compute-1.amazonaws.com', 'ec2-54-208-101-237.compute-1.amazonaws.com']
# server_ips = ['172.31.5.104', '172.31.14.200', '172.31.6.234', '172.31.3.204']

hostnames = ""
ips = ""
for i in range(0, len(server_dns)-2, 2):
    hostnames += server_dns[i] + "," + server_dns[i+1] + ","
    ips += server_ips[i] + "#" + server_ips[i+1] + "^"

hostnames += server_dns[len(server_dns)-2] + "," + server_dns[len(server_dns)-1]
ips += server_ips[len(server_dns)-2] + "#" + server_ips[len(server_dns)-1]

command = "run_chain:chain_hosts=" + ips + ",nt=t"
seq_command  = "run_chain:chain_hosts=" + seq_ip + ",nt=t"
print(command)
p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", hostnames, "mirror:rustup install 1.19.0 && rustup override set 1.19.0", command])
p1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", sequencer, "mirror:rustup install 1.19.0 && rustup override set 1.19.0", seq_command])
p0.wait()
p1.wait()
