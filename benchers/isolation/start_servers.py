#!/usr/local/bin/python2.7

import subprocess
import sys

server_dns = ['ec2-54-144-204-176.compute-1.amazonaws.com', 'ec2-34-228-144-254.compute-1.amazonaws.com', 'ec2-107-21-20-209.compute-1.amazonaws.com', 'ec2-54-208-77-113.compute-1.amazonaws.com']
server_ips = ['172.31.15.138', '172.31.2.64', '172.31.6.234', '172.31.4.131']
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
print(command)
p = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", hostnames, "mirror:rustup install 1.19.0 && rustup override set 1.19.0", command])
p.wait()
