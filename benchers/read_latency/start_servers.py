#!/usr/local/bin/python2.7

import subprocess
import sys

server_dns = ['ec2-107-20-87-53.compute-1.amazonaws.com', 'ec2-54-162-154-10.compute-1.amazonaws.com']
server_ips = ['172.31.15.138', '172.31.4.131']
procs = []

hostnames = ""
ips = ""

hostnames += server_dns[0] + "," + server_dns[1]
ips += server_ips[0] + "^" + server_ips[1]

command = "run_chain:chain_hosts=" + ips + ",nt=t"
print(command)
p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", hostnames, "mirror:rustup install 1.19.0 && rustup override set 1.19.0", command])

p0.wait()
