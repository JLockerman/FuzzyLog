#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

server_ip = '172.31.5.245:13289#172.31.3.165:13289'
# server_ip = '172.31.5.245:13289'

server_ips = [
    '',
    '172.31.5.245:13289',
    '172.31.5.245:13289#172.31.3.165:13289',
    '172.31.5.245:13289#172.31.3.165:13289',
]

client_hostnames = ['ec2-54-91-87-24.compute-1.amazonaws.com', 'ec2-34-227-191-225.compute-1.amazonaws.com', 'ec2-54-208-118-55.compute-1.amazonaws.com', 'ec2-18-232-136-244.compute-1.amazonaws.com', 'ec2-52-23-169-156.compute-1.amazonaws.com', 'ec2-107-20-27-163.compute-1.amazonaws.com', 'ec2-54-198-222-245.compute-1.amazonaws.com', 'ec2-18-204-228-102.compute-1.amazonaws.com', 'ec2-54-172-173-203.compute-1.amazonaws.com', 'ec2-34-233-121-116.compute-1.amazonaws.com', 'ec2-34-224-213-245.compute-1.amazonaws.com', 'ec2-54-89-137-73.compute-1.amazonaws.com', 'ec2-54-208-183-23.compute-1.amazonaws.com', 'ec2-34-224-212-223.compute-1.amazonaws.com']

servers = ['ec2-52-55-100-37.compute-1.amazonaws.com', 'ec2-52-90-95-137.compute-1.amazonaws.com', 'ec2-34-224-39-39.compute-1.amazonaws.com']
servers = [
    '',
    'ec2-52-55-100-37.compute-1.amazonaws.com',
    'ec2-52-55-100-37.compute-1.amazonaws.com,ec2-34-224-39-39.compute-1.amazonaws.com',
    'ec2-52-55-100-37.compute-1.amazonaws.com,ec2-52-90-95-137.compute-1.amazonaws.com,ec2-34-224-39-39.compute-1.amazonaws.com',
]
# server_hostnames = 'ec2-52-55-100-37.compute-1.amazonaws.com,ec2-34-224-39-39.compute-1.amazonaws.com'
# server_hostnames = 'ec2-34-224-39-39.compute-1.amazonaws.com'

ips = [
    '',
    '172.31.5.245',
    '172.31.5.245#172.31.3.165',
    '172.31.5.245#172.31.0.84#172.31.3.165'
]

num_servers = 1

os.chdir('../..')

command = "run_chain:chain_hosts=" + ips[num_servers] + ",nt=t"
print(command)
p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", servers[num_servers],
    command])

time.sleep(2)

command = 'mirror:pkill lat_v_even; cd benchers/append_lat2/ && RUST_BACKTRACE\=short cargo run --release -- ' + server_ips[num_servers]
p1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", client_hostnames[0],
    command])

p1.wait()

p0.kill()

print("")
print("> ")
print("========================================")
print("> ----------------------------------------")
print("========================================")
print("> ")
print("")
sys.stdout.flush()

kill_hoststring = servers[0]
for name in servers[1:len(servers)]:
    kill_hoststring += "," + name

p0 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", kill_hoststring, "kill_server"])
