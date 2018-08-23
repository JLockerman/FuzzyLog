#!/usr/local/bin/python

import os
import subprocess
import sys
import time

server_ip = '172.31.5.245:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.9.164:13289'
# server_ip = '172.31.5.245:13289#172.31.3.165:13289'
# server_ip = '172.31.6.77:13289#172.31.9.164:13289'
client_hostnames = ['ec2-52-207-225-244.compute-1.amazonaws.com', 'ec2-54-86-151-248.compute-1.amazonaws.com', 'ec2-54-172-94-178.compute-1.amazonaws.com', 'ec2-54-89-191-5.compute-1.amazonaws.com', 'ec2-54-175-235-70.compute-1.amazonaws.com', 'ec2-34-228-74-217.compute-1.amazonaws.com', 'ec2-18-232-149-155.compute-1.amazonaws.com', 'ec2-184-73-129-248.compute-1.amazonaws.com', 'ec2-54-84-172-42.compute-1.amazonaws.com', 'ec2-54-85-153-64.compute-1.amazonaws.com', 'ec2-54-166-208-238.compute-1.amazonaws.com', 'ec2-107-22-156-190.compute-1.amazonaws.com', 'ec2-52-204-151-151.compute-1.amazonaws.com', 'ec2-34-235-88-94.compute-1.amazonaws.com']

server_hostnames = 'ec2-34-229-215-103.compute-1.amazonaws.com,ec2-54-196-154-185.compute-1.amazonaws.com,ec2-18-232-182-64.compute-1.amazonaws.com,ec2-174-129-48-72.compute-1.amazonaws.com,ec2-54-209-214-144.compute-1.amazonaws.com,ec2-34-227-61-64.compute-1.amazonaws.com'

ips = '172.31.5.245#172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131#172.31.9.164'
# ips = '172.31.5.245#172.31.0.84#172.31.3.165'
# ips = '172.31.6.77#172.31.4.131#172.31.9.164'


num_handles = len(client_hostnames)

os.chdir('../../..')

for i in [0]: #range(0, 14): #range(0, 14, 2): #[5, 0, 2]: #range(0, 6):
    print("run " + str(i))

    # command = "run_chain:chain_hosts=" + ips + ",trace=fuzzy_log"
    command = "run_chain:chain_hosts=" + ips + ",nt=t"#",trace=fuzzy_log_server"#,debug=t"#,stats=t"
    print(command)
    p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, command])


    ########################################


    command = 'mirror:pkill zk_test; cd examples/zookeeper/tester/ && RUST_BACKTRACE\=short RUSTFLAGS\="-C target-cpu\=native" cargo run --release --' + \
    ' ' + server_ip + \
    ' #server_num' + \
    ' ' + str(num_handles - i) + \
    ' ' + str(100)
    print(command)

    client_hoststring = ""
    for name in client_hostnames[i:-1]:
        client_hoststring += name + ","
    client_hoststring += client_hostnames[-1]

    time.sleep(5)

    p1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", client_hoststring,
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

p0 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames, "kill_server"])
