#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

server_ip = '172.31.5.245:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.9.164:13289'
client_hostnames = ['ec2-54-227-184-208.compute-1.amazonaws.com', 'ec2-54-175-70-159.compute-1.amazonaws.com', 'ec2-34-236-151-197.compute-1.amazonaws.com', 'ec2-54-86-159-222.compute-1.amazonaws.com', 'ec2-54-160-30-178.compute-1.amazonaws.com', 'ec2-54-152-153-92.compute-1.amazonaws.com', 'ec2-52-90-18-216.compute-1.amazonaws.com', 'ec2-54-197-178-50.compute-1.amazonaws.com', 'ec2-107-20-129-89.compute-1.amazonaws.com', 'ec2-34-224-74-6.compute-1.amazonaws.com', 'ec2-54-172-117-204.compute-1.amazonaws.com', 'ec2-54-208-189-40.compute-1.amazonaws.com', 'ec2-52-73-170-3.compute-1.amazonaws.com', 'ec2-54-227-179-217.compute-1.amazonaws.com']

server_hostnames = 'ec2-54-175-35-177.compute-1.amazonaws.com,ec2-34-230-35-216.compute-1.amazonaws.com,ec2-34-228-19-158.compute-1.amazonaws.com,ec2-54-146-247-227.compute-1.amazonaws.com,ec2-52-91-218-31.compute-1.amazonaws.com,ec2-54-210-191-38.compute-1.amazonaws.com'
ips = '172.31.5.245#172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131#172.31.9.164'


num_handles = len(client_hostnames)

for i in [0]: #range(0, 12): #[5, 0, 2]: #range(0, 6):
    print("run " + str(i))

    # command = "run_chain:chain_hosts=" + ips + ",trace=fuzzy_log"
    command = "run_chain:chain_hosts=" + ips + ",nt=t,stats=t"#,debug=t"#,stats=t"
    print(command)
    os.chdir('../../..')
    p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, command])


    ########################################


    command = 'mirror:pkill zk_test; cd examples/zookeeper/tester/ && RUST_BACKTRACE\=1 cargo run --release --features "print_stats" --' + \
    ' ' + server_ip + \
    ' #server_num' + \
    ' ' + str(num_handles - i)
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
