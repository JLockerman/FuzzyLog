#!/usr/local/bin/python

import os
import subprocess
import sys
import time

def run(corfu, num_clients, handles_per_client, window=750):

    data_server_ips = '172.31.13.138:13289#172.31.3.204:13289^172.31.9.12:13289#172.31.14.212:13289^172.31.11.45:13289#172.31.5.245:13289^172.31.0.84:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.4.131:13289'
    seq_ips = '172.31.9.164:13289'

    ds_ips = '172.31.13.138#172.31.3.204^172.31.9.12#172.31.14.212^172.31.11.45#172.31.5.245^172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131'
    s_ips = '172.31.9.164'

    data_hostnames = 'ec2-34-207-242-140.compute-1.amazonaws.com,ec2-34-204-49-149.compute-1.amazonaws.com,ec2-34-207-145-229.compute-1.amazonaws.com,ec2-18-204-225-95.compute-1.amazonaws.com,ec2-54-174-87-12.compute-1.amazonaws.com,ec2-174-129-47-52.compute-1.amazonaws.com,ec2-34-201-69-2.compute-1.amazonaws.com,ec2-52-207-187-127.compute-1.amazonaws.com,ec2-52-201-238-167.compute-1.amazonaws.com,ec2-52-54-159-81.compute-1.amazonaws.com'

    seq_hostnames = 'ec2-54-236-6-240.compute-1.amazonaws.com'

    client_hostnames = ['ec2-52-207-241-251.compute-1.amazonaws.com', 'ec2-54-152-77-38.compute-1.amazonaws.com', 'ec2-54-198-108-116.compute-1.amazonaws.com', 'ec2-52-87-218-45.compute-1.amazonaws.com', 'ec2-34-230-35-84.compute-1.amazonaws.com', 'ec2-52-91-102-90.compute-1.amazonaws.com', 'ec2-54-227-107-134.compute-1.amazonaws.com', 'ec2-34-229-156-220.compute-1.amazonaws.com', 'ec2-52-90-224-18.compute-1.amazonaws.com']

    print(len(client_hostnames))

    client_hostnames = client_hostnames[:num_clients]
    num_clients = len(client_hostnames)

    clients = client_hostnames[0]
    for name in client_hostnames[1:]:
        clients += ',' + name


    command = "run_chain:chain_hosts=" + ds_ips
    d = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H",
        data_hostnames, command])

    command = "run_chain:chain_hosts=" + s_ips
    s = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H",
        seq_hostnames, command])

    time.sleep(5)

    ########################################

    command = 'mirror:pkill scalben; cd benchers/scaling/ &&' + \
        ' cargo run --release --' + \
        ' ' + str(data_server_ips) + \
        ' ' + str(seq_ips) + \
        ' #server_num' + \
        ' -h ' + str(handles_per_client) +\
        ' -w ' + str(window)

    if corfu:
        command += ' -c'

    v = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", clients,
        command])

    v.wait()
    s.kill()
    d.kill()

    print("")
    print("> ")
    print("========================================")
    print("> ----------------------------------------")
    print("========================================")
    print("> ")
    print("")
    sys.stdout.flush()

    k0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", data_hostnames, "kill_server"])
    k1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", seq_hostnames, "kill_server"])
    k2 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", data_hostnames, "try_ex:pkill scalben"])
    k0.wait()
    k1.wait()
    k2.wait()


os.chdir('../..')

# run(False, 9, 2)
# exit(0)

for corfu in [False, True]:

    for num_clients in range(1, 9):
        sys.stdout.flush()
        run(corfu, num_clients, 2)

