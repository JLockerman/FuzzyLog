#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

def run(corfu, num_clients, handles_per_client, window=1000):

    data_server_ips = '172.31.9.12:13289#172.31.14.212:13289^172.31.11.45:13289#172.31.5.245:13289^172.31.0.84:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.4.131:13289'
    seq_ips = '172.31.9.164:13289'

    ds_ips = '172.31.9.12#172.31.14.212^172.31.11.45#172.31.5.245^172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131'
    s_ips = '172.31.9.164'

    data_hostnames = 'ec2-52-206-35-77.compute-1.amazonaws.com,ec2-34-234-70-77.compute-1.amazonaws.com,ec2-35-172-221-43.compute-1.amazonaws.com,ec2-18-205-233-67.compute-1.amazonaws.com,ec2-34-229-91-164.compute-1.amazonaws.com,ec2-107-21-29-228.compute-1.amazonaws.com,ec2-34-203-198-125.compute-1.amazonaws.com,ec2-34-224-84-75.compute-1.amazonaws.com'

    seq_hostnames = 'ec2-34-207-59-108.compute-1.amazonaws.com'

    client_hostnames = ['ec2-34-227-190-147.compute-1.amazonaws.com', 'ec2-54-86-139-102.compute-1.amazonaws.com', 'ec2-34-229-14-104.compute-1.amazonaws.com', 'ec2-54-210-95-82.compute-1.amazonaws.com', 'ec2-54-146-157-99.compute-1.amazonaws.com', 'ec2-54-89-158-9.compute-1.amazonaws.com', 'ec2-54-172-183-98.compute-1.amazonaws.com', 'ec2-52-87-175-153.compute-1.amazonaws.com', 'ec2-52-87-172-9.compute-1.amazonaws.com', 'ec2-34-201-63-15.compute-1.amazonaws.com', 'ec2-54-152-161-181.compute-1.amazonaws.com']

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

# run(False, 11, 2)
# exit(0)

for corfu in [False, True]:

    for num_clients in range(1, 11):
        sys.stdout.flush()
        run(corfu, num_clients, 2)

