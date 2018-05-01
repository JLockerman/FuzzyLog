#!/usr/local/bin/python

import os
import subprocess
import sys
import time

def run(corfu, num_clients, handles_per_client, tx_in, window=750):

    data_server_ips = '172.31.13.138:13289#172.31.3.204:13289^172.31.9.12:13289#172.31.14.212:13289^172.31.11.45:13289#172.31.5.245:13289^172.31.0.84:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.4.131:13289'
    seq_ips = '172.31.9.164:13289'

    ds_ips = '172.31.13.138#172.31.3.204^172.31.9.12#172.31.14.212^172.31.11.45#172.31.5.245^172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131'
    s_ips = '172.31.9.164'

    data_hostnames = 'ec2-52-207-144-1.compute-1.amazonaws.com,ec2-34-207-116-198.compute-1.amazonaws.com,ec2-34-227-75-240.compute-1.amazonaws.com,ec2-34-234-95-38.compute-1.amazonaws.com,ec2-52-54-136-50.compute-1.amazonaws.com,ec2-34-228-14-122.compute-1.amazonaws.com,ec2-34-227-224-125.compute-1.amazonaws.com,ec2-54-88-64-195.compute-1.amazonaws.com,ec2-34-224-79-70.compute-1.amazonaws.com,ec2-54-89-161-227.compute-1.amazonaws.com'

    seq_hostnames = 'ec2-54-172-185-64.compute-1.amazonaws.com'

    client_hostnames = ['ec2-184-72-213-25.compute-1.amazonaws.com', 'ec2-35-172-227-169.compute-1.amazonaws.com', 'ec2-54-237-239-69.compute-1.amazonaws.com', 'ec2-54-227-174-2.compute-1.amazonaws.com', 'ec2-54-210-156-124.compute-1.amazonaws.com', 'ec2-34-224-78-12.compute-1.amazonaws.com', 'ec2-54-237-200-96.compute-1.amazonaws.com', 'ec2-34-204-13-172.compute-1.amazonaws.com', 'ec2-54-145-208-251.compute-1.amazonaws.com']

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

    if tx_in != 0:
        command += ' -t ' + str(tx_in)

    if corfu:
        command += ' -c'

    print(clients)
    print(command)

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

# run(False, 9, 2, 1)
# exit(0)

for corfu in [False, True]:
    if corfu:
        print("> corfu")
    else:
        print("> fuzzy")

    for tx_in in [0, 1000, 100, 10, 1]:
        if tx_in == 0:
            print("> 0%")
        else:
            print("> " + str(100.0/tx_in) + "%")

        for num_clients in range(1, 9):
            sys.stdout.flush()
            run(corfu, num_clients, 2, tx_in)

