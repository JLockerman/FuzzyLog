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

    data_hostnames = 'ec2-34-201-119-82.compute-1.amazonaws.com,ec2-18-232-139-180.compute-1.amazonaws.com,ec2-54-165-208-168.compute-1.amazonaws.com,ec2-18-205-20-202.compute-1.amazonaws.com,ec2-52-90-172-135.compute-1.amazonaws.com,ec2-34-229-75-136.compute-1.amazonaws.com,ec2-35-172-219-199.compute-1.amazonaws.com,ec2-54-198-108-43.compute-1.amazonaws.com,ec2-52-90-95-50.compute-1.amazonaws.com,ec2-54-166-120-151.compute-1.amazonaws.com'

    seq_hostnames = 'ec2-34-224-64-30.compute-1.amazonaws.com'

    client_hostnames = ['ec2-34-224-58-29.compute-1.amazonaws.com', 'ec2-54-91-138-174.compute-1.amazonaws.com', 'ec2-54-90-149-215.compute-1.amazonaws.com', 'ec2-18-232-182-253.compute-1.amazonaws.com', 'ec2-34-234-74-251.compute-1.amazonaws.com', 'ec2-54-164-53-1.compute-1.amazonaws.com', 'ec2-34-228-199-156.compute-1.amazonaws.com', 'ec2-54-146-157-61.compute-1.amazonaws.com', 'ec2-54-89-232-100.compute-1.amazonaws.com']

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

# run(False, 1, 2, 1)
# exit(0)

# for num_clients in range(1, 9):
#     sys.stdout.flush()
#     run(False, num_clients, 2, 1)

# exit(0)

for corfu in [False]:
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

