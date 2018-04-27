#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

def run(experiment, num_chains, nodes_per_chain):

    server_ip = '172.31.5.245:13289'
    ips = '172.31.5.245'
    server_hostnames = 'ec2-54-235-33-196.compute-1.amazonaws.com'

    client_hostnames = ['ec2-54-87-195-25.compute-1.amazonaws.com']

    clients = client_hostnames[0]

    command = "run_chain:chain_hosts=" + ips

    s = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H",
        server_hostnames, command])

    time.sleep(2)

    ########################################

    command = 'mirror:pkill latben; cd benchers/read_latency/ &&' + \
        ' cargo run --release --' + \
        ' ' + str(server_ip) + \
        ' ' + experiment + \
        ' ' + str(num_chains) + \
        ' ' + str(nodes_per_chain)

    v = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", clients,
        command])

    v.wait()
    s.kill()

    print("")
    print("> ")
    print("========================================")
    print("> ----------------------------------------")
    print("========================================")
    print("> ")
    print("")
    sys.stdout.flush()

    subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, "kill_server"])
    # subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", clients, 'mirror:pkill latben'])


os.chdir('../..')

# run("no_link", 1, 1)
# exit(0)

for experiment in ["zigzag"]: #["no_link", "zigzag"]:
    # print("> " + experiment)


    for num_chains in [1, 2, 10, 100]:
        # print(str(num_chains) + " chains")

        for nodes_per_chain in [1, 10, 100, 1000]:
            sys.stdout.flush()
            run(experiment, num_chains, nodes_per_chain)
