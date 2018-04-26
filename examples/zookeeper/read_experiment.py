#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

def run(transaction_in=0, two_replica_sets=True, one_chain=False):
    server_ip = None
    server_hostnames = None
    ips = None
    if two_replica_sets:
        server_ip = '172.31.5.245:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.9.164:13289'

        ips = '172.31.5.245#172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131#172.31.9.164'

        server_hostnames = 'ec2-34-227-111-91.compute-1.amazonaws.com,ec2-184-72-88-37.compute-1.amazonaws.com,ec2-34-201-150-244.compute-1.amazonaws.com,ec2-54-210-157-90.compute-1.amazonaws.com,ec2-52-91-171-122.compute-1.amazonaws.com,ec2-34-228-141-119.compute-1.amazonaws.com'

    else:
        server_ip = '172.31.5.245:13289#172.31.3.165:13289'

        ips = '172.31.5.245#172.31.0.84#172.31.3.165'

        server_hostnames = 'ec2-34-227-111-91.compute-1.amazonaws.com,ec2-184-72-88-37.compute-1.amazonaws.com,ec2-34-201-150-244.compute-1.amazonaws.com'


    client_hostnames = ['ec2-34-234-97-84.compute-1.amazonaws.com', 'ec2-54-85-136-80.compute-1.amazonaws.com', 'ec2-34-228-144-48.compute-1.amazonaws.com', 'ec2-54-158-3-255.compute-1.amazonaws.com', 'ec2-54-205-228-200.compute-1.amazonaws.com', 'ec2-34-203-34-129.compute-1.amazonaws.com', 'ec2-54-91-120-53.compute-1.amazonaws.com', 'ec2-34-235-129-19.compute-1.amazonaws.com', 'ec2-54-235-35-175.compute-1.amazonaws.com', 'ec2-54-89-197-247.compute-1.amazonaws.com', 'ec2-18-204-225-178.compute-1.amazonaws.com', 'ec2-34-204-43-117.compute-1.amazonaws.com', 'ec2-107-20-112-1.compute-1.amazonaws.com', 'ec2-52-91-93-200.compute-1.amazonaws.com']

    socket = 13333
    view_ip = '172.31.2.223:13333^172.31.15.138:13333^172.31.2.64:13333^172.31.10.38:13333^172.31.2.161:13333^172.31.5.104:13333^172.31.14.200:13333^172.31.12.187:13333'

    print(len(client_hostnames))

    clients = client_hostnames[0]
    for client in client_hostnames[1:]:
        clients += ',' + client

    for i in [8]:
        print("run " + str(i))

        command = "run_chain:chain_hosts=" + ips

        s = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H",
            server_hostnames, command])

        time.sleep(3)

        ########################################

        command = 'mirror:pkill zkv; cd examples/zookeeper/view2/ &&' + \
            ' RUST_BACKTRACE\=short cargo run --release --' + \
            ' ' + str(server_ip) + \
            ' ' + str(socket) + \
            ' #server_num' + \
            ' ' + str(i) + \
            ' ' + str(1000) + \
            ' ' + str(view_ip) + \
            ' ' + str(2)

        if transaction_in != 0:
            command += ' -t ' + str(transaction_in)

        if one_chain:
            command += ' -o '

        v = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", clients,
            command])

        # time.sleep(1)

        ########################################

        # command = 'mirror:pkill traffic_gen; cd examples/zookeeper/traffic_gen/ &&' +\
        #     ' cargo run --release --' +\
        #     ' write ' + server_ip

        #w = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", write_host,
        #    command])

        ########################################

        # rs = []
        # num = 0
        # print(client_hostnames[:i])
        # for client in client_hostnames[:i]:
        #     command = 'mirror:pkill traffic_gen; cd examples/zookeeper/traffic_gen/ &&' +\
        #         ' cargo run --release --' +\
        #         ' read ' + str(num) + ' ' + view_ip[num] + ":" + str(socket) + ' ' + str(3) + ' ' + str(500)
        #     num += 1

        #     r = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", client,
        #         command])
        #     rs.append(r)

        # for r in rs:
        #     r.wait()

        #w.wait()
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

    subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", clients,
            'mirror:pkill zkv'])


os.chdir('../..')

# run(0, True, False)
# exit(0)

for one_chain in [False, True]:
    if one_chain:
        print("> color per server")
    else:
        print("> color per view")

    for two_replica_sets in [True, False]:

        if two_replica_sets:
            print("> 2 replica sets")
        else:
            print("> 1 replica sets")

        for transaction_in in [0, 1000, 100, 10, 1]:
            if transaction_in == 0:
                print("> 0%")
            else:
                print("> " + str(100.0 / transaction_in) + "%")

            sys.stdout.flush()
            run(transaction_in, two_replica_sets, one_chain)
