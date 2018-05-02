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

        server_hostnames = 'ec2-34-228-14-122.compute-1.amazonaws.com,ec2-34-227-224-125.compute-1.amazonaws.com,ec2-54-88-64-195.compute-1.amazonaws.com,ec2-34-224-79-70.compute-1.amazonaws.com,ec2-54-89-161-227.compute-1.amazonaws.com,ec2-54-172-185-64.compute-1.amazonaws.com'

    else:
        server_ip = '172.31.5.245:13289#172.31.3.165:13289'

        ips = '172.31.5.245#172.31.0.84#172.31.3.165'

        server_hostnames = 'ec2-34-228-14-122.compute-1.amazonaws.com,ec2-34-227-224-125.compute-1.amazonaws.com,ec2-54-88-64-195.compute-1.amazonaws.com'


    client_hostnames = ['ec2-184-72-213-25.compute-1.amazonaws.com', 'ec2-35-172-227-169.compute-1.amazonaws.com', 'ec2-54-237-239-69.compute-1.amazonaws.com', 'ec2-54-227-174-2.compute-1.amazonaws.com', 'ec2-54-210-156-124.compute-1.amazonaws.com', 'ec2-34-224-78-12.compute-1.amazonaws.com', 'ec2-54-237-200-96.compute-1.amazonaws.com', 'ec2-34-204-13-172.compute-1.amazonaws.com', 'ec2-54-145-208-251.compute-1.amazonaws.com', 'ec2-52-207-144-1.compute-1.amazonaws.com', 'ec2-34-207-116-198.compute-1.amazonaws.com', 'ec2-34-227-75-240.compute-1.amazonaws.com', 'ec2-34-234-95-38.compute-1.amazonaws.com', 'ec2-52-54-136-50.compute-1.amazonaws.com']

    print(len(client_hostnames))

    clients = client_hostnames[0]
    for client in client_hostnames[1:]:
        clients += ',' + client

    for i in [12]:
        print("run " + str(i))

        command = "run_chain:chain_hosts=" + ips

        s = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H",
            server_hostnames, command])

        time.sleep(3)

        ########################################

        command = 'mirror:pkill zk_test; cd examples/zookeeper/tester/ &&' + \
            ' cargo run --release --' + \
            ' ' + server_ip + \
            ' #server_num' + \
            ' ' + str(i) + \
            ' ' + str(1000)

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


os.chdir('../../..')

run(10, True, False)
exit(0)

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
