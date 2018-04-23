#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

server_ip = '172.31.5.245:13289'

write_host = 'ec2-34-224-8-117.compute-1.amazonaws.com'

client_hostnames = ['ec2-35-173-128-2.compute-1.amazonaws.com', 'ec2-54-162-160-189.compute-1.amazonaws.com', 'ec2-34-224-174-112.compute-1.amazonaws.com', 'ec2-34-204-11-226.compute-1.amazonaws.com', 'ec2-34-227-221-75.compute-1.amazonaws.com', 'ec2-54-242-37-180.compute-1.amazonaws.com', 'ec2-54-166-140-78.compute-1.amazonaws.com', 'ec2-54-89-121-116.compute-1.amazonaws.com', 'ec2-35-174-6-11.compute-1.amazonaws.com', 'ec2-34-224-169-105.compute-1.amazonaws.com', 'ec2-52-91-159-228.compute-1.amazonaws.com']

server_hostnames = 'ec2-54-196-152-202.compute-1.amazonaws.com'

view_hosts = 'ec2-35-172-212-149.compute-1.amazonaws.com,ec2-18-205-25-178.compute-1.amazonaws.com,ec2-35-172-213-113.compute-1.amazonaws.com,ec2-35-172-212-105.compute-1.amazonaws.com,ec2-34-229-206-166.compute-1.amazonaws.com,ec2-54-175-209-245.compute-1.amazonaws.com,ec2-54-165-209-245.compute-1.amazonaws.com'

ips = '172.31.5.245'

socket = 13333
view_ip = ['172.31.14.212', '172.31.11.45', '172.31.0.84', '172.31.3.165', '172.31.6.77', '172.31.4.131', '172.31.9.164']


os.chdir('../..')

print(len(client_hostnames))

for i in range(1, 8):
    print("run " + str(i))

    command = "run_chain:chain_hosts=" + ips

    s = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H",
        server_hostnames, command])

    time.sleep(1)

    ########################################

    #FIXME view list
    command = 'mirror:pkill zk_view; cd examples/zookeeper/view/ &&' + \
        ' cargo run --release --' + \
        ' ' + str(server_ip) + \
        ' ' + str(socket) + \
        ' ' + str(i) + \
        ' ' + str(500)

    v = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", view_hosts,
        command])

    time.sleep(1)

    ########################################

    command = 'mirror:pkill traffic_gen; cd examples/zookeeper/traffic_gen/ &&' +\
        ' cargo run --release --' +\
        ' write ' + server_ip

    w = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", write_host,
        command])

    ########################################

    rs = []
    num = 0
    print(client_hostnames[:i])
    for client in client_hostnames[:i]:
        command = 'mirror:pkill traffic_gen; cd examples/zookeeper/traffic_gen/ &&' +\
            ' cargo run --release --' +\
            ' read ' + str(num) + ' ' + view_ip[num] + ":" + str(socket) + ' ' + str(3) + ' ' + str(500)
        num += 1

        r = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", client,
            command])
        rs.append(r)

    for r in rs:
        r.wait()

    w.wait()
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

subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", view_hosts,
        'mirror:pkill zk_view'])
