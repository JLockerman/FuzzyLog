#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

server_ip = '172.31.5.245:13289'

write_host = 'ec2-54-84-203-95.compute-1.amazonaws.com'

client_hostnames = ['ec2-34-224-221-204.compute-1.amazonaws.com', 'ec2-54-208-107-64.compute-1.amazonaws.com', 'ec2-54-197-180-241.compute-1.amazonaws.com', 'ec2-34-207-153-105.compute-1.amazonaws.com', 'ec2-35-173-248-195.compute-1.amazonaws.com', 'ec2-54-197-174-177.compute-1.amazonaws.com', 'ec2-34-227-207-68.compute-1.amazonaws.com', 'ec2-34-229-167-26.compute-1.amazonaws.com', 'ec2-54-197-145-149.compute-1.amazonaws.com', 'ec2-34-233-119-214.compute-1.amazonaws.com', 'ec2-34-201-138-116.compute-1.amazonaws.com', 'ec2-54-242-125-228.compute-1.amazonaws.com']

server_hostnames = 'ec2-52-207-240-246.compute-1.amazonaws.com'

view_hosts = 'ec2-34-234-82-62.compute-1.amazonaws.com'

ips = '172.31.5.245'

socket = 13333
view_ip = '172.31.0.84:' + str(socket)


os.chdir('../..')

for i in range(13, 14):
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

    client_hoststring = ""
    for name in client_hostnames[i:-1]:
        client_hoststring += name + ","
    client_hoststring += client_hostnames[-1]



    command = 'mirror:pkill traffic_gen; cd examples/zookeeper/traffic_gen/ &&' +\
        ' cargo run --release --' +\
        ' read ' + str(i) + ' ' + view_ip + ' ' + str(1) + ' ' + str(1000)

    r = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", client_hoststring,
        command])

    r.wait()
    w.wait()
    s.kill()
    # p00.kill()
    # p01.kill()

    # subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, "kill_server"])

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
