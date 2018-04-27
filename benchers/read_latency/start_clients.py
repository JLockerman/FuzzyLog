#!/usr/local/bin/python2.7

import subprocess
import sys

server_ips = '172.31.15.138:13289^172.31.4.131:13289'
hostname = 'ec2-54-86-154-231.compute-1.amazonaws.com'

command = 'mirror:cd ./benchers/backpointer/ && cargo run --release -- ' + server_ips

p1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", hostname, "mirror:pkill back; rustup install 1.19.0 && rustup override set 1.19.0", command])

p1.wait()
