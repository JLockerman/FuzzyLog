#!/usr/local/bin/python2.7

import subprocess
import sys

server_ips = '172.31.15.138:13289#172.31.2.64:13289^172.31.6.234:13289#172.31.4.131:13289'
hostnames = 'ec2-34-229-147-107.compute-1.amazonaws.com'

command = 'mirror:cd ./benchers/isolation/ && cargo run --release -- -p 100 ' + server_ips

p1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", hostnames, "mirror:rustup install 1.19.0 && rustup override set 1.19.0", command])

p1.wait()
