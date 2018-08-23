#!/usr/local/bin/python2.7

import subprocess
import sys

server_ips = '172.31.5.104:13289#172.31.14.200:13289^172.31.6.234:13289#172.31.3.204:13289'
sequncer_ip = '172.31.9.12:13289'
hostname = 'ec2-54-144-206-162.compute-1.amazonaws.com'

command = 'mirror:cd ./benchers/append_lat/ && cargo run --release -- ' + server_ips + ' ' + sequncer_ip

p1 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", hostname, "mirror:rustup install 1.19.0 && rustup override set 1.19.0", command])

p1.wait()
