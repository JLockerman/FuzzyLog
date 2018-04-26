#!/usr/local/bin/python2.7
import boto3

ec2client = boto3.client('ec2')
response = ec2client.describe_instances()
public_dns = []
private_ip = []
clients = []
servers = []
for reservation in response["Reservations"]:
    for instance in reservation["Instances"]:
        if instance[u'PublicDnsName'] != "":
            public_dns.append(instance[u'PublicDnsName'])
            private_ip.append(instance[u'PrivateIpAddress'])

num_instances = len(private_ip)
print(num_instances)
num_clients = len(public_dns) - 6
num_views = 8

all_hosts = ""
for host in public_dns:
    all_hosts = all_hosts + host + ","

print(all_hosts)

print(public_dns)
print(len(public_dns))
print("")
print("")

print("clients")
print(public_dns[0:num_clients])
print(private_ip[0:num_views])
view_names = private_ip[0] + ":13333"
for name in private_ip[1:num_views]:
    view_names += "^" + name + ":13333"
print(view_names)
print("")

print("Views")

print("")

print("Servers")
print(public_dns[num_clients:])
server_names = ""
print(private_ip[num_clients:])
for name in public_dns[num_clients:]:
    server_names = server_names + name + ","
print(server_names)

