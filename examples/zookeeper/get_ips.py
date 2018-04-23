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
num_clients = len(public_dns) - 8

all_hosts = ""
for host in public_dns:
    all_hosts = all_hosts + host + ","

print(all_hosts)

print(public_dns)
print(len(public_dns))
print("")
print("")
print("Clients")
print(public_dns[0:num_clients])
print("")
print("Servers")
print(public_dns[num_clients:])
server_names = ""
print(private_ip[num_clients:])
for name in public_dns[num_clients:]:
    server_names = server_names + name + ","
print(server_names)
print("")
print("")
print("Client Hosts")
client_hosts = ""
for client in public_dns[0:num_clients]:
    client_hosts = client_hosts + client + ","
print(client_hosts)
server_ips = ""
for i in range(0, len(private_ip[num_clients:]), 3):
    server_ips = server_ips + private_ip[num_clients + i] + ":13289#" + private_ip[num_clients + i + 1] + ":13289#" + private_ip[num_clients + i + 2] + ":13289^"
print(server_ips)

