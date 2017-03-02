from fabric.api import run, env, local, run, parallel, cd, settings, hide, sudo
from fabric.contrib.project import rsync_project
from fabric import network
import os

# This is the default username for EC2 ubuntu instances
# since most of the lab is using those, it is set to the default user
env.user = 'ubuntu'
# Similarly, known_hosts is mostly harmful in this configuration
env.disable_known_hosts = True

@parallel
def setup():
    #install rust
    run('sudo DEBIAN_FRONTEND=noninteractive apt-get -y update')
    run('sudo DEBIAN_FRONTEND=noninteractive apt-get -y install gcc')
    run('curl https://sh.rustup.rs -sSf | sh -s -- -y', pty=False)
    run('export PATH=$HOME/.cargo/bin:$PATH')
    #run('ulimit -n 2048')

@parallel
def sync():
    #local("rsync --filter=':- .gitignore' -P -avz ./ " + env.host_string + ":~/" + dirname)
    rsync_project(remote_dir="~/", exclude=[".*",".*/"], extra_opts="--filter=':- .gitignore'")

@parallel
def mirror(cmd=''):
    sync()
    fuzzy_log_dir = os.path.basename(os.getcwd())
    with cd(fuzzy_log_dir):
        run(cmd, pty=False)

# NOTE connecting to AWS requires use of  the public IP addr
#      unfortunately communication within AWS requires the use of the private IP addr by default
#      so you need to input the server addrs twice once comma separated for -H
#      once '^' separated for chain_hosts
@parallel
def run_chain(chain_hosts, port="13289", trace="", workers="", debug="", stats=""):
    with settings(hide('warnings'), warn_only=True):
        run("pkill tcp_server")
    host_index = index_of_host()
    cmd = "cd servers/tcp_server && "
    if trace != "":
        cmd += "RUST_LOG=" + trace + " "
    cmd += "RUST_BACKTRACE=1 cargo run "
    if debug == "":
        cmd += "--release "
    if stats != "":
        cmd += "--features \"print_stats fuzzy_log/debug_no_drop\" "
    cmd += "-- " + port

    chain_hosts = chain_hosts.split("^")
    if host_index > 0:
        #prev_host = network.normalize(env.all_hosts[host_index-1])[1]
        prev_host = chain_hosts[host_index-1]
        cmd += " -up " + prev_host + ":" + port
    if host_index + 1 < len(env.all_hosts):
        #next_host = network.normalize(env.all_hosts[host_index+1])[1]
        next_host = chain_hosts[host_index+1]
        cmd += " -dwn " + next_host
    if workers != "":
        cmd += " -w " + workers
    mirror(cmd)

@parallel
def kill_server():
    with settings(hide('warnings'), warn_only=True):
        run("pkill tcp_server")

# servers should be of the form <server addr>(^<server addr>)*
# (yes '^' is being used as the separator)
# or of the form <chain head addr>-<chain tail addr>(^<chain head addr>-<chain tail addr>)*
# where <server addr> is of the form <server ip>:<port>
@parallel
def run_clients(num_clients, servers, jobsize="1000", num_writes="100000", trace="", debug="",stats=""):
    with settings(warn_only=True):
        run('pkill fuzzy_log_throughput_bench')
    cmd = "cd benchers/throughput && "
    if trace != "":
        cmd += "RUST_LOG=" + trace + " "
    if debug == "":
        cmd += "cargo run --release "
    else:
        cmd += "cargo run "
    if trace != "":
        cmd += "--no-default-features "
    if stats != "":
        cmd += "--features \"print_stats fuzzy_log/debug_no_drop\" "
    cmd += "-- "

    servers = servers.replace("^", " ")
    clients_per_host = int(num_clients) / len(env.all_hosts)
    if index_of_host() < int(num_clients) % len(env.all_hosts):
        clients_per_host += 1
    if '-' in servers:
        cmd += "rwr -s " + servers
    else:
        cmd += "uwr -s " + servers
    cmd += " -n " + str(clients_per_host) + " -t " + num_clients + " -j " + jobsize + " -w " + num_writes
    mirror(cmd)

def index_of_host():
    return env.all_hosts.index(env.host_string)

@parallel
def shutdown():
    with settings(warn_only=True):
        sudo("poweroff")

@parallel
def print_hosts():
    print("%(host_string)s/%(all_hosts)s" % env)
    print(env.all_hosts.index(env.host_string))
    print(network.normalize(env.host_string))
