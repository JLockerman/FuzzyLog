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
    run('sudo DEBIAN_FRONTEND=noninteractive apt-get -y install g++')
    run('sudo DEBIAN_FRONTEND=noninteractive apt-get -y install make')
    run('curl https://sh.rustup.rs -sSf | sh -s -- -y', pty=False)
    run('export PATH=$HOME/.cargo/bin:$PATH')
    #run('ulimit -n 2048')

@parallel
def sync():
    #local("rsync --filter=':- .gitignore' -P -avz ./ " + env.host_string + ":~/" + dirname)
    rsync_project(remote_dir="~/", exclude=[".*",".*/"], extra_opts="--filter=':- .gitignore'")

@parallel
def clean():
    mirror("./cleaner.sh")

@parallel
def update_rust():
    mirror('rustup override remove')
    run('rustup toolchain uninstall 1.21.0')
    run('rustup toolchain uninstall 1.19.0')
    run('rustup toolchain uninstall 1.18.0')
    # run('rustup toolchain uninstall nightly')
    run('rustup toolchain uninstall stable')
    run('rustup default nightly') #rustup toolchain list
    # run('rustup update')

def export(dir):
    run("echo 'export DELOS_RUST_LOC=" + dir + "' >> ~/.bashrc")


@parallel
def zookeeper():
    server_num = str(index_of_host() + 1)
    with cd("~/zookeeper-3.4.10"):
        run("sudo mkdir /mnt/ramdisk && sudo mount -t tmpfs -o size=1G tmpfs /mnt/ramdisk && mkdir /mnt/ramdisk/zookeeper")
        run("rm -rf /mnt/ramdisk/zookeeper/version-2/")
        run("echo " + server_num +  " > /mnt/ramdisk/zookeeper/myid")
        run("./bin/zkServer.sh start-foreground")

@parallel
def try_ex(cmd=''):
    fuzzy_log_dir = os.path.basename(os.getcwd())
    with settings(hide('warnings'), warn_only=True):
        with cd(fuzzy_log_dir):
            run(cmd, pty=False)


@parallel
def mirror(cmd=''):
    sync()
    cmd = str.replace(cmd, '#server_num', str(index_of_host()))
    fuzzy_log_dir = os.path.basename(os.getcwd())
    with cd(fuzzy_log_dir):
        run(cmd, pty=False)

# NOTE connecting to AWS requires use of  the public IP addr
#      unfortunately communication within AWS requires the use of the private IP addr by default
#      so you need to input the server addrs twice once comma separated for -H
#      once '^' separated for chain_hosts
@parallel
def run_chain(chain_hosts="", port="13289", trace="", workers="", debug="", stats="", nt=""):
    with settings(hide('warnings'), warn_only=True):
        run("pkill delos_tcp_serve")
        run("pkill delos_tcp_server")
        run("pkill gdb")
    host_index = index_of_host()
    #cmd = "cd servers/tcp_server && "
    cmd = ""
    if trace != "":
        cmd += "RUST_LOG=" + trace + " "
    #cmd += "RUST_BACKTRACE=short RUSTFLAGS='-Z sanitizer=memory' cargo run --target x86_64-unknown-linux-gnu "
    # cmd += "RUST_BACKTRACE=short RUSTFLAGS=\"-Z sanitizer=memory -C target-cpu=native\" ASAN_OPTIONS=detect_odr_violation=0 cargo run --target x86_64-unknown-linux-gnu "
    # cmd += "RUST_BACKTRACE=short RUSTFLAGS=\"-Z sanitizer=thread\" ASAN_OPTIONS=detect_odr_violation=0 cargo run --target x86_64-unknown-linux-gnu "
    #MSAN_options='suppressions=san_blacklist.txt'
    # cmd += "RUST_BACKTRACE=short cargo run --target x86_64-unknown-linux-gnu "
    cmd += "RUST_BACKTRACE=short cargo run "
    # cmd += "cargo build "
    if debug == "":
        cmd += "--release "
    if stats != "":
        cmd += "--features \"no_trace print_stats\" " # fuzzy_log/debug_no_drop
    else:
        if nt != "":
            cmd += "--features \"no_trace\" "
    # cmd += "&& gdb -ex=run --args ./target/release/delos_tcp_server " + port
    cmd += "-- " + port

    # cmd = "cargo build --release && perf record --quiet --call-graph dwarf -o perf.data -- ./target/release/delos_tcp_server " + port + " ""

    my_chain = []
    index_in_chain = 0
    my_chain_num = 0
    num_chains = 0
    if chain_hosts != "":
        chains = chain_hosts.split("^")
        num_chains = len(chains)
        chain_len = len(chains[0].split("#"))
        my_chain_num = host_index / chain_len
        index_in_chain = host_index % chain_len
        i = 0
        for chain in chains:
            servers = chain.split("#")
            assert(len(servers) == chain_len)
            if i == my_chain_num:
                my_chain = servers
            i += 1

    if index_in_chain > 0:
        prev_host = my_chain[index_in_chain-1]
        cmd += " -up " + prev_host + ":" + port
    if index_in_chain + 1 < len(my_chain):
        next_host = my_chain[index_in_chain+1]
        cmd += " -dwn " + next_host
    if workers != "":
        cmd += " -w " + workers
    if num_chains > 1:
        cmd += " -ig " + str(my_chain_num) + ":" + str(num_chains)
    cmd = "cd servers/tcp_server && " + cmd
    # print(cmd)
    mirror(cmd)

@parallel
def kill_server():
    with settings(hide('warnings'), warn_only=True):
        run("pkill delos_tcp_serve")
        run("pkill delos_tcp_server")

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
        cmd += "RUST_BACKTRACE=1 RUSTFLAGS=\"-C target-cpu=native\" cargo run --release "
    else:
        cmd += "RUSTFLAGS=\"-C target-cpu=native\" cargo run "
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
