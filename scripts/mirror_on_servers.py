from fabric.api import run, env, local, run, parallel, cd
from fabric.contrib.project import rsync_project
import os

@parallel
def mirror(cmd=''):
    #local("rsync --filter=':- .gitignore' -P -avz ./ " + env.host_string + ":~/" + dirname)
    rsync_project(remote_dir="~/", exclude=[".*",".*/"], extra_opts="--filter=':- .gitignore'")
    fuzzy_log_dir = os.path.basename(os.getcwd())
    with cd(fuzzy_log_dir):
        run("ls")
        run(cmd, pty=False)

@parallel
def setup():
    #install rust
    run('sudo DEBIAN_FRONTEND=noninteractive apt-get -y update')
    run('sudo DEBIAN_FRONTEND=noninteractive apt-get -y install gcc')
    run('curl https://sh.rustup.rs -sSf | sh -s -- -y', pty=False)
    run('export PATH=$HOME/.cargo/bin:$PATH')
    #run('ulimit -n 2048')
