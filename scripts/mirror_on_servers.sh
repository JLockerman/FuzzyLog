#!/bin/bash

if (($# < 3)); then
    echo
    echo -e "\033[0;32mUsage\033[0m: ./mirror_on_servers.sh <username> <script> <hostnames>+"
    echo
    exit
fi

dir_name=${PWD##*/}
username="$1"
shift
echo " directory:  $dir_name"
echo "  username:  $username"
if [[ ! -z "$1" ]]; then
    script="source ~/.profile && cd ~/$dir_name && $1"
    echo "   running:  $script"
else
    script="cd ~/$dir_name"
    echo "   noscript"
fi
shift

transfer_to_servers () {
    #echo "Starting $# servers..."
    for hostname in "$@"
    {
        echo "Sending to:  $username@$hostname:~/$dir_name"
        # The options added to ssh causes it to resuse the connection for both
        # commands, thus only needing one login per server
        rsync -e "ssh -o \"ControlPath=~/.ssh/master$$\" -o \"ControlMaster=auto\" -o \"ControlPersist=10\"" --exclude=".*" --exclude=".*/" --filter=':- .gitignore' -P -avz ./ "$username@$hostname:~/$dir_name"
        ssh -o "ControlPath=~/.ssh/master$$" -o "ControlMaster=auto" -o "ControlPersist=10" -t "$username@$hostname" $script
        #
    }
}

transfer_to_servers $@

echo "done."
