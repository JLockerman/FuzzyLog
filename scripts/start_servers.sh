#!/bin/bash

if (($# < 3)); then
    echo -e "\033[0;32mUsage\033[0m: ./start_servers.sh <server binary> <username> <config file>"
    echo
    echo "       The config file should contain DELOS_CHAIN_SERVERS=<server ips> and"
    echo "       if there are more than one chain server DELOS_LOCK_SERVER=<server ip>."
    echo "       No ip address may be duplicated."
    echo
    exit
fi

filename="$1"
username="$2"

echo "binary file:         $filename"
echo "username:            $username"
echo "config file:         $3"

source "$3"

echo "DELOS_LOCK_SERVER:   $DELOS_LOCK_SERVER"
echo "DELOS_CHAIN_SERVERS: $DELOS_CHAIN_SERVERS"

start_servers () {
    #echo "Starting $# servers..."
    server_num=0
    for addr in "$@"
    {
        hostname=`expr "$addr" : '\(.*\):[0-9]*$'`
        port=`expr "$addr" : '.*:\([0-9]*$\)'`
        # The options added to ssh causes it to resuse the connection for both
        # commands, thus only needing one login per server
        rsync -e "ssh -o \"ControlPath=~/.ssh/master$$\" -o \"ControlMaster=auto\" -o \"ControlPersist=10\"" -P --ignore-existing "$filename" "$username@$hostname:~"
        ssh -o "ControlPath=~/.ssh/master$$" -o "ControlMaster=auto" -o "ControlPersist=10" "$username@$hostname" "chmod +x ./$filename && ./$filename $port $server_num $# &>/dev/null & echo Starting server $server_num on $hostname at port $port press control-c to continue"
        ((server_num+=1))
    }
}

start_servers $DELOS_LOCK_SERVER
start_servers $DELOS_CHAIN_SERVERS

echo "done."
