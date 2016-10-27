#Scripts
##start_servers.sh
```sh
    ./start_servers.sh <server binary> <username> <config file>
```

This script will start a binary on servers found in a config file.
The config file should be of the form
```sh
DELOS_LOCK_SERVER=<server ip:port>
DELOS_CHAIN_SERVERS=<server ip:portss>
```
where `DELOS_CHAIN_SERVERS` contains a list of server ip addresses and ports at which the various chain servers should be started and if there are more than ine such server `DELOS_LOCK_SERVER` shall contain the ip address and port at which the lock server should be started.