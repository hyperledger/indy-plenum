# Overview 
This tool would be helpful for checking zmq connection between two remote machines. It consists of two parts, client and server and performs 2 checks:
* tcp connection check firstly
* zmq connection check if tcp was successful

## Client
As a client this tool has the next required input parameters:
```
--zmq ZMQ   String with address:port representation. Like 127.0.0.1:9702
--tcp TCP   String with address:port representation. Like 127.0.0.1:10000
```
`--zmq` and `--tcp` options are required, because they need to determine address/port tuple for remote connection for tcp and zmq protocols.
Also, it's important to make sure that both of protocols are acceptable to create connection between two nodes.
`address` - it's IP address of node which will be used to run this script as `server`.

## Server
As a server this tool has the next input parameters:
```
--zmq_port ZMQ_PORT  Port which will be used for ZMQ client's connections
--tcp_port TCP_PORT  Port which will be used for TCP client's connections
--addr ADDR          Address which will used for incoming client's
                     connection. 0.0.0.0 by default
```
As for client `--zmq_port` and `--tcp_port` parameters are required too and define port for client's connections.
`--addr` parameter is optional and define IP address for binding. '0.0.0.0' will be used by default.

## Example
Let client's machine IP address be `10.0.0.1` and server's machine IP address be `10.0.0.2`. In this case, for checking connections we can run server as:
```
check_zmq.sh server '--zmq_port 9999 --tcp_port 10000 --addr 10.0.0.2'     # --addr is not required now, 0.0.0.0 is enough
```
and command for client run is:
```
check_zmq.sh client '--zmq 10.0.0.2:9999 --tcp 10.0.0.2:10000'
```

## check_zmq.sh overview

### Requirements
The next packages should be installed:
```
     * python3
     * python3-pip
     * python3-setuptools
```
Also, this script requires `virtualenv` and will install it if doesn't exist.

### Actions
For the first run `check_zmq.sh` will create virtual environment named `plenum_check_zmq` and placed in `<home_directory>/.virtualenvs` directory and will use this env for other runs. 
