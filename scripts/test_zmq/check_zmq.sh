#!/usr/bin/env bash

set -e

function print_usage {
cat << EOM
Usage: 
   check_zmq.sh client '--zmq addr:zmq_port --tcp addr:tcp_port'
   check_zmq.sh server '--zmq_port <zmq_port> --tcp_port <tcp_port>'  
EOM
exit
}

if [ -z $1 ]; then
	print_usage
fi

if [ "$1" != "client" ] && [ "$1" != "server" ]; then
	print_usage
fi


# Make environment

V_DIR=/home/$USER/.virtualenvs
V_NAME=plenum_check_zmq

if ! which virtualenv ; then
	pip3 install virtualenv
fi

if [ ! -d $V_DIR ]; then
	mkdir $V_DIR
fi

pushd $V_DIR

if [ ! -d $V_DIR/$V_NAME ]; then
	virtualenv $V_NAME
	$V_DIR/$V_NAME/bin/pip install plenum-zmq-check
fi

# Activate env

source $V_DIR/$V_NAME/bin/activate

if [ "$1" = "client" ]; then
	client.py $2
fi

if [ "$1" = "server" ]; then
	server.py $2
fi

deactivate
