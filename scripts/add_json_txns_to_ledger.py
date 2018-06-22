#! /usr/bin/env python3

import os
import sys
import json
import argparse

from stp_core.types import HA
from indy_common.config_util import getConfig
from plenum.server.node import Node
from indy_common.config_helper import NodeConfigHelper

config = getConfig()


def get_ha_cliha_node_name(path_to_env):
    node_name_key = 'NODE_NAME'
    node_port_key = 'NODE_PORT'
    node_clien_port_key = 'NODE_CLIENT_PORT'
    node_name = ''
    node_port = ''
    node_clieint_port = ''
    with open(path_to_env) as fenv:
        for line in fenv.readlines():
            print(line)
            if line.find(node_name_key) != -1:
                node_name = line.split('=')[1].strip()
            elif line.find(node_port_key) != -1:
                node_port = int(line.split('=')[1].strip())
            elif line.find(node_clien_port_key) != -1:
                node_clieint_port = int(line.split('=')[1].strip())
    return node_name, node_port, node_clieint_port


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('infpath', help="Path to previous generated txns", type=str, default='/tmp/generated_txns')
    parser.add_argument('--env_file', help='Path to environment file with node name and ports', default='/etc/indy/indy.env')
    args = parser.parse_args()
    path_to_txns = os.path.realpath(args.infpath)
    path_to_env = os.path.realpath(args.env_file)

    if not os.path.exists(path_to_txns):
        print("Path to txns file does not exist")
        sys.exit(1)

    if not os.path.exists(path_to_env):
        print("Path to env file does not exist")
        sys.exit(1)

    nname, nport, ncliport = get_ha_cliha_node_name(path_to_env)
    ha = HA("0.0.0.0", nport)
    cliha = HA("0.0.0.0", ncliport)
    config_helper = NodeConfigHelper(nname, config)

    node = Node(nname,
                ha=ha,
                cliha=cliha,
                config_helper=config_helper,
                config=config)
    i = 0
    with open(path_to_txns) as txns:
        for txn in txns:
            node.domainLedger.add(json.loads(txn))
            i += 1
            if not i % 1000:
                print("added {} txns".format(i))
