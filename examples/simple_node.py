#! /usr/bin/env python3
"""
This is a simple script to start up a node. To see it in action, open up four
separate terminals, and in each one, run this script for a different node.

Usage:
simple_node.py <node_name>

Where <node_name> is one of Alpha, Beta, Gamma, Delta.

"""
import sys
from collections import OrderedDict

from stp_core.loop.looper import Looper
from plenum.common.temp_file_util import SafeTemporaryDirectory
from plenum.server.node import Node


def run_node():

    nodeReg = OrderedDict([
        ('Alpha', ('127.0.0.1', 9701)),
        ('Beta', ('127.0.0.1', 9703)),
        ('Gamma', ('127.0.0.1', 9705)),
        ('Delta', ('127.0.0.1', 9707))])

    # the first argument should be the node name
    try:
        nodeName = sys.argv[1]
    except IndexError:
        names = list(nodeReg.keys())
        print("Please supply a node name (one of {}) as the first argument.".
              format(", ".join(names)))
        print("For example:")
        print("    {} {}".format(sys.argv[0], names[0]))
        return

    with Looper(debug=False) as looper:
        # Nodes persist keys when bootstrapping to other nodes and reconnecting
        # using an ephemeral temporary directory when proving a concept is a
        # nice way to keep things tidy.
        with SafeTemporaryDirectory() as tmpdir:
            node = Node(nodeName, nodeReg, basedirpath=tmpdir)

            # see simple_client.py
            joe_verkey = b'cffbb88a142be2f62d1b408818e21a2f' \
                         b'887c4442ae035a260d4cc2ec28ae24d6'
            node.clientAuthNr.addIdr("Joe", joe_verkey)

            looper.add(node)
            node.startKeySharing()
            looper.run()


if __name__ == '__main__':
    run_node()
