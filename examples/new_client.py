#! /usr/bin/env python3
"""
This is a simple script to demonstrate a client connecting to a running
consensus pool. To see it in action, run simple_node.py in four separate
terminals, and in a fifth one, run this script.

TODO: create client
TODO: demonstrate client verification key bootstrapping
"""

# TODO Remove this file, just for now

from plenum.client.client import Client
from stp_core.loop.looper import Looper
from plenum.common.signer_simple import SimpleSigner
from plenum.common.config_util import getConfig


def run_node():
    config = getConfig()
    with Looper(debug=config.LOOPER_DEBUG) as looper:
        # Nodes persist keys when bootstrapping to other nodes and reconnecting
        # using an ephemeral temporary directory when proving a concept is a
        # nice way to keep things clean.
        basedirpath = config.baseDir
        cliNodeReg = {k: v[0] for k, v in config.cliNodeReg.items()}
        clientName = 'Alice'

        # this seed is used by the signer to deterministically generate
        # a signature verification key that is shared out of band with the
        # consensus pool
        seed = b'22222222222222222222222222222222'
        assert len(seed) == 32
        signer = SimpleSigner(clientName, seed)

        client_address = ('0.0.0.0', 9700)

        client = Client(clientName,
                        cliNodeReg,
                        ha=client_address,
                        signer=signer,
                        basedirpath=basedirpath)
        looper.add(client)

        # give the client time to connect
        looper.runFor(3)

        # a simple message
        msg = {'life_answer': 42}

        # submit the request to the pool
        request, = client.submit_DEPRECATED(msg)

        # allow time for the request to be executed
        looper.runFor(3)

        reply, status = client.getReply(request.reqId)
        print('')
        print('Reply: {}\n'.format(reply))
        print('Status: {}\n'.format(status))


if __name__ == '__main__':
    run_node()
