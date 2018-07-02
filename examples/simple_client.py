#! /usr/bin/env python3
"""
This is a simple script to demonstrate a client connecting to a running
consensus pool. To see it in action, run simple_node.py in four separate
terminals, and in a fifth one, run this script.

TODO: create client
TODO: demonstrate client verification key bootstrapping
"""
from collections import OrderedDict

from plenum.client.client import Client
from stp_core.loop.looper import Looper
from plenum.common.signer_simple import SimpleSigner
from plenum.common.temp_file_util import SafeTemporaryDirectory


def run_node():

    cliNodeReg = OrderedDict([
        ('AlphaC', ('127.0.0.1', 8002)),
        ('BetaC', ('127.0.0.1', 8004)),
        ('GammaC', ('127.0.0.1', 8006)),
        ('DeltaC', ('127.0.0.1', 8008))])

    with Looper(debug=False) as looper:
        # Nodes persist keys when bootstrapping to other nodes and reconnecting
        # using an ephemeral temporary directory when proving a concept is a
        # nice way to keep things clean.
        with SafeTemporaryDirectory() as tmpdir:
            clientName = 'Joe'

            # this seed is used by the signer to deterministically generate
            # a signature verification key that is shared out of band with the
            # consensus pool
            seed = b'a 32 byte super secret seed.....'
            assert len(seed) == 32
            signer = SimpleSigner(clientName, seed)
            assert signer.verkey == b'cffbb88a142be2f62d1b408818e21a2f' \
                                    b'887c4442ae035a260d4cc2ec28ae24d6'

            client_address = ('127.0.0.1', 8000)

            client = Client(clientName,
                            cliNodeReg,
                            ha=client_address,
                            signer=signer,
                            basedirpath=tmpdir)
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
