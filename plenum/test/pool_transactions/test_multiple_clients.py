import pytest
import zmq

from plenum.common.eventually import eventually
from plenum.common.port_dispenser import genHa
from plenum.common.util import randomString
from plenum.test.pool_transactions.helper import addNewClient
from plenum.test.test_client import TestClient

import os, psutil


@pytest.mark.skip(reason='This is not a test')
def testMultipleClients(looper, txnPoolNodeSet, steward1, stewardWallet,
                        tdirWithPoolTxns):
    n = txnPoolNodeSet[0]
    n.nodestack.ctx.set(zmq.MAX_SOCKETS, 4096)
    clientNum = 100
    pr = psutil.Process(os.getpid())
    print('Len connections before starting {}'.format(len(pr.connections())))
    for i in range(clientNum):
        name = randomString()
        wallet = addNewClient(None, looper, steward1, stewardWallet,
                              name)

        def chk():
            for node in txnPoolNodeSet:
                assert wallet.defaultId in node.clientAuthNr.clients

        looper.run(eventually(chk, retryWait=1, timeout=5))
        newSteward = TestClient(name=name,
                                nodeReg=None, ha=genHa(),
                                basedirpath=tdirWithPoolTxns)

        looper.add(newSteward)
        looper.run(newSteward.ensureConnectedToNodes())
        print('Connected {}'.format(i))
        print('Len connections {}'.format(len(pr.connections())))
