import os
import shutil

import pytest
from plenum.client.signer import SimpleSigner

from plenum.test.helper import genTestClient, genHa

from plenum.common.looper import Looper
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


whitelist = ['client already added']


def testClientReconnectUsingDifferentHa(looper, txnPoolNodeSet,
                                        tdirWithPoolTxns, poolTxnClientData):
    """
    Client should not be able to connect to nodes even after it has changed
    its HA. Since running on a local environment, only checking change of port.
    Dont know how to change IP.
    :return:
    """
    # TODO: Check for change of IP too
    name, seed = poolTxnClientData
    signer = SimpleSigner(seed=seed)
    name = "testClient96541"
    ha = genHa()
    client = genTestClient(txnPoolNodeSet, signer=signer, ha=ha,
                           tmpdir=tdirWithPoolTxns, usePoolLedger=True,
                           name=name)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
    basedirpath = client.basedirpath
    dataLocation = client.dataLocation
    looper.removeProdable(client)

    # Removing RAET keep directory otherwise the client will use the same port
    #  since it will a directory of its name in the keep
    shutil.rmtree(os.path.join(basedirpath, name), ignore_errors=True)

    ha = genHa()
    client = genTestClient(txnPoolNodeSet, signer=signer, ha=ha,
                           tmpdir=tdirWithPoolTxns, usePoolLedger=True,
                           name=name)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
