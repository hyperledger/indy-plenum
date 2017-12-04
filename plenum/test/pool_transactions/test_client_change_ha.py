import os
import shutil

from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet
from plenum.test.test_client import genTestClient
from stp_core.network.port_dispenser import genHa

whitelist = ['client already added']


def testClientReconnectUsingDifferentHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                        tdirWithClientPoolTxns, poolTxnClientData):
    """
    Client should not be able to connect to nodes even after it has changed
    its HA. Since running on a local environment, only checking change of port.
    Dont know how to change IP.
    :return:
    """
    # TODO: Check for change of IP too
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithClientPoolTxns)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
    keys_dir = os.path.join(client.keys_dir, client.name)
    client.stop()
    looper.removeProdable(client)

    # Removing RAET keep directory otherwise the client will use the same port
    #  since it will a directory of its name in the keep
    shutil.rmtree(keys_dir, ignore_errors=True)

    ha = genHa()
    client, _ = genTestClient(txnPoolNodeSet, identifier=wallet.defaultId,
                              ha=ha, tmpdir=tdirWithClientPoolTxns,
                              usePoolLedger=True, name=client.name)
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)
