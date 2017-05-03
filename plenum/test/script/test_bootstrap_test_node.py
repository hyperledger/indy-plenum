from plenum.common.test_network_setup import TestNetworkSetup
from plenum.common.txn_util import getTxnOrderedFields

portsStart = 9600


def testBootstrapTestNode(tconf):
    # TODO: Need to add some asserts
    TestNetworkSetup.bootstrapTestNodesCore(
        config=tconf, envName="test", appendToLedgers=False,
        domainTxnFieldOrder=getTxnOrderedFields(),
        ips=None, nodeCount=4, clientCount=1,
        nodeNum=1, startingPort=portsStart, nodeParamsFileName='plenum.env')
