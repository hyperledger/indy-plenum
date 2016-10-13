from plenum.common.test_network_setup import TestNetworkSetup
from plenum.common.txn_util import getTxnOrderedFields

portsStart = 9600


def testBootstrapTestNode(tdir, conf):
    # TODO: Need to add some asserts
    TestNetworkSetup.bootstrapTestNodesCore(
        tdir, conf.poolTransactionsFile,
        conf.domainTransactionsFile,
        getTxnOrderedFields(),
        ips=None, nodeCount=4, clientCount=1,
        nodeNum=1, startingPort=portsStart)
