from plenum.common.test_network_setup import TestNetworkSetup
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.util import randomString

portsStart = 9600


def testBootstrapTestNode(tconf):
    # TODO: Need to add some asserts
    steward_defs, node_defs = TestNetworkSetup.gen_defs(
        ips=None, nodeCount=4, starting_port=portsStart)

    client_defs = TestNetworkSetup.gen_client_defs(clientCount=1)
    trustee_def = TestNetworkSetup.gen_trustee_def(1)
    nodeParamsFile = randomString()

    TestNetworkSetup.bootstrapTestNodesCore(
        config=tconf, envName="test", appendToLedgers=False,
        domainTxnFieldOrder=getTxnOrderedFields(),
        trustee_def=trustee_def, steward_defs=steward_defs,
        node_defs=node_defs, client_defs=client_defs, localNodes=1,
        nodeParamsFileName=nodeParamsFile)
