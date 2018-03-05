from plenum.common.constants import STEWARD
from plenum.test.pool_transactions.helper import sendAddNewNode,  addNewClient
from plenum.test.helper import waitRejectWithReason


def test_add_node_with_not_unique_alias(looper,
                                        tdir,
                                        tconf,
                                        txnPoolNodeSet,
                                        steward1,
                                        stewardWallet):
    newNodeName = "Alpha"
    newSteward = addNewClient(STEWARD, looper, steward1, stewardWallet,
                              "testStewardMy")
    sendAddNewNode(tdir, tconf, newNodeName, steward1, newSteward)

    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1,
                             "Alias " + newNodeName + " already exists",
                             node.clientstack.name)
