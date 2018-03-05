from plenum.common.constants import STEWARD, DATA
from plenum.common.request import Request
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
    newNode = sendAddNewNode(tdir, tconf, newNodeName, steward1, newSteward)
    data = {}
    for item in newNode:
        if isinstance(item, Request):
            data = item.operation.get(DATA)

    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1,
                             "existing data has conflicts with " +
                             "request data {}".format(data),
                             node.clientstack.name)
