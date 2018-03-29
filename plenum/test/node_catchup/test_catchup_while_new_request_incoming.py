import types

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.types import f
from plenum.common.messages.node_messages import CatchupReq
from plenum.common.util import randomString
from plenum.test.delayers import cqDelay
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests
from plenum.test.node_catchup.helper import checkNodeDataForEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_pool_refresh
from plenum.test.test_node import TestNode
from stp_core.loop.eventually import eventually


def testNewNodeCatchupWhileIncomingRequests(looper, txnPoolNodeSet,
                                            testNodeClass, tdir,
                                            tconf,
                                            sdk_pool_handle,
                                            sdk_wallet_steward,
                                            allPluginsPath):
    """
    A new node joins while transactions are happening, its catchup requests
    include till where it has to catchup, which would be less than the other
    node's ledger size. In the meantime, the new node will stash all requests
    """

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 5)

    def chkAfterCall(self, req, frm):
        r = self.processCatchupReq(req, frm)
        typ = getattr(req, f.LEDGER_ID.nm)
        if typ == DOMAIN_LEDGER_ID:
            ledger = self.getLedgerForMsg(req)
            assert req.catchupTill <= ledger.size
        return r

    for node in txnPoolNodeSet:
        node.nodeMsgRouter.routes[CatchupReq] = \
            types.MethodType(chkAfterCall, node.ledgerManager)
        node.nodeIbStasher.delay(cqDelay(3))

    print('Sending 5 requests')
    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_steward, 5)
    looper.runFor(1)
    new_steward_name = randomString()
    new_node_name = "Epsilon"
    new_steward_wallet_handle, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, new_node_name, tdir, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=True)
    sdk_pool_refresh(looper, sdk_pool_handle)
    txnPoolNodeSet.append(new_node)
    looper.runFor(2)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 5)
    # TODO select or create a timeout for this case in 'waits'
    looper.run(eventually(checkNodeDataForEquality, new_node,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=80))
    assert new_node.spylog.count(TestNode.processStashedOrderedReqs) > 0
