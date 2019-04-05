import pytest
import types

from plenum.common.txn_util import get_type, set_type
from plenum.common.util import randomString
from plenum.test.helper import sdk_send_random_and_check

from plenum.common.ledger import Ledger
from plenum.test.conftest import getValueFromModule
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_pool_refresh
from stp_core.common.log import getlogger
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import CatchupReq, CatchupRep
from plenum.common.types import f
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()

txnCount = 10


def testNodeRejectingInvalidTxns(looper, sdk_pool_handle, sdk_wallet_client,
                                 tconf, tdir, txnPoolNodeSet, patched_node,
                                 request,
                                 sdk_wallet_steward, testNodeClass, allPluginsPath,
                                 do_post_node_creation):
    """
    A newly joined node is catching up and sends catchup requests to other
    nodes but one of the nodes replies with incorrect transactions. The newly
    joined node detects that and rejects the transactions and thus blacklists
    the node. Ii thus cannot complete the process till the timeout and then
    requests the missing transactions.
    """
    txnCount = getValueFromModule(request, "txnCount", 5)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              txnCount)
    new_steward_name = randomString()
    new_node_name = "Epsilon"
    new_steward_wallet_handle, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, new_node_name, tdir, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=True,
        do_post_node_creation=do_post_node_creation)
    sdk_pool_refresh(looper, sdk_pool_handle)

    bad_node = patched_node

    do_not_tell_clients_about_newly_joined_node(txnPoolNodeSet)

    logger.debug('Catchup request processor of {} patched'.format(bad_node))

    looper.run(checkNodesConnected(txnPoolNodeSet))

    # catchup #1 -> CatchupTransactionsTimeout -> catchup #2
    catchup_timeout = waits.expectedPoolCatchupTime(len(txnPoolNodeSet) + 1)
    timeout = 2 * catchup_timeout + tconf.CatchupTransactionsTimeout

    # have to skip seqno_db check because the txns are not executed
    # on the new node
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         customTimeout=timeout,
                         exclude_from_check=['check_last_ordered_3pc_backup'])

    assert new_node.isNodeBlacklisted(bad_node.name)


@pytest.fixture
def patched_node(txnPoolNodeSet):
    node = get_any_non_primary_node(txnPoolNodeSet)
    node_will_send_incorrect_catchup(node)
    return node


def get_any_non_primary_node(nodes):
    npr = getNonPrimaryReplicas(nodes, 0)
    return npr[0].node


def do_not_tell_clients_about_newly_joined_node(nodes):
    for node in nodes:
        node.sendPoolInfoToClients = types.MethodType(lambda x, y: None, node)


def node_will_send_incorrect_catchup(node):
    node.nodeMsgRouter.routes[CatchupReq] = types.MethodType(
        _sendIncorrectTxns,
        node.ledgerManager
    )


def _sendIncorrectTxns(self, req):
    ledgerId = getattr(req, f.LEDGER_ID.nm)
    if ledgerId == DOMAIN_LEDGER_ID:
        logger.info("{} being malicious and sending incorrect transactions"
                    " for catchup request {} from {}".
                    format(self, req, req.frm))
        start, end = getattr(req, f.SEQ_NO_START.nm), \
                     getattr(req, f.SEQ_NO_END.nm)
        ledger = self.ledgerRegistry[ledgerId].ledger
        txns = {}
        for seqNo, txn in ledger.getAllTxn(start, end):
            # Since the type of random request is `buy`
            if get_type(txn) == "buy":
                set_type(txn, "randombuy")
            txns[seqNo] = txn
        consProof = [Ledger.hashToStr(p) for p in
                     ledger.tree.consistency_proof(end, ledger.size)]
        self.sendTo(msg=CatchupRep(getattr(req, f.LEDGER_ID.nm), txns,
                                   consProof), to=req.frm)
    else:
        self.processCatchupReq(req)
