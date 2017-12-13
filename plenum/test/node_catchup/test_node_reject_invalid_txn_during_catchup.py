import pytest
import types

from plenum.common.ledger import Ledger
from stp_core.common.log import getlogger
from plenum.common.constants import TXN_TYPE, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import CatchupReq, CatchupRep
from plenum.common.types import f
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas
from plenum.test import waits


# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()


txnCount = 10


def testNodeRejectingInvalidTxns(tconf, txnPoolNodeSet, patched_node,
                                 nodeCreatedAfterSomeTxns):
    """
    A newly joined node is catching up and sends catchup requests to other
    nodes but one of the nodes replies with incorrect transactions. The newly
    joined node detects that and rejects the transactions and thus blacklists
    the node. Ii thus cannot complete the process till the timeout and then
    requests the missing transactions.
    """
    looper, newNode, client, wallet, _, _ = nodeCreatedAfterSomeTxns
    bad_node = patched_node

    do_not_tell_clients_about_newly_joined_node(txnPoolNodeSet)

    logger.debug('Catchup request processor of {} patched'.format(bad_node))

    looper.run(checkNodesConnected(txnPoolNodeSet))

    # catchup #1 -> CatchupTransactionsTimeout -> catchup #2
    catchup_timeout = waits.expectedPoolCatchupTime(len(txnPoolNodeSet) + 1)
    timeout = 2 * catchup_timeout + tconf.CatchupTransactionsTimeout

    # have to skip seqno_db check because the txns are not executed
    # on the new node
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:-1],
                         customTimeout=timeout)

    assert newNode.isNodeBlacklisted(bad_node.name)


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


def _sendIncorrectTxns(self, req, frm):
    ledgerId = getattr(req, f.LEDGER_ID.nm)
    if ledgerId == DOMAIN_LEDGER_ID:
        logger.info("{} being malicious and sending incorrect transactions"
                    " for catchup request {} from {}".
                    format(self, req, frm))
        start, end = getattr(req, f.SEQ_NO_START.nm), \
            getattr(req, f.SEQ_NO_END.nm)
        ledger = self.getLedgerForMsg(req)
        txns = {}
        for seqNo, txn in ledger.getAllTxn(start, end):
            # Since the type of random request is `buy`
            if txn.get(TXN_TYPE) == "buy":
                txn[TXN_TYPE] = "randombuy"
            txns[seqNo] = txn
        consProof = [Ledger.hashToStr(p) for p in
                     ledger.tree.consistency_proof(end, ledger.size)]
        self.sendTo(msg=CatchupRep(getattr(req, f.LEDGER_ID.nm), txns,
                                   consProof), to=frm)
    else:
        self.processCatchupReq(req, frm)
