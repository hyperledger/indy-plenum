import pytest
import types

from plenum.common.txn_util import get_type, set_type
from plenum.test.delayers import lsDelay, delay_3pc
from plenum.test.helper import sdk_send_random_and_check, assert_in

from plenum.common.ledger import Ledger
from plenum.test.stasher import delay_rules, delay_rules_without_processing
from stp_core.common.log import getlogger
from plenum.common.constants import DOMAIN_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.common.messages.node_messages import CatchupReq, CatchupRep
from plenum.common.types import f
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.test_node import getNonPrimaryReplicas

from stp_core.loop.eventually import eventually

logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    old = tconf.CATCHUP_BATCH_SIZE
    tconf.CATCHUP_BATCH_SIZE = 1  # To make sure all nodes receive catchup requests
    yield tconf
    tconf.CATCHUP_BATCH_SIZE = old


def test_node_reject_invalid_txn_during_catchup(looper, sdk_pool_handle, sdk_wallet_client,
                                                tconf, tdir, txnPoolNodeSet,
                                                bad_node, lagging_node):
    """
    Make sure that catching up node will blacklist nodes which send incorrect catchup replies
    """
    normal_nodes = [node for node in txnPoolNodeSet
                    if node not in [bad_node, lagging_node]]
    normal_stashers = [node.nodeIbStasher for node in normal_nodes]

    with delay_rules_without_processing(lagging_node.nodeIbStasher, delay_3pc()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

        # Perform catchup, while making sure that cons proof from bad node is received
        # before cons proofs from normal nodes, so bad node can participate in catchup
        with delay_rules(normal_stashers, lsDelay()):
            lagging_node.start_catchup()

            node_leecher = lagging_node.ledgerManager._node_leecher
            audit_cons_proof_service = node_leecher._leechers[AUDIT_LEDGER_ID]._cons_proof_service
            looper.run(eventually(lambda: assert_in(bad_node.name, audit_cons_proof_service._cons_proofs)))

        waitNodeDataEquality(looper, lagging_node, *normal_nodes)
        assert lagging_node.isNodeBlacklisted(bad_node.name)


@pytest.fixture
def bad_node(txnPoolNodeSet):
    node = get_any_non_primary_node(txnPoolNodeSet)
    node_will_send_incorrect_catchup(node)
    return node


@pytest.fixture
def lagging_node(txnPoolNodeSet, bad_node):
    return get_any_non_primary_node(set(txnPoolNodeSet) - {bad_node})


def get_any_non_primary_node(nodes):
    npr = getNonPrimaryReplicas(nodes, 0)
    return npr[0].node


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
                                   consProof), to=frm)
    else:
        self.processCatchupReq(req, frm)
