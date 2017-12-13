
import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.test.delayers import cr_delay
from plenum.test.spy_helpers import get_count

from stp_core.loop.eventually import eventually
from plenum.common.types import HA
from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    check_last_ordered_3pc
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_ledger_state
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_ledger_manager import TestLedgerManager
from plenum.test.test_node import checkNodesConnected, TestNode
from plenum.test import waits

# Do not remove the next import
from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()
txnCount = 5


def testNewNodeCatchup(newNodeCaughtUp):
    """
    A new node that joins after some transactions are done should eventually get
    those transactions.
    TODO: Test correct statuses are exchanged
    TODO: Test correct consistency proofs are generated
    :return:
    """


def testPoolLegerCatchupBeforeDomainLedgerCatchup(txnPoolNodeSet,
                                                  newNodeCaughtUp):
    """
    For new node, this should be the sequence of events:
     1. Pool ledger starts catching up.
     2. Pool ledger completes catching up.
     3. Domain ledger starts catching up
     4. Domain ledger completes catching up
    Every node's pool ledger starts catching up before it
    """
    newNode = newNodeCaughtUp
    starts = newNode.ledgerManager.spylog.getAll(
        TestLedgerManager.startCatchUpProcess.__name__)
    completes = newNode.ledgerManager.spylog.getAll(
        TestLedgerManager.catchupCompleted.__name__)
    startTimes = {}
    completionTimes = {}
    for start in starts:
        startTimes[start.params.get('ledgerId')] = start.endtime
    for comp in completes:
        completionTimes[comp.params.get('ledgerId')] = comp.endtime
    assert startTimes[0] < completionTimes[0] < \
        startTimes[1] < completionTimes[1]


@pytest.mark.skip(reason="SOV-554. "
                         "Test implementation pending, although bug fixed")
def testDelayedLedgerStatusNotChangingState():
    """
    Scenario: When a domain `LedgerStatus` arrives when the node is in
    `participating` mode, the mode should not change to `discovered` if found
    the arriving `LedgerStatus` to be ok.
    """
    raise NotImplementedError
