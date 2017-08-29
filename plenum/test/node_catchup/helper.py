import types

from plenum.common.util import check_if_all_equal_in_list
from stp_core.common.log import getlogger
from typing import Iterable

from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID
from stp_core.loop.eventually import eventually
from stp_core.types import HA
from plenum.test.helper import checkLedgerEquality, checkStateEquality, \
    check_seqno_db_equality, assertEquality, check_last_ordered_3pc
from plenum.test.test_client import TestClient
from plenum.test.test_node import TestNode
from plenum.test import waits
import pytest


logger = getlogger()


# TODO: This should just take an arbitrary number of nodes and check for their
#  ledgers to be equal
def checkNodeDataForEquality(node: TestNode,
                             *otherNodes: Iterable[TestNode]):
    # Checks for node's ledgers and state's to be equal
    for n in otherNodes:
        check_last_ordered_3pc(node, n)
        check_seqno_db_equality(node.seqNoDB, n.seqNoDB)
        checkLedgerEquality(node.domainLedger, n.domainLedger)
        checkStateEquality(node.getState(DOMAIN_LEDGER_ID),
                           n.getState(DOMAIN_LEDGER_ID))
        if n.poolLedger:
            checkLedgerEquality(node.poolLedger, n.poolLedger)
            checkStateEquality(node.getState(POOL_LEDGER_ID),
                               n.getState(POOL_LEDGER_ID))


def checkNodeDataForInequality(node: TestNode,
                               *otherNodes: Iterable[TestNode]):
    # Checks for node's ledgers and state's to be unequal
    with pytest.raises(AssertionError):
        checkNodeDataForEquality(node, *otherNodes)


def waitNodeDataEquality(looper,
                         referenceNode: TestNode,
                         *otherNodes: Iterable[TestNode],
                         customTimeout=None):
    """
    Wait for node ledger to become equal

    :param referenceNode: node whose ledger used as a reference
    """

    numOfNodes = len(otherNodes) + 1
    timeout = customTimeout or waits.expectedPoolGetReadyTimeout(numOfNodes)
    looper.run(eventually(checkNodeDataForEquality,
                          referenceNode,
                          *otherNodes,
                          retryWait=1, timeout=timeout))


def waitNodeDataInequality(looper,
                           referenceNode: TestNode,
                           *otherNodes: Iterable[TestNode],
                           customTimeout=None):
    """
    Wait for node ledger to become equal

    :param referenceNode: node whose ledger used as a reference
    """

    numOfNodes = len(otherNodes) + 1
    timeout = customTimeout or waits.expectedPoolGetReadyTimeout(numOfNodes)
    looper.run(eventually(checkNodeDataForInequality,
                          referenceNode,
                          *otherNodes,
                          retryWait=1, timeout=timeout))


def ensure_all_nodes_have_same_data(looper, nodes, custom_timeout=None):
    node = next(iter(nodes))
    other_nodes = [n for n in nodes if n != node]
    waitNodeDataEquality(looper, node, *other_nodes,
                         customTimeout=custom_timeout)


def ensureNewNodeConnectedClient(looper, client: TestClient, node: TestNode):
    stackParams = node.clientStackParams
    client.nodeReg[stackParams['name']] = HA('127.0.0.1', stackParams['ha'][1])
    looper.run(client.ensureConnectedToNodes())


def checkClientPoolLedgerSameAsNodes(client: TestClient,
                                     *nodes: Iterable[TestNode]):
    for n in nodes:
        checkLedgerEquality(client.ledger, n.poolLedger)


def ensureClientConnectedToNodesAndPoolLedgerSame(looper,
                                                  client: TestClient,
                                                  *nodes: Iterable[TestNode]):
    looper.run(client.ensureConnectedToNodes())
    timeout = waits.expectedPoolGetReadyTimeout(len(nodes))
    looper.run(eventually(checkClientPoolLedgerSameAsNodes,
                          client,
                          *nodes,
                          retryWait=.5,
                          timeout=timeout))


def check_ledger_state(node, ledger_id, ledger_state):
    assertEquality(node.ledgerManager.getLedgerInfoByType(ledger_id).state,
                   ledger_state)


def check_last_3pc_master(node, other_nodes):
    last_3pc = [node.replicas[0].last_ordered_3pc]
    for n in other_nodes:
        last_3pc.append(n.replicas[0].last_ordered_3pc)
    assert check_if_all_equal_in_list(last_3pc)


def make_a_node_catchup_twice(target_node, other_nodes, ledger_id, shorten_by):
    """
    All `other_nodes` make the `node` catchup multiple times by serving
    consistency proof of a ledger smaller by `shorten_by` txns
    """
    nodes_to_send_proof_of_small_ledger = {n.name for n in other_nodes}
    orig_methods = {}
    for node in other_nodes:
        orig_methods[node.name] = node.ledgerManager._buildConsistencyProof

        def patched_method(self, ledgerId, seqNoStart, seqNoEnd):
            if self.owner.name in nodes_to_send_proof_of_small_ledger:
                import inspect
                curframe = inspect.currentframe()
                calframe = inspect.getouterframes(curframe, 2)
                # For domain ledger, send a proof for a small ledger to the bad
                # node
                if calframe[1][
                    3] == node.ledgerManager.getConsistencyProof.__name__ \
                        and calframe[2].frame.f_locals['frm'] == target_node.name \
                        and ledgerId == ledger_id:
                    # Pop so this node name, so proof for smaller ledger is not
                    # served again
                    nodes_to_send_proof_of_small_ledger.remove(self.owner.name)
                    logger.debug('{} sending a proof to {} for {} instead '
                                 'of {}'.format(self.owner.name, target_node.name,
                                                seqNoEnd - shorten_by, seqNoEnd))
                    return orig_methods[node.name](ledgerId, seqNoStart,
                                                   seqNoEnd - shorten_by)
            return orig_methods[node.name](ledgerId, seqNoStart, seqNoEnd)

        node.ledgerManager._buildConsistencyProof = types.MethodType(
            patched_method, node.ledgerManager)
