import types
from functools import partial

import pytest

from plenum.common.util import check_if_all_equal_in_list
from plenum.test import waits
from plenum.test.helper import checkLedgerEquality, checkStateEquality, \
    check_seqno_db_equality, assertEquality, check_last_ordered_3pc
from plenum.test.test_client import TestClient
from plenum.test.test_node import TestNode
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from stp_core.types import HA

logger = getlogger()


# TODO: This should just take an arbitrary number of nodes and check for their
#  ledgers to be equal
def checkNodeDataForEquality(node: TestNode,
                             *otherNodes: TestNode,
                             exclude_from_check=None):
    def chk_ledger_and_state(first_node, second_node, ledger_id):
        checkLedgerEquality(first_node.getLedger(ledger_id),
                            second_node.getLedger(ledger_id))
        if not exclude_from_check or 'check_state' not in exclude_from_check:
            checkStateEquality(first_node.getState(ledger_id),
                               second_node.getState(ledger_id))

    # Checks for node's ledgers and state's to be equal
    for n in otherNodes:
        if exclude_from_check and 'check_last_ordered_3pc' not in exclude_from_check:
            check_last_ordered_3pc(node, n)
        else:
            logger.debug("Excluding check_last_ordered_3pc check")

        if exclude_from_check and 'check_seqno_db' not in exclude_from_check:
            check_seqno_db_equality(node.seqNoDB, n.seqNoDB)
        else:
            logger.debug("Excluding check_seqno_db_equality check")

        for ledger_id in n.ledgerManager.ledgerRegistry:
            chk_ledger_and_state(node, n, ledger_id)


def checkNodeDataForInequality(node: TestNode,
                               *otherNodes: TestNode,
                               exclude_from_check=None):
    # Checks for node's ledgers and state's to be unequal
    with pytest.raises(AssertionError):
        checkNodeDataForEquality(node, *otherNodes, exclude_from_check=exclude_from_check)


def waitNodeDataEquality(looper,
                         referenceNode: TestNode,
                         *otherNodes: TestNode,
                         customTimeout=None,
                         exclude_from_check=None):
    """
    Wait for node ledger to become equal

    :param referenceNode: node whose ledger used as a reference
    """

    numOfNodes = len(otherNodes) + 1
    timeout = customTimeout or waits.expectedPoolGetReadyTimeout(numOfNodes)
    kwargs = {'exclude_from_check': exclude_from_check}
    looper.run(eventually(partial(checkNodeDataForEquality, **kwargs),
                          referenceNode,
                          *otherNodes,
                          retryWait=1, timeout=timeout))


def waitNodeDataInequality(looper,
                           referenceNode: TestNode,
                           *otherNodes: TestNode,
                           exclude_from_check=None,
                           customTimeout=None):
    """
    Wait for node ledger to become equal

    :param referenceNode: node whose ledger used as a reference
    """

    numOfNodes = len(otherNodes) + 1
    timeout = customTimeout or waits.expectedPoolGetReadyTimeout(numOfNodes)
    kwargs = {'exclude_from_check': exclude_from_check}
    looper.run(eventually(partial(checkNodeDataForInequality, **kwargs),
                          referenceNode,
                          *otherNodes,
                          retryWait=1, timeout=timeout))


def ensure_all_nodes_have_same_data(looper, nodes, custom_timeout=None,
                                    exclude_from_check=None):
    node = next(iter(nodes))
    other_nodes = [n for n in nodes if n != node]
    waitNodeDataEquality(looper, node, *other_nodes,
                         customTimeout=custom_timeout,
                         exclude_from_check=exclude_from_check)


def ensureNewNodeConnectedClient(looper, client: TestClient, node: TestNode):
    stackParams = node.clientStackParams
    client.nodeReg[stackParams['name']] = HA('127.0.0.1', stackParams['ha'][1])
    looper.run(client.ensureConnectedToNodes())


def checkClientPoolLedgerSameAsNodes(client: TestClient,
                                     *nodes: TestNode):
    for n in nodes:
        checkLedgerEquality(client.ledger, n.poolLedger)


def ensureClientConnectedToNodesAndPoolLedgerSame(looper,
                                                  client: TestClient,
                                                  *nodes: TestNode):
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

def make_a_node_catchup_less(target_node, other_nodes, ledger_id, shorten_by):
    """
    All `other_nodes` make the `node` catchup multiple times by serving
    consistency proof of a ledger smaller by `shorten_by` txns
    """
    orig_methods = {}
    for node in other_nodes:
        node.catchup_twice = True
        orig_methods[node.name] = node.ledgerManager._buildConsistencyProof

        def patched_method(self, ledgerId, seqNoStart, seqNoEnd):
            if self.owner.catchup_twice:
                import inspect
                curframe = inspect.currentframe()
                calframe = inspect.getouterframes(curframe, 2)
                # For domain ledger, send a proof for a small ledger to the bad
                # node
                if calframe[1][
                    3] == node.ledgerManager.getConsistencyProof.__name__ \
                        and calframe[2].frame.f_locals['frm'] == target_node.name \
                        and ledgerId == ledger_id:
                    logger.debug('{} sending a proof to {} for {} instead '
                                 'of {}'.format(self.owner.name, target_node.name,
                                                seqNoEnd - shorten_by, seqNoEnd))
                    return orig_methods[node.name](ledgerId, seqNoStart,
                                                   seqNoEnd - shorten_by)
            return orig_methods[node.name](ledgerId, seqNoStart, seqNoEnd)

        node.ledgerManager._buildConsistencyProof = types.MethodType(
            patched_method, node.ledgerManager)


def repair_node_catchup_less(other_nodes):
    for node in other_nodes:
        node.catchup_twice = False
