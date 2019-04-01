import types
from functools import partial

import pytest

from plenum.common.constants import AUDIT_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, \
    Checkpoint
from plenum.common.util import check_if_all_equal_in_list, getMaxFailures
from plenum.test import waits
from plenum.test.helper import checkLedgerEquality, checkStateEquality, \
    check_seqno_db_equality, assertEquality, check_last_ordered_3pc, check_primaries_equality, check_view_no, \
    check_last_ordered_3pc_backup
from plenum.test.test_node import TestNode
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


# TODO: This should just take an arbitrary number of nodes and check for their
#  ledgers to be equal
def checkNodeDataForEquality(node: TestNode,
                             *otherNodes: TestNode,
                             exclude_from_check=None,
                             include_in_check=None):
    def chk_ledger_and_state(first_node, second_node, ledger_id):
        checkLedgerEquality(first_node.getLedger(ledger_id),
                            second_node.getLedger(ledger_id))
        if not exclude_from_check or 'check_state' not in exclude_from_check:
            checkStateEquality(first_node.getState(ledger_id),
                               second_node.getState(ledger_id))

    # Checks for node's ledgers and state's to be equal
    check_audit_ledger = not exclude_from_check or ('check_audit' not in exclude_from_check)
    for n in otherNodes:
        if not exclude_from_check or 'check_last_ordered_3pc' not in exclude_from_check:
            check_last_ordered_3pc(node, n)
        else:
            logger.debug("Excluding check_last_ordered_3pc check")

        if include_in_check and 'check_last_ordered_3pc_backup' in include_in_check:
            check_last_ordered_3pc_backup(node, n)
        else:
            logger.debug("Excluding check_last_ordered_3pc_backup check")

        if not exclude_from_check or 'check_view_no' not in exclude_from_check:
            check_view_no(node, n)
        else:
            logger.debug("Excluding check_view_no check")

        if not exclude_from_check or 'check_seqno_db' not in exclude_from_check:
            check_seqno_db_equality(node.seqNoDB, n.seqNoDB)
        else:
            logger.debug("Excluding check_seqno_db_equality check")

        if not exclude_from_check or 'check_primaries' not in exclude_from_check:
            check_primaries_equality(node, n)
        else:
            logger.debug("Excluding check_primaries_equality check")

        for ledger_id in n.ledgerManager.ledgerRegistry:
            if not check_audit_ledger and ledger_id == AUDIT_LEDGER_ID:
                continue
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
                         exclude_from_check=None,
                         include_in_check=None):
    """
    Wait for node ledger to become equal

    :param referenceNode: node whose ledger used as a reference
    """

    numOfNodes = len(otherNodes) + 1
    timeout = customTimeout or waits.expectedPoolGetReadyTimeout(numOfNodes)
    kwargs = {'exclude_from_check': exclude_from_check,
              'include_in_check': include_in_check}
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


def check_ledger_state(node, ledger_id, ledger_state):
    assertEquality(node.ledgerManager._node_leecher._leechers[ledger_id].state, ledger_state)


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
        seeder = node.ledgerManager._node_seeder
        orig_methods[node.name] = seeder._build_consistency_proof

        def patched_method(self, ledgerId, seqNoStart, seqNoEnd):
            node_name = self._provider.node_name()
            if node_name in nodes_to_send_proof_of_small_ledger:
                import inspect
                curframe = inspect.currentframe()
                calframe = inspect.getouterframes(curframe, 2)
                # For domain ledger, send a proof for a small ledger to the bad
                # node
                if calframe[1][3] == seeder.process_ledger_status.__name__ \
                        and calframe[1].frame.f_locals['frm'] == target_node.name \
                        and ledgerId == ledger_id:
                    # Pop so this node name, so proof for smaller ledger is not
                    # served again
                    nodes_to_send_proof_of_small_ledger.remove(node_name)
                    logger.debug('{} sending a proof to {} for {} instead of {}'.
                                 format(node_name, target_node.name, seqNoEnd - shorten_by, seqNoEnd))
                    return orig_methods[node.name](ledgerId, seqNoStart, seqNoEnd - shorten_by)
            return orig_methods[node.name](ledgerId, seqNoStart, seqNoEnd)

        seeder._build_consistency_proof = types.MethodType(patched_method, seeder)


def make_a_node_catchup_less(target_node, other_nodes, ledger_id, shorten_by):
    """
    All `other_nodes` make the `node` catchup multiple times by serving
    consistency proof of a ledger smaller by `shorten_by` txns
    """
    orig_methods = {}
    for node in other_nodes:
        seeder = node.ledgerManager._node_seeder
        seeder.catchup_twice = True
        orig_methods[node.name] = seeder._build_consistency_proof

        def patched_method(self, ledgerId, seqNoStart, seqNoEnd):
            node_name = self._provider.node_name()
            if self.catchup_twice:
                import inspect
                curframe = inspect.currentframe()
                calframe = inspect.getouterframes(curframe, 2)
                # For domain ledger, send a proof for a small ledger to the bad
                # node
                if calframe[1][3] == seeder.process_ledger_status.__name__ \
                        and calframe[1].frame.f_locals['frm'] == target_node.name \
                        and ledgerId == ledger_id:
                    logger.info('{} sending a proof to {} for {} instead of {}'.
                                format(node_name, target_node.name, seqNoEnd - shorten_by, seqNoEnd))
                    return orig_methods[node.name](ledgerId, seqNoStart,
                                                   seqNoEnd - shorten_by)
            return orig_methods[node.name](ledgerId, seqNoStart, seqNoEnd)

        seeder._build_consistency_proof = types.MethodType(patched_method, seeder)


def repair_node_catchup_less(other_nodes):
    for node in other_nodes:
        seeder = node.ledgerManager._node_seeder
        seeder.catchup_twice = False


def repair_broken_node(node):
    node.nodeMsgRouter.extend(
        (
            (PrePrepare, node.sendToReplica),
            (Prepare, node.sendToReplica),
            (Commit, node.sendToReplica),
            (Checkpoint, node.sendToReplica),
        )
    )
    return node


def get_number_of_completed_catchups(node):
    return len(node.ledgerManager.spylog.getAll(node.ledgerManager._on_catchup_complete))
