import pytest

from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import checkNodeDataForEquality
from plenum.test.test_node import TestNode


@pytest.fixture(scope="module")
def testNodeClass(patchPluginManager):
    return TestNode


def exclude_from_consensus(node):
    def do_nothing(msg, sender):
        pass

    node.nodeMsgRouter.extend([
        (PrePrepare, do_nothing),
        (Prepare, do_nothing),
        (Commit, do_nothing)
    ])


def test_observer_node(txnPoolNodeSet,
                       looper,
                       sdk_pool_handle, sdk_wallet_client):
    '''
    Integration tests checking the full workflow between a real Node Observer and real Node Observables.
    '''
    # exclude Delta from consensus
    observer_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]
    exclude_from_consensus(observer_node)

    # add Delta as Observer to other nodes
    for node in other_nodes:
        node.add_observer(observer_node.name, ObserverSyncPolicyType.EACH_BATCH)

    # send requests, so that they will be propagated to Observer (Delta)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client,
                              60)

    # check that Delta is in sync with other Nodes.
    checkNodeDataForEquality(observer_node,
                             *other_nodes,
                             exclude_from_check=['check_last_ordered_3pc',
                                                 'check_last_ordered_3pc_backup'])
