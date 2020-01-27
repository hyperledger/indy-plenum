import pytest

from plenum.test.helper import view_change_timeout
from plenum.test.view_change.helper import node_sent_instance_changes_count

from stp_core.common.log import getlogger
from plenum.test.pool_transactions.helper import start_not_added_node, add_started_node

logger = getlogger()


@pytest.fixture(scope="module", autouse=True)
def tconf(tconf):
    with view_change_timeout(tconf, 10):
        yield tconf


def test_no_instance_change_on_primary_disconnection_for_not_ready_node(
        looper, txnPoolNodeSet, tdir, tconf,
        allPluginsPath, sdk_pool_handle, sdk_wallet_steward):
    """
    Test steps:
    1. create a new node, but don't add it to the pool (so not send NODE txn), so that the node is not ready.
    2. wait for more than NEW_VIEW_TIMEOUT (a timeout for initial check for disconnected primary)
    3. make sure no InstanceChange sent by the new node
    4. add the node to the pool (send NODE txn) and make sure that the node is ready now.
    5. wait for more than NEW_VIEW_TIMEOUT (a timeout for initial check for disconnected primary)
    6. make sure no InstanceChange sent by the new node
    """

    # 1. create a new node, but don't add it to the pool (so not send NODE txn), so that the node is not ready.
    sigseed, bls_key, new_node, node_ha, client_ha, key_proof = \
        start_not_added_node(looper,
                             tdir, tconf, allPluginsPath,
                             "TestTheta")

    # 2. wait for more than NEW_VIEW_TIMEOUT (a timeout for initial check for disconnected primary)
    looper.runFor(tconf.NEW_VIEW_TIMEOUT + 2)

    # 3. make sure no InstanceChange sent by the new node
    assert 0 == node_sent_instance_changes_count(new_node)

    logger.info("Start added node {}".format(new_node))

    # 4. add the node to the pool (send NODE txn) and make sure that the node is ready now.
    add_started_node(looper,
                     new_node,
                     node_ha,
                     client_ha,
                     txnPoolNodeSet,
                     sdk_pool_handle,
                     sdk_wallet_steward,
                     bls_key,
                     key_proof)

    # 5. wait for more than NEW_VIEW_TIMEOUT (a timeout for initial check for disconnected primary)
    looper.runFor(tconf.NEW_VIEW_TIMEOUT + 2)

    # 6. make sure no InstanceChange sent by the new node
    assert 0 == node_sent_instance_changes_count(new_node)
