import pytest

from plenum.server.view_change.view_changer import ViewChanger

from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, stewardAndWallet1, steward1, \
    stewardWallet

from stp_core.common.log import getlogger
from plenum.test.pool_transactions.helper import start_not_added_node, add_started_node

logger = getlogger()


@pytest.fixture(scope="module", autouse=True)
def tconf(tconf):
    old_vc_timeout = tconf.VIEW_CHANGE_TIMEOUT
    tconf.VIEW_CHANGE_TIMEOUT = 5
    yield tconf
    tconf.VIEW_CHANGE_TIMEOUT = old_vc_timeout


def test_no_instance_change_on_primary_disconnection_for_not_ready_node(
        looper, txnPoolNodeSet, tdir, tconf,
        allPluginsPath, steward1, stewardWallet,
        client_tdir):
    """
    Test steps:
    1. create a new node, but don't add it to the pool (so not send NODE txn), so that the node is not ready.
    2. wait for more than VIEW_CHANGE_TIMEOUT (a timeout for initial check for disconnected primary)
    3. make sure no InstanceChange sent by the new node
    4. add the node to the pool (send NODE txn) and make sure that the node is ready now.
    5. wait for more than VIEW_CHANGE_TIMEOUT (a timeout for initial check for disconnected primary)
    6. make sure no InstanceChange sent by the new node
    """

    # 1. create a new node, but don't add it to the pool (so not send NODE txn), so that the node is not ready.
    sigseed, bls_key, new_node, node_ha, client_ha = \
        start_not_added_node(looper,
                             tdir, tconf, allPluginsPath,
                             "TestTheta")

    # 2. wait for more than VIEW_CHANGE_TIMEOUT (a timeout for initial check for disconnected primary)
    looper.runFor(tconf.VIEW_CHANGE_TIMEOUT + 2)

    # 3. make sure no InstanceChange sent by the new node
    assert 0 == new_node.view_changer.spylog.count(ViewChanger.sendInstanceChange.__name__)

    # 4. add the node to the pool (send NODE txn) and make sure that the node is ready now.
    add_started_node(looper,
                     new_node,
                     node_ha,
                     client_ha,
                     txnPoolNodeSet,
                     client_tdir,
                     steward1, stewardWallet,
                     sigseed, bls_key)

    # 5. wait for more than VIEW_CHANGE_TIMEOUT (a timeout for initial check for disconnected primary)
    looper.runFor(tconf.VIEW_CHANGE_TIMEOUT + 2)

    # 6. make sure no InstanceChange sent by the new node
    assert 0 == new_node.view_changer.spylog.count(ViewChanger.sendInstanceChange.__name__)
