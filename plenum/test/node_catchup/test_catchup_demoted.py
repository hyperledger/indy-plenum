import pytest

from plenum.common.util import hexToFriendly

from plenum.common.constants import VALIDATOR
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForInequality
from plenum.test.pool_transactions.helper import \
    sdk_send_update_node
from stp_core.common.log import getlogger

from plenum.test.node_catchup.conftest import whitelist

logger = getlogger()


@pytest.mark.skip(reason="INDY-1297. Node does not catch up on promotion anymore.")
def test_catch_up_after_demoted(
        txnPoolNodeSet,
        sdk_node_set_with_node_added_after_some_txns,
        sdk_wallet_client):
    logger.info(
        "1. add a new node after sending some txns and check that catch-up "
        "is done (the new node is up to date)")
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_set_with_node_added_after_some_txns
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])

    logger.info("2. turn the new node off (demote)")
    node_dest = hexToFriendly(new_node.nodestack.verhex)
    sdk_send_update_node(looper, new_steward_wallet_handle,
                         sdk_pool_handle,
                         node_dest, new_node.name,
                         None, None,
                         None, None,
                         services=[])

    logger.info("3. send more requests, "
                "so that the new node's state is outdated")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 5)
    checkNodeDataForInequality(new_node, *txnPoolNodeSet[:-1])

    logger.info("4. turn the new node on")
    sdk_send_update_node(looper, new_steward_wallet_handle,
                         sdk_pool_handle,
                         node_dest, new_node.name,
                         None, None,
                         None, None,
                         services=[VALIDATOR])

    logger.info("5. make sure catch-up is done "
                "(the new node is up to date again)")
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])

    logger.info("6. send more requests and make sure "
                "that the new node participates in processing them")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              new_steward_wallet_handle, 10)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
