import pytest

from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit, Checkpoint
from plenum.test.spy_helpers import getAllReturnVals
from stp_core.common.log import getlogger
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_last_3pc_master
from plenum.test.pool_transactions.helper import \
    sdk_add_new_steward_and_node, sdk_pool_refresh
from plenum.test.test_node import checkNodesConnected, getNonPrimaryReplicas


def whitelist():
    return ['got error while verifying message']


logger = getlogger()


@pytest.yield_fixture("module")
def sdk_node_created_after_some_txns(looper, testNodeClass, do_post_node_creation,
                                     sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward,
                                     txnPoolNodeSet, tdir, tconf, allPluginsPath, request):
    txnCount = getValueFromModule(request, "txnCount", 5)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              txnCount)
    new_steward_name = randomString()
    new_node_name = "Epsilon"
    new_steward_wallet_handle, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, new_node_name, tdir, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=True,
        do_post_node_creation=do_post_node_creation)
    sdk_pool_refresh(looper, sdk_pool_handle)
    yield looper, new_node, sdk_pool_handle, new_steward_wallet_handle


@pytest.yield_fixture("module")
def sdk_node_created_after_some_txns_not_started(looper, testNodeClass, do_post_node_creation,
                                     sdk_pool_handle, sdk_wallet_client, sdk_wallet_steward,
                                     txnPoolNodeSet, tdir, tconf, allPluginsPath, request):
    txnCount = getValueFromModule(request, "txnCount", 5)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              txnCount)
    new_steward_name = randomString()
    new_node_name = "Epsilon"
    new_steward_wallet_handle, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, new_node_name, tdir, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=False,
        do_post_node_creation=do_post_node_creation)
    sdk_pool_refresh(looper, sdk_pool_handle)
    yield looper, new_node, sdk_pool_handle, new_steward_wallet_handle


@pytest.fixture("module")
def sdk_node_set_with_node_added_after_some_txns(
        txnPoolNodeSet, sdk_node_created_after_some_txns):
    looper, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_created_after_some_txns
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    return looper, new_node, sdk_pool_handle, new_steward_wallet_handle


@pytest.fixture("module")
def sdk_new_node_caught_up(txnPoolNodeSet,
                           sdk_node_set_with_node_added_after_some_txns):
    looper, new_node, _, _ = sdk_node_set_with_node_added_after_some_txns
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])
    check_last_3pc_master(new_node, txnPoolNodeSet[:4])

    # Check if catchup done once
    catchup_done_once = True
    for li in new_node.ledgerManager.ledgerRegistry.values():
        catchup_done_once = catchup_done_once and (li.num_txns_caught_up > 0)

    if not catchup_done_once:
        # It might be the case that node has to do catchup again, in that case
        # check the return value of `num_txns_caught_up_in_last_catchup` to be
        # greater than 0

        assert max(
            getAllReturnVals(
                new_node,
                new_node.num_txns_caught_up_in_last_catchup)) > 0

    for li in new_node.ledgerManager.ledgerRegistry.values():
        assert not li.receivedCatchUpReplies
        assert not li.recvdCatchupRepliesFrm

    return new_node


@pytest.yield_fixture("module")
def poolAfterSomeTxns(
        looper,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client,
        request):
    txnCount = getValueFromModule(request, "txnCount", 5)
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              txnCount)
    yield looper, sdk_pool_handle, sdk_wallet_client


@pytest.fixture
def broken_node_and_others(txnPoolNodeSet):
    node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[-1].node
    other = [n for n in txnPoolNodeSet if n != node]

    def brokenSendToReplica(msg, frm):
        logger.warning(
            "{} is broken. 'sendToReplica' does nothing".format(node.name))

    node.nodeMsgRouter.extend(
        (
            (PrePrepare, brokenSendToReplica),
            (Prepare, brokenSendToReplica),
            (Commit, brokenSendToReplica),
            (Checkpoint, brokenSendToReplica),
        )
    )

    return node, other
