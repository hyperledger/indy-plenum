from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import addNewSteward, sendAddNewNode
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional
from plenum.test.test_node import checkProtocolInstanceSetup
from plenum.test.view_change.helper import ensure_view_change


from plenum.test.conftest import tdirWithPoolTxns
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper, nodeThetaAdded, \
    stewardAndWallet1, steward1, stewardWallet
from plenum.test.primary_selection.conftest import one_node_added
from plenum.test.batching_3pc.conftest import tconf


def test_different_ledger_request_interleave(tconf, looper, txnPoolNodeSet,
                                             client1, wallet1, one_node_added,
                                             client1Connected,
                                             tdirWithPoolTxns, steward1,
                                             stewardWallet, allPluginsPath):
    """
    Send pool and domain ledger requests such that they interleave, and do
    view change in between and verify the pool is functional
    """
    new_node = one_node_added
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)

    # Send domain ledger requests but don't wait for replies
    requests = sendRandomRequests(wallet1, client1, 2)
    # Add another node by sending pool ledger request
    _, _, new_theta = nodeThetaAdded(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                     tconf, steward1, stewardWallet,
                                     allPluginsPath, name='new_theta')

    # Send more domain ledger requests but don't wait for replies
    requests.extend(sendRandomRequests(wallet1, client1, 3))

    # Do view change without waiting for replies
    ensure_view_change(looper, nodes=txnPoolNodeSet)
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)

    # Make sure all requests are completed
    waitForSufficientRepliesForRequests(looper, client1,
                                        requests=requests)

    ensure_pool_functional(looper, txnPoolNodeSet, wallet1, client1)

    new_steward, new_steward_wallet = addNewSteward(looper, tdirWithPoolTxns,
                                                    steward1, stewardWallet,
                                                    'another_ste')

    # Send another pool ledger request (NODE) but don't wait for completion of
    # request
    next_node_name = 'next_node'
    r = sendAddNewNode(tdirWithPoolTxns, next_node_name, new_steward, new_steward_wallet)
    node_req = r[0]

    # Send more domain ledger requests but don't wait for replies
    requests = [node_req, *
                sendRandomRequests(new_steward_wallet, new_steward, 5)]

    # Make sure all requests are completed
    waitForSufficientRepliesForRequests(looper, new_steward,
                                        requests=requests)

    # Make sure pool is functional
    ensure_pool_functional(looper, txnPoolNodeSet, wallet1, client1)
