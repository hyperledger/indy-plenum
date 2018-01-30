from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected

CHK_FREQ = 5


def test_upper_bound_of_checkpoint_after_catchup_is_divisible_by_chk_freq(
        chkFreqPatched, looper, txnPoolNodeSet, steward1, stewardWallet,
        client1, wallet1, client1Connected, tdir, client_tdir, tconf,
        allPluginsPath):

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    _, _, epsilon = addNewStewardAndNode(looper, steward1, stewardWallet,
                                         'EpsilonSteward', 'Epsilon',
                                         tdir, client_tdir, tconf,
                                         allPluginsPath)
    txnPoolNodeSet.append(epsilon)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, epsilon, *txnPoolNodeSet[:-1])
    # Epsilon did not participate in ordering of the batch with EpsilonSteward
    # NYM transaction and the batch with Epsilon NODE transaction.
    # Epsilon got these transactions via catch-up.

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)

    for replica in epsilon.replicas:
        assert len(replica.checkpoints) == 1
        assert next(iter(replica.checkpoints)) == (7, 10)
