from plenum.test import waits
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected

CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_second_checkpoint_after_catchup_can_be_stabilized(
        chkFreqPatched, looper, txnPoolNodeSet, steward1, stewardWallet,
        client1, wallet1, client1Connected, tdir, client_tdir, tconf,
        allPluginsPath):

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

    for replica in epsilon.replicas:
        assert len(replica.checkpoints) == 0

        assert replica.h == 2
        assert replica.H == 17

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 7)
    stabilization_timeout = \
        waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.runFor(stabilization_timeout)

    for replica in epsilon.replicas:
        assert len(replica.checkpoints) == 2
        keys_iter = iter(replica.checkpoints)

        assert next(keys_iter) == (3, 5)
        assert replica.checkpoints[3, 5].seqNo == 5
        assert replica.checkpoints[3, 5].digest is None
        assert replica.checkpoints[3, 5].isStable is False

        assert next(keys_iter) == (6, 10)
        assert replica.checkpoints[6, 10].seqNo == 9
        assert replica.checkpoints[6, 10].digest is None
        assert replica.checkpoints[6, 10].isStable is False

        assert replica.h == 2
        assert replica.H == 17

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    looper.runFor(stabilization_timeout)

    for replica in epsilon.replicas:
        assert len(replica.checkpoints) == 1
        keys_iter = iter(replica.checkpoints)

        assert next(keys_iter) == (6, 10)
        assert replica.checkpoints[6, 10].seqNo == 10
        assert replica.checkpoints[6, 10].digest is not None
        assert replica.checkpoints[6, 10].isStable is True

        assert replica.h == 10
        assert replica.H == 25
