def testAllDependentBatchesDiscardedAfterBatchReject():
    """
    Check if a batch if rejected, each batch that was created based on the
    rejected batch is discarded.
    :return:
    """


def testTreeStateRevertedAfterBatchRejection():
    """"
    After a batch is rejected, all nodes revert their trees to last known
    correct state
    """


def testDiscardedBatchesRetried():
    """"
    After a batch is rejected and each batch that was created based on the
    rejected batch is discarded, the discarded batches are tried again
    """


def testMoreBatchesWillBeSentAfterDiscardedBatchesAreRetired():
    """
    After retrying discarded batches, new batches are sent
    :return:
    """
