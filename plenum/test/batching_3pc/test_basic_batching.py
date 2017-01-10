def testRequestStaticValidation():
    """
    Check that for requests which fail static validation, REQNACK is sent
    :return:
    """
    pass


def test3PCOverBatchWithThresholdReqs():
    """
    Check that 3 phase commit happens when threshold number of requests are
    received and propagated.
    :return:
    """
    pass


def test3PCOverBatchWithLessThanThresholdReqs():
    """
    Check that 3 phase commit happens when threshold number of requests are
    not received but threshold time has passed
    :return:
    """
    pass


def testTreeRootsCorrectAfterEachBatch():
    """
    Check if both state root and txn tree root are correct and same on each
    node after each batch
    :return:
    """


def testRequestDynamicValidation():
    """
    Check that for requests which fail dynamic (state based) validation,
    REJECT is sent to the client
    :return:
    """
    pass
