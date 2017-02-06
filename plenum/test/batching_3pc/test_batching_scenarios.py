def testPrePrepareProcessedInOrder():
    """
    A non-primary receives PRE-PREPARE out of order, it receives with ppSeqNo 2
     earlier than it receives the one with ppSeqNo 1 but it stashes the one
     with ppSeqNo 2 and only unstashes it for processing once it has
     processed PRE-PREPARE with ppSeqNo 1
    :return:
    """
    pass
