from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestReplica


def recvdPrePrepare(replica: TestReplica):
    return getAllArgs(replica, TestReplica.processPrePrepare)


def processedPrePrepare(replica: TestReplica):
    return getAllArgs(replica, TestReplica.addToPrePrepares)


def sentPrepare(replica: TestReplica, viewNo: int = None, ppSeqNo: int = None):
    params = getAllArgs(replica,
                        TestReplica.doPrepare)
    return [param["pp"] for param in params
            if (viewNo is None or param["pp"].viewNo == viewNo) and
            (viewNo is None or param["pp"].viewNo == viewNo)]


def recvdPrepare(replica: TestReplica):
    return getAllArgs(replica, TestReplica.processPrepare)
