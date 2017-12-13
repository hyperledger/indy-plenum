from plenum.test.spy_helpers import getAllArgs
from plenum.test.test_node import TestReplica


def recvd_pre_prepares(replica: TestReplica):
    return [arg['pre_prepare']
            for arg in getAllArgs(replica, TestReplica.processPrePrepare)]


def processedPrePrepare(replica: TestReplica):
    return getAllArgs(replica, TestReplica.addToPrePrepares)


def sentPrepare(replica: TestReplica, viewNo: int = None, ppSeqNo: int = None):
    params = getAllArgs(replica,
                        TestReplica.doPrepare)
    return [param["pp"] for param in params
            if (viewNo is None or param["pp"].viewNo == viewNo) and
            (viewNo is None or param["pp"].viewNo == viewNo)]


def recvd_prepares(replica: TestReplica):
    return [arg['prepare']
            for arg in getAllArgs(replica, TestReplica.processPrepare)]
