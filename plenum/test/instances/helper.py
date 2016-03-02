from plenum.test.helper import getAllArgs, TestReplica


def recvdPrePrepare(replica: TestReplica):
    return getAllArgs(replica, TestReplica.processPrePrepare)


def sentPrepare(replica: TestReplica, viewNo: int = None, ppSeqNo: int = None):
    params = getAllArgs(replica,
                        TestReplica.doPrepare)
    return [param["pp"] for param in params
            if (viewNo is None or param["pp"].viewNo == viewNo) and
            (viewNo is None or param["pp"].viewNo == viewNo)]


def recvdPrepare(replica: TestReplica):
    return getAllArgs(replica, TestReplica.processPrepare)
