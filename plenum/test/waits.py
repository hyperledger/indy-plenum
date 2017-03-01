import math

from plenum.common.log import getlogger
from plenum.common.config_util import getConfig

logger = getlogger()


def totalConnections(nodeCount: int) -> int:
    # TODO: move to utils
    return math.ceil((nodeCount * (nodeCount - 1)) / 2)


def _expectedNodeInterconnectionTime(count) -> int:
    conf = getConfig()
    return count * conf.ExpectedConnectTime + 4


def expectedNodeInterconnectionTime(nodeCount) -> int:
    c = totalConnections(nodeCount)
    t = _expectedNodeInterconnectionTime(c)
    logger.debug("wait time for {} nodes and {} connections is {}"
                 .format(nodeCount, c, t))
    return t


def expectedTransactionExecutionTime(nodeCount) -> int:
    # TODO: maybe it should depend on transaction type?
    # TODO: implement

    return int(7.5 * nodeCount)


def expectedCatchupTime() -> int:
    # TODO: implement
    pass


def expectedAgentCommunicationTime() -> int:
    # TODO: implement
    pass


def expectedClientRequestPropagationTime(nodeCount) -> int:
    # TODO: move 1.4 to config
    # Also what if we have own config for tests?
    return int(2.5 * nodeCount)


def expectedReqAckQuorumTime(nodeCount) -> int:
    return int(1.25 * nodeCount)

def expectedViewChangeTime(nodeCount) -> int:
    return int(0.75 * nodeCount)

def expectedOrderingTime(numInstances) -> int:
    # TODO: should not be less than one second
    return int(2.14 * numInstances)
