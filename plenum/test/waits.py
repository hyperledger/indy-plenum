import math

from plenum.common.log import getlogger
from plenum.common.config_util import getConfig

logger = getlogger()


def expectedWaitDirect(count):
    conf = getConfig()
    return count * conf.ExpectedConnectTime + 4


def expectedWait(nodeCount):
    c = totalConnections(nodeCount)
    w = expectedWaitDirect(c)
    logger.debug("wait time for {} nodes and {} connections is {}".format(
            nodeCount, c, w))
    return w


def totalConnections(nodeCount: int) -> int:
    return math.ceil((nodeCount * (nodeCount - 1)) / 2)
