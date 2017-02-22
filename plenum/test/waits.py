import math

from plenum.common.log import getlogger
from plenum.common.config_util import getConfig

logger = getlogger()


def totalConnections(nodeCount: int) -> int:
    return math.ceil((nodeCount * (nodeCount - 1)) / 2)


def _expectedNodeInterconnectionTime(count):
    conf = getConfig()
    return count * conf.ExpectedConnectTime + 4


def expectedNodeInterconnectionTime(nodeCount):
    c = totalConnections(nodeCount)
    t = _expectedNodeInterconnectionTime(c)
    logger.debug("wait time for {} nodes and {} connections is {}"
                 .format(nodeCount, c, t))
    return t
