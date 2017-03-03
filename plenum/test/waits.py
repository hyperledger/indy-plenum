import math

from plenum.common.log import getlogger
from plenum.common.config_util import getConfig
from plenum.common.util import totalConnections
from plenum.common import util
logger = getlogger()


def _expectedNodeInterconnectionTime(count) -> float:
    conf = getConfig()
    return count * conf.ExpectedConnectTime + 4


def expectedNodeInterconnectionTime(nodeCount) -> float:
    c = totalConnections(nodeCount)
    t = _expectedNodeInterconnectionTime(c)
    logger.debug("wait time for {} nodes and {} connections is {}"
                 .format(nodeCount, c, t))
    return t


def expectedNodeToNodeMessageDeliveryTime() -> float:
    return 5


def expectedTransactionExecutionTime(nodeCount) -> float:
    # TODO: maybe it should depend on transaction type?
    # TODO: implement

    return int(7.5 * nodeCount)


def expectedCatchupTime() -> float:
    # TODO: implement
    pass


def expectedAgentCommunicationTime() -> float:
    # TODO: implement
    pass


def expectedClientRequestPropagationTime(nodeCount) -> float:
    # TODO: move 1.4 to config
    # Also what if we have own config for tests?
    return int(2.5 * nodeCount)


def expectedReqAckQuorumTime(nodeCount) -> float:
    return int(1.25 * nodeCount)


def expectedViewChangeTime(nodeCount) -> float:
    return int(0.75 * nodeCount)


def expectedOrderingTime(numInstances) -> float:
    # TODO: should not be less than one second
    return int(2.14 * numInstances)


def expectedElectionTimeout(nodeCount) -> float:
    return 15 + 2 * nodeCount


def expectedGetReadyTimeout(nodeCount) -> float:
    return 5 * nodeCount


def expectedClientConnectionTimeout(fVal) -> float:
    return 3 * fVal


def expectedPoolLedgerCheck(nodeCount) -> float:
    """
    Expected time required for checking that 'pool ledger' on nodes and client
    is the same
    """

    return 3 * nodeCount