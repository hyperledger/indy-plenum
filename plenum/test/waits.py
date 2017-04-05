from plenum.common.log import getlogger
from plenum.common.config_util import getConfig
from plenum.common.util import totalConnections


logger = getlogger()
config = getConfig()


#########################
# Pool internal timeouts
#########################


def expectedNodeInterconnectionTime(nodeCount) -> float:
    count = totalConnections(nodeCount)
    t = count * config.ExpectedConnectTime + 4
    return t


def expectedCatchupTime(customConsistencyProofsTimeout = None) -> float:
    timeout = customConsistencyProofsTimeout or config.ConsistencyProofsTimeout
    # TODO: implement
    return timeout + 30


def expectedPoolGetReadyTimeout(nodeCount) -> float:
    return 10 * nodeCount


def expectedPoolLedgerCheck(nodeCount) -> float:
    """
    Expected time required for checking that 'pool ledger' on nodes and client
    is the same
    """
    return 4 * nodeCount


def expectedNodeStartUpTimeout() -> float:
    return 5


def expectedRequestStashingTime() -> float:
    return 20


#########################
# Pool election timeouts
#########################

def expectedNominationTimeout(nodeCount) -> float:
    return nodeCount * 3


def expectedElectionTimeout(nodeCount) -> float:
    return expectedNominationTimeout(nodeCount) + 4 * nodeCount


def expectedNextPerfCheck(nodes) -> float:
    return max(n.perfCheckFreq for n in nodes) + 1


def expectedViewChangeTime(nodeCount) -> float:
    return int(0.75 * nodeCount)


#########################
# Processing timeouts
#########################

def expectedNodeToNodeMessageDeliveryTime() -> float:
    return 5


def expectedPrePrepareTime(nodeCount) -> float:
    # TODO: move 1.4 to config
    # Also what if we have own config for tests?
    return int(2.5 * nodeCount)


def expectedOrderingTime(numInstances) -> float:
    # TODO: should not be less than one second
    return int(2.14 * numInstances)


#########################
# Client timeouts
#########################

def expectedClientConnectionTimeout(fVal) -> float:
    return 3 * fVal


def expectedClientRequestPropagationTime(nodeCount) -> float:
    # TODO: move 1.4 to config
    # Also what if we have own config for tests?
    return int(2.5 * nodeCount)


def expectedTransactionExecutionTime(nodeCount) -> float:
    # TODO: maybe it should depend on transaction type?
    # TODO: implement
    # CLIENT_REPLY_TIMEOUT

    return int(7.5 * nodeCount)


def expectedReqAckQuorumTime(nodeCount) -> float:
    # CLIENT_REQACK_TIMEOUT
    return 5


def expectedReqNAckQuorumTime() -> float:
    return 5


#########################
# Agent timeouts
#########################

def expectedAgentCommunicationTime() -> float:
    # TODO: implement
    raise NotImplementedError()

