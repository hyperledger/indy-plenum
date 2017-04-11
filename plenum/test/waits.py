from stp_core.common.log import getlogger
from plenum.common.config_util import getConfig
from plenum.common.util import totalConnections
from plenum.config import CLIENT_REQACK_TIMEOUT, CLIENT_REPLY_TIMEOUT

logger = getlogger()
config = getConfig()


#########################
# Pool internal timeouts
#########################


def expectedNodeInterconnectionTime(nodeCount):
    count = totalConnections(nodeCount)
    return count * config.ExpectedConnectTime


def expectedCatchupTime(nodeCount, customConsistencyProofsTimeout=None):
    timeout = customConsistencyProofsTimeout or config.ConsistencyProofsTimeout
    return timeout * nodeCount


def expectedPoolGetReadyTimeout(nodeCount):
    # looks the same with catchup process
    return expectedCatchupTime(nodeCount)


def expectedPoolLedgerCheck(nodeCount):
    """
    Expected time required for checking that 'pool ledger' on nodes and client
    is the same
    """
    return 5 * nodeCount


def expectedNodeStartUpTimeout():
    return 5


def expectedRequestStashingTime():
    return 20


#########################
# Pool election timeouts
#########################

def expectedNominationTimeout(nodeCount):
    return 3 * nodeCount


def expectedElectionTimeout(nodeCount):
    return expectedNominationTimeout(nodeCount) + 4 * nodeCount


def expectedNextPerfCheck(nodes):
    return max([n.perfCheckFreq for n in nodes]) + 1


def expectedViewChangeTime(nodeCount):
    return int(0.75 * nodeCount)


#########################
# Processing timeouts
#########################

def expectedNodeToNodeMessageDeliveryTime():
    return 5


def expectedPropagateTime(nodeCount):
    return expectedNodeToNodeMessageDeliveryTime() * nodeCount


def expectedPrePrepareTime(nodeCount):
    return expectedNodeToNodeMessageDeliveryTime() * nodeCount


def expectedOrderingTime(numInstances):
    return int(2.14 * numInstances)


#########################
# Client timeouts
#########################

def expectedClientConnectionTimeout(fVal):
    return 3 * fVal


def expectedClientRequestPropagationTime(nodeCount):
    return int(2.5 * nodeCount)


def expectedTransactionExecutionTime(nodeCount):
    return int(CLIENT_REPLY_TIMEOUT * nodeCount)


def expectedReqAckQuorumTime():
    return CLIENT_REQACK_TIMEOUT


def expectedReqNAckQuorumTime():
    return CLIENT_REQACK_TIMEOUT


#########################
# Agent timeouts
#########################

def expectedAgentCommunicationTime():
    # TODO: implement if it is needed
    raise NotImplementedError()

