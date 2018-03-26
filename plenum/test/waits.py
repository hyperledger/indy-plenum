from plenum.server.quorums import Quorums

from stp_core.common.log import getlogger
from plenum.common.config_util import getConfig
from plenum.common.util import totalConnections
from plenum.config import CLIENT_REQACK_TIMEOUT, CLIENT_REPLY_TIMEOUT

logger = getlogger()

# Peer (node/client) to peer message delivery time
__Peer2PeerRequestDeliveryTime = 0.5
__Peer2PeerRequestExchangeTime = 2 * __Peer2PeerRequestDeliveryTime

# It's expected what the Node will start in one second
__NodeStartUpTime = 1

# The Instance order time
__ProtocolInstanceOrderTime = 1

# Time from replied to persisted in ledger
__PersistRepliedTime = 1


#########################
# The Node timeouts
#########################

def expectedNodeStartUpTimeout():
    """
    From: The Node is not raised
    To: The Node is ready to connect
    """
    return __NodeStartUpTime


def expectedNodeToNodeMessageDeliveryTime():
    """
    From: The Node ready to send a message
    To: The message is received other Node
    """
    return __Peer2PeerRequestDeliveryTime


def expectedNodeToAllNodesMessageDeliveryTime(nodeCount):
    """
    From: The Node ready to send a message
    To: The message is received by all other Nodes
    """
    return expectedNodeToNodeMessageDeliveryTime() * (nodeCount - 1)


#########################
# Pool internal timeouts
#########################

def expectedPoolStartUpTimeout(nodeCount):
    """
    From: the Pool is not raised
    To: the Pool is ready to connect
    """
    return nodeCount * expectedNodeStartUpTimeout()


def expectedPoolInterconnectionTime(nodeCount):
    """
    From: the Pool up
    To: the Pool is fully connected
    """
    config = getConfig()
    interconnectionCount = totalConnections(nodeCount)
    nodeConnectionTimeout = config.ExpectedConnectTime
    # '+KITZStack.RETRY_TIMEOUT_RESTRICTED' is a workaround for
    # bug (`'str' object has no attribute 'keys'`) which supposed to be
    # fixed in the 3pcbatch feature
    # https://evernym.atlassian.net/browse/SOV-995
    # TODO check actual state
    # multiply by 2 because we need to re-create connections which can be done on a second re-try only
    # (we may send pings on some of the re-tries)
    return min(0.8 * config.TestRunningTimeLimitSec,
               interconnectionCount * nodeConnectionTimeout +
               2 * config.RETRY_TIMEOUT_RESTRICTED + 2)


def expectedPoolDisconnectionTime(nodeCount):
    return __Peer2PeerRequestDeliveryTime * nodeCount


def expectedPoolConsistencyProof(nodeCount):
    """
    From: any time the Pool ready for the consistency proof procedure
    To: each of the Nodes finish the consistency proof procedure
        (ready for catchup if it is needed)
    """
    config = getConfig()
    nodeCPTimeout = __Peer2PeerRequestExchangeTime + \
                    config.ConsistencyProofsTimeout
    return nodeCount * nodeCPTimeout


def expectedPoolCatchupTime(nodeCount):
    """
    From: the consistency proof procedure is finished
    To: each of the Nodes finished the the catchup procedure
    """
    config = getConfig()
    return nodeCount * config.CatchupTransactionsTimeout


def expectedPoolGetReadyTimeout(nodeCount):
    """
    From: the Pool is disconnected
    To: the pool ledger is equal across the Nodes
    """
    return expectedPoolInterconnectionTime(nodeCount) + \
           expectedPoolConsistencyProof(nodeCount) + \
           expectedPoolCatchupTime(nodeCount)


def expectedPoolLedgerRepliedMsgPersisted(nodeCount):
    """
    From: a message is replied to client
    To: the message is stored in the ledger
    """
    return nodeCount * __PersistRepliedTime


#########################
# Pool election timeouts
#########################

def expectedPoolViewChangeStartedTimeout(nodeCount):
    """
    From: the VIEW_CHANGE is send
    To: the view is changed started (before NOMINATE)
    """
    interconnectionCount = totalConnections(nodeCount)
    return expectedNodeToNodeMessageDeliveryTime() * interconnectionCount


def expectedPoolNominationTimeout(nodeCount):
    """
    From: the NOMINATE is sent
    To: the NOMINATE is received by each node in the Pool
    """
    interconnectionCount = totalConnections(nodeCount)
    return expectedNodeToNodeMessageDeliveryTime() * interconnectionCount


def expectedPoolElectionTimeout(nodeCount, numOfReelections=0):
    """
    From: the Pool ready for the view change procedure
    To: the Pool changed the View
    """
    # not sure what nomination + primary is enough
    interconnectionCount = totalConnections(nodeCount)
    primarySelectTimeout = \
        expectedNodeToNodeMessageDeliveryTime() * interconnectionCount

    oneElectionTimeout = \
        expectedPoolViewChangeStartedTimeout(nodeCount) + \
        expectedPoolNominationTimeout(nodeCount) + \
        primarySelectTimeout

    return (1 + numOfReelections) * oneElectionTimeout


def expectedPoolNextPerfCheck(nodes):
    """
    From: any time
    To: the performance check is finished across the Pool
    """
    # +1 means 'wait awhile after max timeout'
    return max([n.perfCheckFreq for n in nodes]) + 1


#########################
# Processing timeouts
#########################


def expectedPropagateTime(nodeCount):
    """
    From: the Client sent the requests
    To: the requests are propageted
    """
    count = totalConnections(nodeCount)
    return expectedNodeToNodeMessageDeliveryTime() * count


def expectedPrePrepareTime(nodeCount):
    """
    From: the requests are propageted
    To: the requests are pre-prepared
    """
    count = totalConnections(nodeCount)
    return expectedNodeToNodeMessageDeliveryTime() * count


def expectedPrepareTime(nodeCount):
    """
    From: the requests are pre-prepared
    To: the requests are prepared
    """
    count = totalConnections(nodeCount)
    return expectedNodeToNodeMessageDeliveryTime() * count


def expectedCommittedTime(nodeCount):
    """
    From: the requests are prepared
    To: the requests are committed
    """
    count = totalConnections(nodeCount)
    return expectedNodeToNodeMessageDeliveryTime() * count


def expectedOrderingTime(numInstances):
    """
    From: the requests are committed
    To: the requests are ordered
    """
    return __ProtocolInstanceOrderTime * numInstances


#########################
# Client timeouts
#########################


def expectedClientToPoolConnectionTimeout(nodeCount):
    """
    From: the Client is not connected to the Pool
    To: the Client is connected to the Pool
    """
    # '+KITZStack.RETRY_TIMEOUT_RESTRICTED' is a workaround for
    # bug (`'str' object has no attribute 'keys'`) which supposed to be
    # fixed in the 3pcbatch feature
    # https://evernym.atlassian.net/browse/SOV-995
    # TODO check actual state
    config = getConfig()
    return config.ExpectedConnectTime * nodeCount + \
           config.RETRY_TIMEOUT_RESTRICTED


def expectedClientConsistencyProof(nodeCount):
    """
    From: the Client is connected to the Pool
    To: the Client finished the consistency proof procedure
    """
    config = getConfig()
    qN = Quorums(nodeCount).commit.value
    return qN * __Peer2PeerRequestExchangeTime + \
           config.ConsistencyProofsTimeout


def expectedClientCatchupTime(nodeCount):
    """
    From: the Client finished the consistency proof procedure
    To: the Client finished the catchup procedure
    """
    config = getConfig()
    qN = Quorums(nodeCount).commit.value
    return qN * 2 * __Peer2PeerRequestExchangeTime + \
           config.CatchupTransactionsTimeout


def expectedClientToPoolRequestDeliveryTime(nodeCount):
    """
    From: the Client send a request
    To: the request is delivered to f nodes
    """
    qN = Quorums(nodeCount).commit.value
    return __Peer2PeerRequestExchangeTime * qN


def expectedClientRequestPropagationTime(nodeCount):
    """
    From: The requests are sent
    To: The Propagation procedure finish
    """
    return expectedPropagateTime(nodeCount)


def expectedTransactionExecutionTime(nodeCount):
    # QUESTION: Why is the expected execution time a multiple of
    # CLIENT_REPLY_TIMEOUT, its huge,
    # it should be a little less than CLIENT_REPLY_TIMEOUT
    # return int(CLIENT_REPLY_TIMEOUT * nodeCount)
    return CLIENT_REPLY_TIMEOUT * 0.25 * nodeCount


def expectedReqAckQuorumTime():
    # TODO depends from nodeCount
    return CLIENT_REQACK_TIMEOUT


def expectedReqNAckQuorumTime():
    # TODO depends from nodeCount
    return CLIENT_REQACK_TIMEOUT


def expectedReqRejectQuorumTime():
    # TODO depends from nodeCount
    return CLIENT_REQACK_TIMEOUT


#########################
# Agent timeouts
#########################

def expectedAgentCommunicationTime():
    # TODO: implement if it is needed
    raise NotImplementedError()
