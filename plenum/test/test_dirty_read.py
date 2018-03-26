from plenum.common.types import f
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    getRepliesFromClientInbox, send_signed_requests, \
    waitForSufficientRepliesForRequests
from plenum.common.constants import GET_TXN, DATA, TXN_TYPE, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Ordered
from stp_core.common.log import getlogger

logger = getlogger()


def make_node_slow(node):
    old = node.serviceReplicas

    async def serviceReplicas(limit):
        for replica in node.replicas:
            for index, message in enumerate(list(replica.outBox)):
                if isinstance(message, Ordered):
                    del replica.outBox[index]
        return await old(limit)

    node.serviceReplicas = serviceReplicas
