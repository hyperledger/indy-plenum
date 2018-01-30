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


def test_dirty_read(looper, nodeSet, client1, wallet1):
    """
    Tests the case when read request comes before write request is
    not executed on some nodes
    """

    slow_nodes = list(nodeSet)[2:4]
    for node in slow_nodes:
        logger.debug("Making node {} slow".format(node))
        make_node_slow(node)

    set_request = sendReqsToNodesAndVerifySuffReplies(looper,
                                                      wallet1,
                                                      client1,
                                                      numReqs=1)[0]

    received_replies = getRepliesFromClientInbox(inbox=client1.inBox,
                                                reqId=set_request.reqId)

    seq_no = received_replies[0]["result"]["seqNo"]
    get_request = [wallet1.signOp({
        TXN_TYPE: GET_TXN,
        f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
        DATA: seq_no
    })]
    send_signed_requests(client1, get_request)
    waitForSufficientRepliesForRequests(looper,
                                        client1,
                                        requests=get_request)
    received_replies = getRepliesFromClientInbox(inbox=client1.inBox,
                                                 reqId=get_request[0].reqId)
    results = [str(reply['result'][DATA]) for reply in received_replies]

    assert len(set(results)) == 1
