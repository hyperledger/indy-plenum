from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Checkpoint
from plenum.test.helper import \
    send_signed_requests, \
    waitForSufficientRepliesForRequests, \
    random_requests


def set_checkpoint_faking(replica):
    old = replica.send

    def send(msg, stat=None):
        if isinstance(msg, Checkpoint):
            replica.inBox.append((msg, "Self1"))
            replica.inBox.append((msg, "Self2"))
        old(msg, stat)

    replica.send = send


def test_request_executed_once_and_without_failing_behind(tconf, looper,
                                                          nodeSet, client1,
                                                          wallet1):
    """
    Checks that all requests executed only once and without failing behind in
    wrote transactions

    1. Send request
    2. When it is ordered - send checkpoint to replica to force it
    make garbage collection
    3. Check that ordered request executed and client receives reply
    4. Check that ledgers on all nodes are the same and contain all
    expected transactions
    """

    number_of_requests = 5
    tconf.CHK_FREQ = 1

    for node in nodeSet:
        for replica in node.replicas:
            set_checkpoint_faking(replica)

    requests = [wallet1.signOp(req)
                for req in random_requests(number_of_requests)]

    for request in requests:
        send_signed_requests(client1, [request])
        waitForSufficientRepliesForRequests(looper,
                                            client1,
                                            requests=[request])

    expected = [request.reqId for request in requests]
    for node in nodeSet:
        real_ledger_state = [txn[1]["reqId"]
                             for txn in node.getLedger(DOMAIN_LEDGER_ID).getAllTxn()]
        assert expected == real_ledger_state
