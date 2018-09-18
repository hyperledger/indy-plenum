from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Checkpoint
from plenum.common.txn_util import get_req_id
from plenum.test.helper import sdk_send_random_and_check


def set_checkpoint_faking(replica):
    old = replica.send

    def send(msg, stat=None):
        if isinstance(msg, Checkpoint):
            replica.inBox.append((msg, "Self1"))
            replica.inBox.append((msg, "Self2"))
        old(msg, stat)

    replica.send = send


def test_request_executed_once_and_without_failing_behind(tconf, looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_client):
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

    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            set_checkpoint_faking(replica)

    replies = sdk_send_random_and_check(looper, txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client,
                                        number_of_requests)

    expected = [get_req_id(reply["result"]) for _, reply in replies]
    for node in txnPoolNodeSet:
        real_ledger_state = [get_req_id(txn)
                             for _, txn in node.getLedger(DOMAIN_LEDGER_ID).getAllTxn()
                             if get_req_id(txn) is not None]
        assert expected == real_ledger_state
