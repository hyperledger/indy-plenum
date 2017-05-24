from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper

from plenum.test.delayers import delay_3pc_messages
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies


def test_chaotic(looper, txnPoolNodeSet, client1, wallet1,
                 client1Connected):
    """
    Once nodes start, introduce arbitrary delays so view change happens and
    elections happen, but now again after some requests primary loses
    connection and view change happens and this time election messages are
    received slowly but again elections complete and some more requests succeed.
    Now add a single node and see that it processes requests. Now add more nodes
    such that f changes and it still processes requests.
    This test would remain a WIP till some time, break it into multiple tests
    """
    min_delay = 3
    max_delay = 6

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 2)
    delay_3pc_messages(txnPoolNodeSet, 0, min_delay=min_delay,
                       max_delay=max_delay)

