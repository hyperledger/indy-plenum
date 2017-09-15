from plenum.test.waits import expectedPoolGetReadyTimeout
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test.helper import send_reqs_to_nodes_and_verify_all_replies, sendRandomRequests
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected


logger = getlogger()


def test_node_requests_missing_three_phase_messages(looper, txnPoolNodeSet, wallet1, client1Connected):
    """
    2 of 4 nodes go down, so pool can not process any more incoming requests.
    A new request comes in. After a while those 2 nodes come back alive.
    Another request comes in. Check that previously disconnected two nodes
    request missing PREPARES and PREPREPARES and the pool successfully handles
    both transactions after that.
    """
    INIT_REQS_CNT = 10
    MISSING_REQS_CNT = 1
    REQS_AFTER_RECONNECT_CNT = 1
    disconnected_nodes = txnPoolNodeSet[2:]
    alive_nodes = txnPoolNodeSet[:2]

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1Connected, INIT_REQS_CNT)
    waitNodeDataEquality(looper, disconnected_nodes[0], *txnPoolNodeSet[:-1])

    init_ledger_size = txnPoolNodeSet[0].domainLedger.size

    for node in disconnected_nodes:
        disconnect_node_and_ensure_disconnected(looper, txnPoolNodeSet, node, stopNode=False)

    sendRandomRequests(wallet1, client1Connected, MISSING_REQS_CNT)

    def check_pp_out_of_sync(alive_nodes, disconnected_nodes):

        def get_last_pp(node):
            return node.replicas._master_replica.lastPrePrepare

        last_3pc_key_alive = get_last_pp(alive_nodes[0])
        for node in alive_nodes[1:]:
            assert get_last_pp(node) == last_3pc_key_alive

        last_3pc_key_diconnected = get_last_pp(disconnected_nodes[0])
        assert last_3pc_key_diconnected != last_3pc_key_alive
        for node in disconnected_nodes[1:]:
            assert get_last_pp(node) == last_3pc_key_diconnected

    looper.run(eventually(check_pp_out_of_sync,
                          alive_nodes,
                          disconnected_nodes,
                          retryWait=1,
                          timeout=expectedPoolGetReadyTimeout(
                              len(txnPoolNodeSet))))

    for node in disconnected_nodes:
        reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, node)

    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client1Connected, REQS_AFTER_RECONNECT_CNT)
    waitNodeDataEquality(looper, disconnected_nodes[0], *txnPoolNodeSet[:-1])

    for node in txnPoolNodeSet:
        assert node.domainLedger.size == (init_ledger_size + MISSING_REQS_CNT + REQS_AFTER_RECONNECT_CNT)
