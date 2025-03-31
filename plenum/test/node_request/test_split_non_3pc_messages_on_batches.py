from plenum.test.helper import vdr_send_random_requests, vdr_eval_timeout, \
    vdr_get_and_check_replies
from stp_core.validators.message_length_validator import MessageLenValidator


def test_msg_max_length_check_node_to_node(looper,
                                           txnPoolNodeSet,
                                           vdr_pool_handle,
                                           vdr_wallet_client,
                                           vdr_wallet_client2):
    """
    Two clients send 2*N requests each at the same time.
    N < MSG_LEN_LIMIT but 2*N > MSG_LEN_LIMIT so the requests pass the max
    length check for client-node requests but do not pass the check
    for node-node requests.
    """
    N = 10
    # it is an empirical value for N random requests
    # it has to be adjusted if the world changed (see pydoc)
    max_len_limit = 3000

    patch_msg_len_validators(max_len_limit, txnPoolNodeSet)

    reqs1 = vdr_send_random_requests(looper, vdr_pool_handle, vdr_wallet_client, N)
    reqs2 = vdr_send_random_requests(looper, vdr_pool_handle, vdr_wallet_client2, N)

    total_timeout = vdr_eval_timeout(N, len(txnPoolNodeSet))
    vdr_get_and_check_replies(looper, reqs1, timeout=total_timeout)
    vdr_get_and_check_replies(looper, reqs2, timeout=total_timeout)


def patch_msg_len_validators(max_len_limit, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        assert hasattr(node.nodestack, 'msgLenVal')
        assert hasattr(node.nodestack, 'msg_len_val')
        node.nodestack.msgLenVal = MessageLenValidator(max_len_limit)
        node.nodestack.msg_len_val = MessageLenValidator(max_len_limit)
