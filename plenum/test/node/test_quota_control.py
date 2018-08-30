from plenum.server.quota_control import QuotaControl
from stp_zmq.zstack import Quota

MAX_REQUEST_QUEUE_LEN = 1000
MAX_NODE_QUOTA = Quota(count=100, size=1024*1024)
MAX_CLIENT_QUOTA = Quota(count=100, size=1024*1024)
ZERO_QUOTA = Quota(count=0, size=0)


def create_quota_control(dynamic: bool = True) -> QuotaControl:
    return QuotaControl(dynamic=dynamic,
                        max_request_queue_size=MAX_REQUEST_QUEUE_LEN,
                        max_node_quota=MAX_NODE_QUOTA,
                        max_client_quota=MAX_CLIENT_QUOTA)


def test_quota_control_gives_maximum_quotas_initially():
    qc = create_quota_control()
    assert qc.node_quota == MAX_NODE_QUOTA
    assert qc.client_quota == MAX_CLIENT_QUOTA


def test_quota_control_gives_maximum_quotas_when_request_queue_length_is_below_limit():
    qc = create_quota_control()
    qc.set_request_queue_len(MAX_REQUEST_QUEUE_LEN - 1)
    assert qc.node_quota == MAX_NODE_QUOTA
    assert qc.client_quota == MAX_CLIENT_QUOTA


def test_quote_control_gives_no_quota_for_client_when_queue_length_reaches_limit():
    qc = create_quota_control()
    qc.set_request_queue_len(MAX_REQUEST_QUEUE_LEN)
    assert qc.node_quota == MAX_NODE_QUOTA
    assert qc.client_quota == ZERO_QUOTA


def test_quote_control_restores_client_quotas_when_request_queue_length_drops_below_limit():
    qc = create_quota_control()
    qc.set_request_queue_len(MAX_REQUEST_QUEUE_LEN)
    qc.set_request_queue_len(MAX_REQUEST_QUEUE_LEN - 1)
    assert qc.node_quota == MAX_NODE_QUOTA
    assert qc.client_quota == MAX_CLIENT_QUOTA


def test_quota_control_gives_maximum_quotas_when_dynamic_control_is_disabled():
    qc = create_quota_control(dynamic=False)
    qc.set_request_queue_len(2 * MAX_REQUEST_QUEUE_LEN)
    assert qc.node_quota == MAX_NODE_QUOTA
    assert qc.client_quota == MAX_CLIENT_QUOTA
