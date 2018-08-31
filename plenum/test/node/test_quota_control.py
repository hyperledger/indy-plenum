import pytest

from plenum.server.quota_control import QuotaControl, StaticQuotaControl, RequestQueueQuotaControl
from stp_zmq.zstack import Quota

MAX_REQUEST_QUEUE_SIZE = 1000
MAX_NODE_QUOTA = Quota(count=100, size=1024 * 1024)
MAX_CLIENT_QUOTA = Quota(count=100, size=1024 * 1024)
ZERO_QUOTA = Quota(count=0, size=0)


@pytest.fixture()
def static_qc():
    return StaticQuotaControl(node_quota=MAX_NODE_QUOTA, client_quota=MAX_CLIENT_QUOTA)


@pytest.fixture()
def request_queue_qc():
    return RequestQueueQuotaControl(max_request_queue_size=MAX_REQUEST_QUEUE_SIZE,
                                    max_node_quota=MAX_NODE_QUOTA,
                                    max_client_quota=MAX_CLIENT_QUOTA)


def test_static_quota_control_gives_maximum_quotas_initially(static_qc):
    assert static_qc.node_quota == MAX_NODE_QUOTA
    assert static_qc.client_quota == MAX_CLIENT_QUOTA


def test_static_quota_control_always_gives_maximum_quotas(static_qc):
    static_qc.update_state({'request_queue_size': 2 * MAX_REQUEST_QUEUE_SIZE})
    assert static_qc.node_quota == MAX_NODE_QUOTA
    assert static_qc.client_quota == MAX_CLIENT_QUOTA


def test_request_queue_quota_control_gives_maximum_quotas_initially(request_queue_qc):
    assert request_queue_qc.node_quota == MAX_NODE_QUOTA
    assert request_queue_qc.client_quota == MAX_CLIENT_QUOTA


def test_request_queue_quota_control_gives_maximum_quotas_when_request_queue_size_is_below_limit(request_queue_qc):
    request_queue_qc.update_state({'request_queue_size': MAX_REQUEST_QUEUE_SIZE - 1})
    assert request_queue_qc.node_quota == MAX_NODE_QUOTA
    assert request_queue_qc.client_quota == MAX_CLIENT_QUOTA


def test_request_queue_quota_control_gives_no_quota_for_client_when_queue_size_reaches_limit(request_queue_qc):
    request_queue_qc.update_state({'request_queue_size': MAX_REQUEST_QUEUE_SIZE})
    assert request_queue_qc.node_quota == MAX_NODE_QUOTA
    assert request_queue_qc.client_quota == ZERO_QUOTA


def test_request_queue_quota_control_restores_client_quotas_when_request_queue_size_drops_below_limit(request_queue_qc):
    request_queue_qc.update_state({'request_queue_size': MAX_REQUEST_QUEUE_SIZE})
    request_queue_qc.update_state({'request_queue_size': MAX_REQUEST_QUEUE_SIZE - 1})
    assert request_queue_qc.node_quota == MAX_NODE_QUOTA
    assert request_queue_qc.client_quota == MAX_CLIENT_QUOTA
