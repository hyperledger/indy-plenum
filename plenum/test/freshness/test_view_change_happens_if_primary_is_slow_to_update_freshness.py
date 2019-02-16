import pytest

from plenum.test.freshness.helper import has_freshness_instance_change
from plenum.test.helper import freshness
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def test_view_change_happens_if_primary_is_slow_to_update_freshness(looper, tconf, txnPoolNodeSet,
                                                                    sdk_wallet_client, sdk_pool_handle,
                                                                    monkeypatch):
    monkeypatch.setattr(txnPoolNodeSet[0].master_replica._freshness_checker,
                        'freshness_timeout', 3 * FRESHNESS_TIMEOUT)

    current_view_no = txnPoolNodeSet[0].viewNo
    for node in txnPoolNodeSet:
        assert node.viewNo == current_view_no

    def check_next_view():
        for node in txnPoolNodeSet:
            assert node.viewNo > current_view_no

    looper.run(eventually(check_next_view, timeout=5 * FRESHNESS_TIMEOUT))

    assert sum(1 for node in txnPoolNodeSet if has_freshness_instance_change(node)) >= 3

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
