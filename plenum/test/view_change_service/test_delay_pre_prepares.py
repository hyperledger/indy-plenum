import pytest

from plenum.test.view_change_service.helper import check_view_change_one_slow_node


@pytest.fixture(scope="module", params=[1, 2])
def vc_counts(request):
    return request.param


@pytest.fixture(scope="module", params=[True, False])
def slow_node_is_next_primary(request):
    return request.param


def test_delay_pre_prepare_for_next_primary(looper,
                                            txnPoolNodeSet,
                                            vdr_pool_handle,
                                            vdr_wallet_client,
                                            slow_node_is_next_primary,
                                            vc_counts):
    check_view_change_one_slow_node(looper, txnPoolNodeSet, vdr_pool_handle, vdr_wallet_client,
                                    vc_counts=vc_counts, slow_node_is_next_primary=slow_node_is_next_primary,
                                    delay_commit=False,
                                    delay_pre_prepare=True)
