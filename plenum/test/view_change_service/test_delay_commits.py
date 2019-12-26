import pytest

from plenum.test.view_change_service.helper import check_view_change_one_slow_node


@pytest.fixture(scope="module", params=[1])
def vc_counts(request):
    return request.param


@pytest.fixture(scope="module", params=[True])
def slow_node_is_next_primary(request):
    return request.param


def test_delay_commits_for_one_node(looper,
                                    txnPoolNodeSet,
                                    sdk_pool_handle,
                                    sdk_wallet_client,
                                    slow_node_is_next_primary,
                                    vc_counts):
    check_view_change_one_slow_node(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client,
                                    vc_counts=vc_counts, slow_node_is_next_primary=slow_node_is_next_primary,
                                    delay_commit=True,
                                    delay_pre_prepare=False)
