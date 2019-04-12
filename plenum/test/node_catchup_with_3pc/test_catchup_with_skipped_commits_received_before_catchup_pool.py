import pytest

from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.helper import max_3pc_batch_limits
from plenum.test.node_catchup_with_3pc.helper import check_catchup_with_skipped_commits_received_before_catchup, \
    patched_out_of_order_commits_interval


@pytest.fixture(scope="module")
def tconf(tconf):
    with patched_out_of_order_commits_interval(tconf) as tconf:
        with max_3pc_batch_limits(tconf, size=1) as tconf:
            yield tconf


def test_catchup_with_skipped_commits_received_before_catchup_pool(tdir, tconf,
                                                                    looper,
                                                                    txnPoolNodeSet,
                                                                    sdk_pool_handle,
                                                                    sdk_wallet_new_steward):
    check_catchup_with_skipped_commits_received_before_catchup(NodeLeecherService.State.SyncingPool,
                                                               looper,
                                                               txnPoolNodeSet,
                                                               sdk_pool_handle,
                                                               sdk_wallet_new_steward)
