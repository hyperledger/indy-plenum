import pytest

from plenum.test.helper import sdk_send_random_and_check
from plenum.test.spy_helpers import get_count
from stp_core.loop.eventually import eventually

REQ_COUNT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    old_chk_freq = tconf.CHK_FREQ
    old_3pc_batch_size = tconf.Max3PCBatchSize
    tconf.CHK_FREQ = REQ_COUNT
    tconf.Max3PCBatchSize = 1
    yield tconf

    tconf.CHK_FREQ = old_chk_freq
    tconf.Max3PCBatchSize = old_3pc_batch_size


def test_clean_verified_reqs(looper,
                           txnPoolNodeSet,
                           sdk_wallet_steward,
                           sdk_pool_handle):
    """ As for now requests object is cleaned only after checkpoint stabilization,
    therefore need to forcing checkpoint sending"""
    def checkpoint_check(nodes):
        for node in nodes:
            assert get_count(node.master_replica, node.master_replica.markCheckPointStable) > 0

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              REQ_COUNT)
    looper.run(eventually(checkpoint_check, txnPoolNodeSet))
    for node in txnPoolNodeSet:
        assert len(node.requests) == 0
        assert len(node.clientAuthNr._verified_reqs) == 0
