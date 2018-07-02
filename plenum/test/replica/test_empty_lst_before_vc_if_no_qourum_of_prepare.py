import pytest

from plenum.common.messages.node_messages import ThreePhaseKey
from plenum.test.delayers import pDelay
from plenum.test.helper import sdk_send_random_and_check


@pytest.fixture(scope="module")
def tconf(tconf):
    old_m = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = old_m


def test_empty_lst_before_vc_if_no_qourum_of_prepare(looper,
                                                     txnPoolNodeSet,
                                                     sdk_pool_handle,
                                                     sdk_wallet_steward):
    A, B, C, D = txnPoolNodeSet
    for n in [A, B, C]:
        # Do not send prepares to Delta node
        D.nodeIbStasher.delay(pDelay(1000, 0, sender_filter=n.name))
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_steward,
                              5)
    quorum = D.master_replica.quorums.prepare.value
    assert D.master_replica.prepares
    # There is no quorum for all prepares
    for key in D.master_replica.prepares.keys():
        assert D.master_replica.prepares.hasQuorum(ThreePhaseKey(*key), quorum) == False
    assert D.master_replica.last_prepared_certificate_in_view() is None