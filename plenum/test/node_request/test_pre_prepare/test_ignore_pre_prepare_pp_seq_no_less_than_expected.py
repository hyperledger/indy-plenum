import pytest

from stp_core.common.util import adict
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.test_node import getNonPrimaryReplicas
from plenum.test.pool_transactions.conftest import looper


def test_ignore_pre_prepare_pp_seq_no_less_than_expected(looper,
                                                         txnPoolNodeSet,
                                                         sdk_wallet_client,
                                                         sdk_pool_handle):
    """
    A node should NOT pend a pre-prepare request which
    has ppSeqNo less than expected.

    https://jira.hyperledger.org/browse/INDY-159,
    https://jira.hyperledger.org/browse/INDY-160

    """
    replica = getNonPrimaryReplicas(txnPoolNodeSet, instId=0)[0]
    replica.last_ordered_3pc = (replica.viewNo, 10)

    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              count=1)
    assert len(replica.prePreparesPendingPrevPP) == 0, \
        "the pending request buffer is empty"
