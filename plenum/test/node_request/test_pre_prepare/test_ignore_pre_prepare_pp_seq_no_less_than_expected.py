import pytest

from stp_core.common.util import adict
from plenum.test.helper import sendRandomRequests, waitForSufficientRepliesForRequests
from plenum.test.test_node import getNonPrimaryReplicas


def test_ignore_pre_prepare_pp_seq_no_less_than_expected(looper,
                                                         nodeSet, up,
                                                         wallet1, client1):
    """
    A node should NOT pend a pre-prepare request which
    has ppSeqNo less than expected.

    https://jira.hyperledger.org/browse/INDY-159,
    https://jira.hyperledger.org/browse/INDY-160

    """
    replica = getNonPrimaryReplicas(nodeSet, instId=0)[0]
    replica.last_ordered_3pc = (replica.viewNo, 10)

    requests = sendRandomRequests(wallet1, client1, 1)
    waitForSufficientRepliesForRequests(looper, client1,
                                        requests=requests)
    assert len(replica.prePreparesPendingPrevPP) == 0, \
        "the pending request buffer is empty"
