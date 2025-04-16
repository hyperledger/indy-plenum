import pytest

from plenum.test.helper import vdr_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count
from stp_core.loop.eventually import eventually

REQ_COUNT = 5


def test_no_reauth(looper,
                   txnPoolNodeSet,
                   vdr_wallet_steward,
                   vdr_pool_handle):
    auth_obj = txnPoolNodeSet[0].authNr(0).core_authenticator
    auth_count_before = get_count(auth_obj, auth_obj.authenticate)
    vdr_send_random_and_check(looper,
                              txnPoolNodeSet,
                              vdr_pool_handle,
                              vdr_wallet_steward,
                              REQ_COUNT)
    auth_count_after = get_count(auth_obj, auth_obj.authenticate)
    assert auth_count_after - auth_count_before == REQ_COUNT
