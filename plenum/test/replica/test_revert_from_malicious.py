import pytest

from plenum.common.exceptions import InvalidClientMessageException, RequestRejectedException
from plenum.test.helper import vdr_send_random_and_check
from plenum.test.test_node import getPrimaryReplica


def test_revert_pp_from_malicious(looper,
                                  txnPoolNodeSet,
                                  vdr_pool_handle,
                                  vdr_wallet_client):
    def raise_invalid_ex():
        raise InvalidClientMessageException(1, 2, "3")
    malicious_primary = getPrimaryReplica(txnPoolNodeSet).node
    not_malicious_nodes = set(txnPoolNodeSet) - {malicious_primary}
    for n in not_malicious_nodes:
        n.master_replica._ordering_service._do_dynamic_validation = lambda *args, **kwargs: raise_invalid_ex()
    with pytest.raises(RequestRejectedException, match="client request invalid"):
        vdr_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  vdr_pool_handle,
                                  vdr_wallet_client,
                                  1)
