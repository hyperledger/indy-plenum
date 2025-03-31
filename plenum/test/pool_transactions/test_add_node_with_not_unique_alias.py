import pytest

from plenum.common.exceptions import RequestRejectedException
from plenum.test.pool_transactions.helper import vdr_add_new_nym, vdr_add_new_node, vdr_pool_refresh


def test_add_node_with_not_unique_alias(looper,
                                        tdir,
                                        tconf,
                                        vdr_pool_handle,
                                        vdr_wallet_steward,
                                        allPluginsPath):
    new_node_name = "Alpha"
    new_steward_wallet, steward_did = vdr_add_new_nym(looper,
                                                      vdr_pool_handle,
                                                      vdr_wallet_steward,
                                                      alias="TEST_STEWARD1",
                                                      role='STEWARD')
    with pytest.raises(RequestRejectedException) as e:
        vdr_add_new_node(looper,
                         vdr_pool_handle,
                         (new_steward_wallet, steward_did),
                         new_node_name,
                         tdir,
                         tconf,
                         allPluginsPath)
    assert 'existing data has conflicts with request data' in \
           e._excinfo[1].args[0]
    vdr_pool_refresh(looper, vdr_pool_handle)