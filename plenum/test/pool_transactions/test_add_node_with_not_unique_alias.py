import pytest

from plenum.common.exceptions import RequestRejectedException
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_node, sdk_pool_refresh


def test_add_node_with_not_unique_alias(looper,
                                        tdir,
                                        tconf,
                                        sdk_pool_handle,
                                        sdk_wallet_steward,
                                        allPluginsPath):
    new_node_name = "Alpha"
    new_steward_wallet, steward_did = sdk_add_new_nym(looper,
                                                      sdk_pool_handle,
                                                      sdk_wallet_steward,
                                                      alias="TEST_STEWARD1",
                                                      role='STEWARD')
    with pytest.raises(RequestRejectedException) as e:
        sdk_add_new_node(looper,
                         sdk_pool_handle,
                         (new_steward_wallet, steward_did),
                         new_node_name,
                         tdir,
                         tconf,
                         allPluginsPath)
    assert 'existing data has conflicts with request data' in \
           e._excinfo[1].args[0]
    sdk_pool_refresh(looper, sdk_pool_handle)