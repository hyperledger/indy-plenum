import pytest

from plenum.common.exceptions import RequestRejectedException, \
    RequestNackedException
from plenum.common.keygen_utils import init_bls_keys
from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.common.constants import CLIENT_STACK_SUFFIX
from plenum.common.util import randomString, hexToFriendly
from plenum.test.pool_transactions.helper import sdk_send_update_node, \
    sdk_add_new_steward_and_node, sdk_pool_refresh, \
    update_node_data_and_reconnect, demote_node
from plenum.test.test_node import checkNodesConnected

from stp_core.common.log import getlogger
from stp_core.network.port_dispenser import genHa

logger = getlogger()


# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def test_node_alias_cannot_be_changed(looper, txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_node_theta_added):
    """
    The node alias cannot be changed.
    """
    new_steward_wallet, new_node = sdk_node_theta_added
    node_dest = hexToFriendly(new_node.nodestack.verhex)
    with pytest.raises(RequestRejectedException) as e:
        sdk_send_update_node(looper, new_steward_wallet, sdk_pool_handle,
                             node_dest, 'foo',
                             None, None,
                             None, None)
    assert 'data has conflicts with request data' in e._excinfo[1].args[0]
    sdk_pool_refresh(looper, sdk_pool_handle)


def testNodePortChanged(looper, txnPoolNodeSet,
                        sdk_wallet_steward,
                        sdk_pool_handle,
                        sdk_node_theta_added,
                        tdir, tconf):
    """
    A running node's port is changed
    """
    orig_view_no = txnPoolNodeSet[0].viewNo
    new_steward_wallet, new_node = sdk_node_theta_added

    node_new_ha = genHa(1)
    new_port = node_new_ha.port
    node_ha = txnPoolNodeSet[0].nodeReg[new_node.name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[new_node.name + CLIENT_STACK_SUFFIX]

    update_node_data_and_reconnect(looper, txnPoolNodeSet,
                                   new_steward_wallet,
                                   sdk_pool_handle,
                                   new_node,
                                   node_ha.host, new_port,
                                   cli_ha.host, cli_ha.port,
                                   tdir, tconf)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, new_steward_wallet, sdk_pool_handle)

    # Make sure that no additional view changes happened
    assert all(n.viewNo == orig_view_no for n in txnPoolNodeSet)


def test_fail_node_bls_key_validation(looper,
                                      sdk_pool_handle,
                                      sdk_node_theta_added):
    """
    Test request for change node bls key with incorrect
    bls key proof of possession.
    """
    new_steward_wallet, new_node = sdk_node_theta_added
    node_dest = hexToFriendly(new_node.nodestack.verhex)
    bls_key, key_proof = init_bls_keys(new_node.keys_dir, new_node.name)
    # change key_proof
    key_proof = key_proof.upper()
    with pytest.raises(RequestNackedException) as e:
        sdk_send_update_node(looper, new_steward_wallet, sdk_pool_handle,
                             node_dest, new_node.name,
                             None, None,
                             None, None,
                             bls_key=bls_key,
                             key_proof=key_proof)
        assert "Proof of possession {} " \
               "is incorrect for BLS key {}".format(key_proof, bls_key) \
               in e._excinfo[1].args[0]
