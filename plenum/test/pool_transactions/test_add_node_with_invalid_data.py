import pytest
from plenum.test.helper import sdk_get_bad_response

from plenum.common.constants import VALIDATOR
from plenum.common.util import randomString, hexToFriendly

from plenum.common.exceptions import RequestRejectedException
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_sign_and_send_prepared_request, \
    prepare_new_node_data, prepare_node_request, sdk_send_update_node


def create_specific_node_request(looper, steward_wallet_handle,
                                 tconf, tdir, new_node_name=randomString(5),
                                 new_node_ip=None, new_node_port=None, new_client_ip=None,
                                 new_client_port=None):
    sigseed, verkey, bls_key, node_ip, node_port, client_ip, client_port, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)

    node_ip = new_node_ip if new_node_ip else node_ip
    node_port = new_node_port if new_node_port else node_port
    client_ip = new_client_ip if new_client_ip else client_ip
    client_port = new_client_port if new_client_port else client_port

    _, steward_did = steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=client_ip,
                             clientPort=client_port,
                             nodeIp=node_ip,
                             nodePort=node_port,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[VALIDATOR],
                             key_proof=key_proof))
    return node_request


def test_add_node_with_existing_data(looper,
                                     txnPoolNodeSet,
                                     tdir,
                                     tconf,
                                     sdk_pool_handle,
                                     sdk_wallet_stewards):
    alias = randomString(5)
    new_node_name = "Node-" + alias
    steward_wallet_handle = sdk_add_new_nym(looper,
                                            sdk_pool_handle,
                                            sdk_wallet_stewards[0],
                                            alias="Steward-" + alias,
                                            role='STEWARD')
    # Setting already existing HAs
    existing_ha = txnPoolNodeSet[0].nodeReg[txnPoolNodeSet[-1].name]
    existing_cli_ha = txnPoolNodeSet[0].cliNodeReg[txnPoolNodeSet[-1].name + 'C']

    # Check for existing alias
    node_request = create_specific_node_request(
        looper, steward_wallet_handle, tconf, tdir, txnPoolNodeSet[0].name)
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)
    sdk_get_bad_response(looper, [request_couple], RequestRejectedException,
                         "Node's alias must be unique")

    # Check for existing node HAs
    node_request = create_specific_node_request(
        looper, steward_wallet_handle, tconf, tdir, new_node_name,
        new_node_ip=existing_ha[0], new_node_port=existing_ha[1])
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)
    sdk_get_bad_response(looper, [request_couple], RequestRejectedException,
                         "Node's nodestack addresses must be unique")

    # Check for existing client HAs
    node_request = create_specific_node_request(
        looper, steward_wallet_handle, tconf, tdir, new_node_name,
        new_client_ip=existing_cli_ha[0], new_client_port=existing_cli_ha[1])
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)
    sdk_get_bad_response(looper, [request_couple], RequestRejectedException,
                         "Node's clientstack addresses must be unique")


def test_try_change_node_alias(looper,
                               txnPoolNodeSet,
                               sdk_pool_handle,
                               sdk_wallet_stewards):
    node = txnPoolNodeSet[1]
    node_dest = hexToFriendly(node.nodestack.verhex)
    with pytest.raises(RequestRejectedException) as e:
        sdk_send_update_node(looper, sdk_wallet_stewards[1],
                             sdk_pool_handle,
                             node_dest, node.name + '-foo',
                             None, None,
                             None, None,
                             services=[])
    assert e.match("Node's alias cannot be changed")
