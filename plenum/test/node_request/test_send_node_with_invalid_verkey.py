import json

import base58
import pytest

from plenum.common.constants import STEWARD_STRING, VALIDATOR, VERKEY
from plenum.common.exceptions import RequestNackedException
from plenum.common.util import randomString
from plenum.test.helper import vdr_get_bad_response
from plenum.test.pool_transactions.helper import vdr_add_new_nym, prepare_new_node_data, vdr_prepare_node_request, \
    vdr_sign_and_send_prepared_request, vdr_change_node_keys

invalid_dest = 'a' * 43


def test_send_node_with_invalid_dest_verkey(looper, vdr_pool_handle,
                                            vdr_wallet_steward, tdir, tconf):
    node_name = "Psi"
    new_steward_name = "testClientSteward" + randomString(3)
    new_steward_wallet_handle = vdr_add_new_nym(looper,
                                                vdr_pool_handle,
                                                vdr_wallet_steward,
                                                alias=new_steward_name,
                                                role=STEWARD_STRING)
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, node_name)

    # Invalid dest passes static validation
    assert len(base58.b58decode(invalid_dest)) == 32

    _, steward_did = new_steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        vdr_prepare_node_request(steward_did,
                             new_node_name=node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             destination=invalid_dest,
                             services=[VALIDATOR],
                             key_proof=key_proof))

    request_couple = vdr_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                        vdr_pool_handle, node_request)
    vdr_get_bad_response(looper, [request_couple], RequestNackedException,
                         'Node\'s dest is not correct Ed25519 key.')

    node_request = looper.loop.run_until_complete(
        vdr_prepare_node_request(steward_did,
                             new_node_name=node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[VALIDATOR],
                             key_proof=key_proof))

    node_request = json.loads(node_request)
    node_request['operation'][VERKEY] = invalid_dest
    node_request = json.dumps(node_request)

    request_couple = vdr_sign_and_send_prepared_request(looper, new_steward_wallet_handle,
                                                        vdr_pool_handle, node_request)
    vdr_get_bad_response(looper, [request_couple], RequestNackedException,
                         'Node\'s verkey is not correct Ed25519 key.')


def test_edit_node_with_invalid_verkey(looper, txnPoolNodeSet, vdr_pool_handle, vdr_wallet_steward):
    with pytest.raises(RequestNackedException) as e:
        vdr_change_node_keys(looper, txnPoolNodeSet[0], vdr_wallet_steward, vdr_pool_handle, invalid_dest)
    e.match('Node\'s verkey is not correct Ed25519 key.')
