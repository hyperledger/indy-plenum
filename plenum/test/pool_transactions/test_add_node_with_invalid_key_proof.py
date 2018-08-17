import pytest
from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_get_and_check_replies
from plenum.common.constants import VALIDATOR
from plenum.test.pool_transactions.helper import prepare_new_node_data, \
    prepare_node_request, sdk_sign_and_send_prepared_request, \
    create_and_start_new_node
from plenum.test.test_node import TestNode


def test_add_node_with_invalid_key_proof(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_steward,
                                         tdir, tconf,
                                         allPluginsPath):
    new_node_name = "NewNode"
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)
    # change key_proof
    key_proof = key_proof.upper()

    # filling node request
    _, steward_did = sdk_wallet_steward
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[VALIDATOR],
                             key_proof=key_proof))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper,
                                                        sdk_wallet_steward,
                                                        sdk_pool_handle,
                                                        node_request)

    # waitng for replies
    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])
        assert "Proof of possession {} " \
               "is incorrect for BLS key {}".format(key_proof, bls_key) \
               in e._excinfo[1].args[0]
