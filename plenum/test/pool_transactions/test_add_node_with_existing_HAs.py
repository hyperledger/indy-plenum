import pytest
from plenum.test.helper import sdk_get_and_check_replies

from plenum.common.constants import VALIDATOR
from plenum.common.util import randomString

from plenum.common.exceptions import RequestRejectedException
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_sign_and_send_prepared_request, \
    prepare_new_node_data, prepare_node_request


def test_add_node_with_existing_HAs(looper,
                                    txnPoolNodeSet,
                                    tdir,
                                    tconf,
                                    sdk_pool_handle,
                                    sdk_wallet_steward):
    alias = randomString(5)
    new_node_name = "Node-" + alias
    steward_wallet_handle = sdk_add_new_nym(looper,
                                            sdk_pool_handle,
                                            sdk_wallet_steward,
                                            alias="Steward-" + alias,
                                            role='STEWARD')

    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)

    # Setting already existing HAs
    ha = txnPoolNodeSet[0].nodeReg[txnPoolNodeSet[-1].name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[txnPoolNodeSet[-1].name + 'C']
    nodeIp = ha[0]
    nodePort = ha[1]
    clientIp = cli_ha[0]
    clientPort = cli_ha[1]

    _, steward_did = steward_wallet_handle
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

    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)

    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])
    e.match('existing data has conflicts with request data')
