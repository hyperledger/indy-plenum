from plenum.common.signer_simple import SimpleSigner
from plenum.test.pool_transactions.helper import prepare_node_request,\
    sdk_sign_and_send_prepared_request, sdk_pool_refresh
from plenum.common.constants import VALIDATOR
from plenum.common.util import randomString
from plenum.test.helper import sdk_get_and_check_replies
from stp_core.network.port_dispenser import genHa


def add_ne_node(looper, sdk_pool_handle, steward_wallet_handle):
    new_node_name = randomString(7)
    nodeSigner = SimpleSigner()
    dest = nodeSigner.identifier
    (nodeIp, nodePort), (clientIp, clientPort) = genHa(2)
    _, steward_did = steward_wallet_handle
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did, new_node_name=new_node_name, destination=dest, clientIp=clientIp,
                             clientPort=clientPort, nodeIp=nodeIp, nodePort=nodePort, services=[VALIDATOR]))
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle, sdk_pool_handle, node_request)
    sdk_get_and_check_replies(looper, [request_couple])
    return dest, new_node_name


def test_demote_nonexisted(looper, txnPoolNodeSet, sdk_pool_handle, tdir, tconf, sdk_wallet_new_steward):
    dst, name = add_ne_node(looper, sdk_pool_handle, sdk_wallet_new_steward)
    assert dst
    sdk_pool_refresh(looper, sdk_pool_handle)
    assert len(txnPoolNodeSet[0].nodeReg) == len(txnPoolNodeSet) + 1
    _, st_did = sdk_wallet_new_steward
    node_request = looper.loop.run_until_complete(
        prepare_node_request(st_did, destination=dst, new_node_name=name, services=[]))

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_new_steward,
                                                        sdk_pool_handle, node_request)
    sdk_get_and_check_replies(looper, [request_couple])
