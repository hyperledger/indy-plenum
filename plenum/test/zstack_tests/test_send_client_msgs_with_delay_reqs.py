import types

import pytest
from indy.pool import close_pool_ledger
from plenum.common.config_helper import PNodeConfigHelper

from plenum.common.constants import TRUSTEE_STRING, STEWARD_STRING, VALIDATOR
from plenum.common.exceptions import PoolLedgerTimeoutException, RequestNackedException, RequestRejectedException
from plenum.common.util import randomString
from plenum.test.conftest import _gen_pool_handler
from plenum.test.delayers import cDelay, req_delay
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_request, sdk_get_and_check_replies, \
    sdk_signed_random_requests, sdk_send_signed_requests, sdk_sign_and_submit_req, sdk_sign_and_submit_op, assertExp, \
    sdk_set_protocol_version
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_nym, sdk_add_new_steward_and_node, sdk_add_new_node, \
    prepare_new_node_data, prepare_node_request, sdk_sign_and_send_prepared_request, create_and_start_new_node, \
    prepare_nym_request
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected, ensure_node_disconnected, TestNode
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()



@pytest.fixture(scope='module')
def sdk_test_pool_handle(looper, txnPoolNodeSet, tdirWithPoolTxns, sdk_pool_data):
    # TODO think about moving protocol version setting to separate
    # fixture like 'sdk_init' since some sdk request builders don't
    # requires pool handle but use protocol version
    sdk_set_protocol_version(looper)
    pool_name, open_config = sdk_pool_data
    pool_handle = looper.loop.run_until_complete(
        _gen_pool_handler(tdirWithPoolTxns, pool_name, open_config))
    looper.loop.run_until_complete(close_pool_ledger(pool_handle))
    yield pool_handle
    try:
        looper.loop.run_until_complete(close_pool_ledger(pool_handle))
    except Exception as e:
        logger.debug("Unhandled exception: {}".format(e))

def test_send_client_msgs_with_delay_reqs(looper,
                                          txnPoolNodeSet,
                                          sdk_pool_handle,
                                          sdk_wallet_steward,
                                          sdk_test_pool_handle):
    seed = randomString(32)
    alias = randomString(5)
    wh, _ = sdk_wallet_steward
    dest = None
    verkey = None
    skipverkey = False
    nym_request, new_did = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_steward, seed,
                            alias, None, dest, verkey, skipverkey))
    # for n in txnPoolNodeSet:
    #     n.

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_steward,
                                                        sdk_pool_handle, nym_request)


    rep = sdk_get_and_check_replies(looper, [request_couple])[0]


def add_new_node(looper,
                     sdk_pool_handle,
                     steward_wallet_handle,
                     new_node_name,
                     tdir, tconf,
                     allPluginsPath=None, autoStart=True, nodeClass=TestNode,
                     do_post_node_creation=None,
                     services=[VALIDATOR]):
    nodeClass = TestNode
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)

    # filling node request
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
                             services=services,
                             key_proof=key_proof))

    # sending request using 'sdk_' functions
    request_couple = sdk_sign_and_send_prepared_request(looper, steward_wallet_handle,
                                                        sdk_pool_handle, node_request)

    # waitng for replies
    #sdk_get_and_check_replies(looper, [request_couple])
    looper.runFor(10)

    return create_and_start_new_node(looper, new_node_name, tdir, sigseed,
                                     (nodeIp, nodePort), (clientIp, clientPort),
                                     tconf, autoStart, allPluginsPath,
                                     nodeClass,
                                     do_post_node_creation=do_post_node_creation,
                                     configClass=PNodeConfigHelper)


def test_send_client_msgs_with_delay_reqs_2(looper,
                                          txnPoolNodeSet,
                                          sdk_pool_handle,
                                          sdk_wallet_client,
                                          sdk_wallet_steward,
                                            tdir, tconf, allPluginsPath):
    print(len(txnPoolNodeSet))
    faulty_nodes = list(txnPoolNodeSet[1:3])
    for i in range(6):
        new_steward_wallet_handle = sdk_add_new_nym(looper,
                                                    sdk_pool_handle,
                                                    sdk_wallet_steward,
                                                    alias="new_steward_{}".format(i),
                                                    role=STEWARD_STRING)
        new_node = add_new_node(
            looper,
            sdk_pool_handle,
            new_steward_wallet_handle,
            "new_node_{}".format(i),
            tdir,
            tconf,
            allPluginsPath)

        # steward, new_node = sdk_add_new_steward_and_node(
        #     looper, sdk_pool_handle, sdk_wallet_steward,
        #     "new_steward_{}".format(i),
        #     "new_node_{}".format(i), tdir, tconf,
        #     allPluginsPath=allPluginsPath)
        txnPoolNodeSet.append(new_node)
        # looper.run(checkNodesConnected(txnPoolNodeSet))
        waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                             exclude_from_check=['check_last_ordered_3pc_backup'])

    print(len(txnPoolNodeSet))
    for node in faulty_nodes:
        node.stop()
        looper.removeProdable(node)
        txnPoolNodeSet.remove(node)
        ensure_node_disconnected(looper, node, txnPoolNodeSet)

    print(len(txnPoolNodeSet))
    start_master_last_ordered_3pc = txnPoolNodeSet[0].master_last_ordered_3PC[1]
    resp_task = sdk_add_new_nym(looper,
                                sdk_pool_handle,
                                sdk_wallet_client,
                                role=TRUSTEE_STRING,
                                no_wait=True)
    looper.run(
        eventually(
            lambda node: assertExp(node.master_last_ordered_3PC[1] ==
                                   start_master_last_ordered_3pc + 1), txnPoolNodeSet[0]))
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    print(txnPoolNodeSet[-1].master_last_ordered_3PC)

    with pytest.raises(RequestRejectedException, match="Only Steward is allowed to do these transactions"):
        _, resp = sdk_get_and_check_replies(looper, [resp_task])[0]
