from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.test_node import ensure_node_disconnected, TestNode, checkNodesConnected, ensureElectionsDone

from plenum.common.constants import TXN_TYPE, DATA
from plenum.test.helper import sdk_gen_request, sdk_sign_and_submit_req_obj, sdk_get_reply, sdk_get_and_check_replies
from plenum.test.plugin.demo_plugin.constants import AUCTION_START, GET_AUCTION


def send_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_steward):
    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {'id': 'abc'}
    }
    return successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def send_get_auction_txn(looper,
                     sdk_pool_handle, sdk_wallet_steward):
    op = {
        TXN_TYPE: GET_AUCTION,
        DATA: {'auction_id': 'id'}
    }
    return successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


def successful_op(looper, op, sdk_wallet, sdk_pool_handle):
    req_obj = sdk_gen_request(op, identifier=sdk_wallet[1])
    req = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle,
                                      sdk_wallet, req_obj)
    return sdk_get_and_check_replies(looper, [req])


def restart_nodes(looper, nodeSet, restart_set, tconf, tdir, allPluginsPath,
                  after_restart_timeout=None, start_one_by_one=True, wait_for_elections=True):
    for node_to_stop in restart_set:
        node_to_stop.cleanupOnStopping = True
        node_to_stop.stop()
        looper.removeProdable(node_to_stop)

    rest_nodes = [n for n in nodeSet if n not in restart_set]
    for node_to_stop in restart_set:
        ensure_node_disconnected(looper, node_to_stop, nodeSet, timeout=2)

    if after_restart_timeout:
        looper.runFor(after_restart_timeout)

    for node_to_restart in restart_set.copy():
        config_helper = PNodeConfigHelper(node_to_restart.name, tconf, chroot=tdir)
        restarted_node = TestNode(node_to_restart.name, config_helper=config_helper, config=tconf,
                                  pluginPaths=allPluginsPath, ha=node_to_restart.nodestack.ha,
                                  cliha=node_to_restart.clientstack.ha)
        looper.add(restarted_node)

        idx = nodeSet.index(node_to_restart)
        nodeSet[idx] = restarted_node
        restart_set[idx] = restarted_node

        rest_nodes += [restarted_node]
        if start_one_by_one:
            looper.run(checkNodesConnected(rest_nodes))

    if not start_one_by_one:
        looper.run(checkNodesConnected(nodeSet))

    if wait_for_elections:
        ensureElectionsDone(looper=looper, nodes=nodeSet)
