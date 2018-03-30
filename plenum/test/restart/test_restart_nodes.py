import pytest
from plenum.test.test_node import ensure_node_disconnected, checkNodesConnected
from plenum.test import waits
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.test_node import TestNode


def get_group(nodeSet, group_cnt, include_primary=False):
    if group_cnt >= len(nodeSet):
        return nodeSet

    ret_group = []
    primary_name = nodeSet[0].master_primary_name
    primary_idx = next(i for i, _ in enumerate(nodeSet) if nodeSet[i].name == primary_name)
    if not include_primary:
        primary_idx += 1

    while len(ret_group) < group_cnt:
        ret_group.append(nodeSet[primary_idx % len(nodeSet)])
        primary_idx += 1

    return ret_group


def restart_nodes(looper, nodeSet, restart_set, tconf, tdir, allPluginsPath,
                  after_restart_timeout=None, per_add_timeout=None):
    for node_to_stop in restart_set:
        node_to_stop.cleanupOnStopping = True
        node_to_stop.stop()
        looper.removeProdable(node_to_stop)

    rest_nodes = [n for n in nodeSet if n not in restart_set]
    for node_to_stop in restart_set:
        ensure_node_disconnected(looper, node_to_stop, nodeSet, timeout=2)

    if after_restart_timeout:
        looper.runFor(after_restart_timeout)

    for node_to_restart in restart_set:
        config_helper = PNodeConfigHelper(node_to_restart.name, tconf, chroot=tdir)
        restarted_node = TestNode(node_to_restart.name, config_helper=config_helper, config=tconf,
                                  pluginPaths=allPluginsPath, ha=node_to_restart.nodestack.ha,
                                  cliha=node_to_restart.clientstack.ha)
        looper.add(restarted_node)
        idx = nodeSet.index(node_to_restart)
        nodeSet[idx] = restarted_node
        if per_add_timeout:
            looper.run(checkNodesConnected(rest_nodes + [restarted_node], customTimeout=per_add_timeout))
        rest_nodes += [restarted_node]

    if not per_add_timeout:
        looper.run(checkNodesConnected(nodeSet, customTimeout=after_restart_timeout))


nodeCount = 7


def test_restart_groups(looper, txnPoolNodeSet, tconf, tdir,
                        sdk_pool_handle, sdk_wallet_client, allPluginsPath):
    tm = tconf.ToleratePrimaryDisconnection + waits.expectedPoolElectionTimeout(len(txnPoolNodeSet))

    restart_group = get_group(txnPoolNodeSet, 4, include_primary=False)

    restart_nodes(looper, txnPoolNodeSet, restart_group, tconf, tdir, allPluginsPath,
                  after_restart_timeout=tm, per_add_timeout=tm)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
