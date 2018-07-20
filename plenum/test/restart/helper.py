from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.test_node import ensure_node_disconnected, checkNodesConnected, ensureElectionsDone, TestNode


def get_group(nodeSet, group_cnt, include_primary=False):
    if group_cnt >= len(nodeSet):
        return nodeSet.copy()

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
        idx = restart_set.index(node_to_restart)
        restart_set[idx] = restarted_node

        rest_nodes += [restarted_node]
        if start_one_by_one:
            looper.run(checkNodesConnected(rest_nodes))

    if not start_one_by_one:
        looper.run(checkNodesConnected(nodeSet))

    if wait_for_elections:
        ensureElectionsDone(looper=looper, nodes=nodeSet)
