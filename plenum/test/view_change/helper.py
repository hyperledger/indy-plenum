import types

from plenum.common.util import randomString
from plenum.server.view_change.node_view_changer import create_view_changer
from stp_core.types import HA

from plenum.test.delayers import delayNonPrimaries, delay_3pc_messages, \
    reset_delays_and_process_delayeds
from plenum.test.helper import checkViewNoForNodes, \
    sdk_send_random_requests, sdk_send_random_and_check
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, sdk_add_new_steward_and_node, sdk_pool_refresh
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data, waitNodeDataEquality
from plenum.test.test_node import get_master_primary_node, ensureElectionsDone, \
    TestNode, checkNodesConnected, TestViewChanger
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


def start_stopped_node(stopped_node, looper, tconf,
                       tdir, allPluginsPath,
                       delay_instance_change_msgs=True,
                       start=True):
    nodeHa, nodeCHa = HA(*
                         stopped_node.nodestack.ha), HA(*
                                                        stopped_node.clientstack.ha)
    config_helper = PNodeConfigHelper(stopped_node.name, tconf, chroot=tdir)
    restarted_node = TestNode(stopped_node.name,
                              config_helper=config_helper,
                              config=tconf,
                              ha=nodeHa, cliha=nodeCHa,
                              pluginPaths=allPluginsPath)
    if start:
        looper.add(restarted_node)
    return restarted_node


def provoke_and_check_view_change(looper, nodes, newViewNo, sdk_pool_handle, sdk_wallet_client):
    if {n.viewNo for n in nodes} == {newViewNo}:
        return True

    # If throughput of every node has gone down then check that
    # view has changed
    tr = [n.monitor.isMasterThroughputTooLow() for n in nodes]
    if all(tr):
        logger.info('Throughput ratio gone down, its {}'.format(tr))
        checkViewNoForNodes(nodes, newViewNo)
    else:
        logger.info('Master instance has not degraded yet, '
                    'sending more requests')
        sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client)
        assert False


def provoke_and_wait_for_view_change(looper,
                                     nodeSet,
                                     expectedViewNo,
                                     sdk_pool_handle,
                                     sdk_wallet_client,
                                     customTimeout=None):
    timeout = customTimeout or waits.expectedPoolViewChangeStartedTimeout(
        len(nodeSet))
    # timeout *= 30
    return looper.run(eventually(provoke_and_check_view_change,
                                 looper,
                                 nodeSet,
                                 expectedViewNo,
                                 sdk_pool_handle,
                                 sdk_wallet_client,
                                 timeout=timeout))


def simulate_slow_master(looper, txnPoolNodeSet, sdk_pool_handle,
                         sdk_wallet_steward, delay=10, num_reqs=4):
    m_primary_node = get_master_primary_node(list(txnPoolNodeSet))
    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    delayNonPrimaries(txnPoolNodeSet, 0, delay)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_steward, num_reqs)
    return m_primary_node


def ensure_view_change(looper, nodes, exclude_from_check=None,
                       custom_timeout=None):
    """
    This method patches the master performance check to return False and thus
    ensures that all given nodes do a view change
    """
    old_view_no = checkViewNoForNodes(nodes)

    old_meths = do_view_change(nodes)

    perf_check_freq = next(iter(nodes)).config.PerfCheckFreq
    timeout = custom_timeout or waits.expectedPoolViewChangeStartedTimeout(
        len(nodes)) + perf_check_freq
    nodes_to_check = nodes if exclude_from_check is None else [
        n for n in nodes if n not in exclude_from_check]
    logger.debug('Checking view no for nodes {}'.format(nodes_to_check))
    looper.run(eventually(checkViewNoForNodes, nodes_to_check, old_view_no + 1,
                          retryWait=1, timeout=timeout))

    revert_do_view_change(nodes, old_meths)

    return old_view_no + 1


def do_view_change(nodes):
    old_meths = {node.name: {} for node in nodes}
    view_changes = {}
    for node in nodes:
        old_meths[node.name]['isMasterDegraded'] = node.monitor.isMasterDegraded
        old_meths[node.name]['_update_new_ordered_reqs_count'] = node._update_new_ordered_reqs_count
        view_changes[node.name] = node.monitor.totalViewChanges

        def slow_master(self):
            # Only allow one view change
            rv = self.totalViewChanges == view_changes[self.name]
            if rv:
                logger.info('{} making master look slow'.format(self))
            return rv

        node.monitor.isMasterDegraded = types.MethodType(
            slow_master, node.monitor)
        node._update_new_ordered_reqs_count = types.MethodType(
            lambda self: True, node)
    return old_meths


def revert_do_view_change(nodes, old_meths):
    logger.debug('Patching back perf check for all nodes')
    for node in nodes:
        node.monitor.isMasterDegraded = old_meths[node.name]['isMasterDegraded']
        node._update_new_ordered_reqs_count = old_meths[node.name]['_update_new_ordered_reqs_count']


def ensure_several_view_change(looper, nodes, vc_count=1,
                               exclude_from_check=None, custom_timeout=None):
    """
    This method patches the master performance check to return False and thus
    ensures that all given nodes do a view change
    Also, this method can do several view change.
    If you try do several view_change by calling ensure_view_change,
    than monkeypatching method isMasterDegraded would work unexpectedly.
    Therefore, we return isMasterDegraded only after doing view_change needed count
    """
    old_meths = {}
    view_changes = {}
    expected_view_no = None
    for node in nodes:
        old_meths[node.name] = node.monitor.isMasterDegraded

    for __ in range(vc_count):
        old_view_no = checkViewNoForNodes(nodes)
        expected_view_no = old_view_no + 1

        for node in nodes:
            view_changes[node.name] = node.monitor.totalViewChanges

            def slow_master(self):
                # Only allow one view change
                rv = self.totalViewChanges == view_changes[self.name]
                if rv:
                    logger.info('{} making master look slow'.format(self))
                return rv

            node.monitor.isMasterDegraded = types.MethodType(slow_master, node.monitor)

        perf_check_freq = next(iter(nodes)).config.PerfCheckFreq
        timeout = custom_timeout or waits.expectedPoolViewChangeStartedTimeout(len(nodes)) + perf_check_freq
        nodes_to_check = nodes if exclude_from_check is None else [n for n in nodes if n not in exclude_from_check]
        logger.debug('Checking view no for nodes {}'.format(nodes_to_check))
        looper.run(eventually(checkViewNoForNodes, nodes_to_check, expected_view_no, retryWait=1, timeout=timeout))
        ensureElectionsDone(looper=looper, nodes=nodes, customTimeout=timeout)
        ensure_all_nodes_have_same_data(looper, nodes, custom_timeout=timeout, exclude_from_check=exclude_from_check)

    return expected_view_no


def ensure_view_change_by_primary_restart(
        looper, nodes,
        tconf, tdirWithPoolTxns, allPluginsPath, customTimeout=None):
    """
    This method stops current primary for a while to force a view change

    Returns new set of nodes
    """
    old_view_no = checkViewNoForNodes(nodes)
    primaryNode = [node for node in nodes if node.has_master_primary][0]

    logger.debug("Disconnect current primary node {} from others, "
                 "current viewNo {}".format(primaryNode, old_view_no))

    disconnect_node_and_ensure_disconnected(looper, nodes,
                                            primaryNode, stopNode=True)
    looper.removeProdable(primaryNode)
    remainingNodes = list(set(nodes) - {primaryNode})

    logger.debug("Waiting for viewNo {} for nodes {}"
                 "".format(old_view_no + 1, remainingNodes))
    timeout = customTimeout or waits.expectedPoolViewChangeStartedTimeout(
        len(remainingNodes)) + nodes[0].config.ToleratePrimaryDisconnection
    looper.run(eventually(checkViewNoForNodes, remainingNodes, old_view_no + 1,
                          retryWait=1, timeout=timeout))

    logger.debug("Starting stopped ex-primary {}".format(primaryNode))
    restartedNode = start_stopped_node(primaryNode, looper, tconf,
                                       tdirWithPoolTxns, allPluginsPath,
                                       delay_instance_change_msgs=False)
    nodes = remainingNodes + [restartedNode]
    logger.debug("Ensure all nodes are connected")
    looper.run(checkNodesConnected(nodes))
    logger.debug("Ensure all nodes have the same data")
    ensure_all_nodes_have_same_data(looper, nodes=nodes)

    return nodes


def check_each_node_reaches_same_end_for_view(nodes, view_no):
    # Check if each node agreed on the same ledger summary and last ordered
    # seq no for same view
    args = {}
    vals = {}
    for node in nodes:
        params = [e.params for e in node.replicas[0].spylog.getAll(
            node.replicas[0].primary_changed.__name__)
                  if e.params['view_no'] == view_no]
        assert params
        args[node.name] = (params[0]['last_ordered_pp_seq_no'],
                           params[0]['ledger_summary'])
        vals[node.name] = node.replicas[0].view_ends_at[view_no - 1]

    arg = list(args.values())[0]
    for a in args.values():
        assert a == arg

    val = list(args.values())[0]
    for v in vals.values():
        assert v == val


def disconnect_master_primary(nodes):
    pr_node = get_master_primary_node(nodes)
    for node in nodes:
        if node != pr_node:
            node.nodestack.getRemote(pr_node.nodestack.name).disconnect()
    return pr_node


def check_replica_queue_empty(node):
    replica = node.replicas[0]

    assert len(replica.prePrepares) == 0
    assert len(replica.prePreparesPendingFinReqs) == 0
    assert len(replica.prepares) == 0
    assert len(replica.sentPrePrepares) == 0
    assert len(replica.batches) == 0
    assert len(replica.commits) == 0
    assert len(replica.commitsWaitingForPrepare) == 0
    assert len(replica.ordered) == 0


def check_all_replica_queue_empty(nodes):
    for node in nodes:
        check_replica_queue_empty(node)


def ensure_view_change_complete(looper, nodes, exclude_from_check=None,
                                customTimeout=None):
    ensure_view_change(looper, nodes)
    ensureElectionsDone(looper=looper, nodes=nodes,
                        customTimeout=customTimeout)
    ensure_all_nodes_have_same_data(looper, nodes, customTimeout,
                                    exclude_from_check=exclude_from_check)


def ensure_view_change_complete_by_primary_restart(
        looper, nodes, tconf, tdirWithPoolTxns, allPluginsPath):
    nodes = ensure_view_change_by_primary_restart(
        looper, nodes, tconf, tdirWithPoolTxns, allPluginsPath)
    ensureElectionsDone(looper=looper, nodes=nodes)
    ensure_all_nodes_have_same_data(looper, nodes)
    return nodes


def view_change_in_between_3pc(looper, nodes, slow_nodes,
                               sdk_pool_handle,
                               sdk_wallet_client,
                               slow_delay=1, wait=None):
    sdk_send_random_and_check(looper, nodes, sdk_pool_handle, sdk_wallet_client, 4)
    delay_3pc_messages(slow_nodes, 0, delay=slow_delay)

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 10)
    if wait:
        looper.runFor(wait)

    ensure_view_change_complete(looper, nodes, customTimeout=60)

    reset_delays_and_process_delayeds(slow_nodes)

    sdk_send_random_and_check(looper, nodes, sdk_pool_handle,
                              sdk_wallet_client, 5, total_timeout=30)
    sdk_send_random_and_check(looper, nodes, sdk_pool_handle,
                              sdk_wallet_client, 5, total_timeout=30)


def view_change_in_between_3pc_random_delays(
        looper,
        nodes,
        slow_nodes,
        sdk_pool_handle,
        sdk_wallet_client,
        tconf,
        min_delay=0,
        max_delay=0):
    sdk_send_random_and_check(looper, nodes, sdk_pool_handle, sdk_wallet_client, 4)

    # max delay should not be more than catchup timeout.
    max_delay = max_delay or tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE - 1
    delay_3pc_messages(slow_nodes, 0, min_delay=min_delay, max_delay=max_delay)

    sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 10)

    ensure_view_change_complete(looper,
                                nodes,
                                customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT + max_delay,
                                exclude_from_check=['check_last_ordered_3pc'])

    reset_delays_and_process_delayeds(slow_nodes)

    sdk_send_random_and_check(looper, nodes, sdk_pool_handle, sdk_wallet_client, 10)



def add_new_node(looper, nodes, sdk_pool_handle, sdk_wallet_steward,
                 tdir, tconf, all_plugins_path, name=None):
    node_name = name or "Psi"
    new_steward_name = "testClientSteward" + randomString(3)
    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        new_steward_name, node_name, tdir, tconf,
        allPluginsPath=all_plugins_path)
    nodes.append(new_node)
    looper.run(checkNodesConnected(nodes))
    timeout = waits.expectedPoolCatchupTime(nodeCount=len(nodes))
    waitNodeDataEquality(looper, new_node, *nodes[:-1],
                         customTimeout=timeout)
    sdk_pool_refresh(looper, sdk_pool_handle)
    return new_node


def restart_node(looper, txnPoolNodeSet, node_to_disconnect, tconf, tdir,
                 allPluginsPath, wait_node_data_equality=True):
    idx = txnPoolNodeSet.index(node_to_disconnect)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            node_to_disconnect)
    looper.removeProdable(name=node_to_disconnect.name)

    # add node_to_disconnect to pool
    node_to_disconnect = start_stopped_node(node_to_disconnect, looper, tconf,
                                            tdir, allPluginsPath)
    node_to_disconnect.view_changer = create_view_changer(node_to_disconnect, TestViewChanger)

    txnPoolNodeSet[idx] = node_to_disconnect
    looper.run(checkNodesConnected(txnPoolNodeSet))
    if wait_node_data_equality:
        waitNodeDataEquality(looper, node_to_disconnect, *txnPoolNodeSet)


def nodes_received_ic(nodes, frm, view_no=1):
    for n in nodes:
        assert n.view_changer.instance_changes.has_inst_chng_from(view_no,
                                                                 frm.name)
