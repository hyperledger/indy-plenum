import types

from plenum.test.helper import checkViewNoForNodes, sendRandomRequests, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import get_master_primary_node
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.test import waits

logger = getlogger()


def provoke_and_check_view_change(nodes, newViewNo, wallet, client):

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
        sendRandomRequests(wallet, client, 10)
        assert False


def provoke_and_wait_for_view_change(looper,
                                     nodeSet,
                                     expectedViewNo,
                                     wallet,
                                     client,
                                     customTimeout=None):
    timeout = customTimeout or waits.expectedPoolViewChangeStartedTimeout(len(nodeSet))
    # timeout *= 30
    return looper.run(eventually(provoke_and_check_view_change,
                                 nodeSet,
                                 expectedViewNo,
                                 wallet,
                                 client,
                                 timeout=timeout))


def ensure_view_change(looper, nodes, exclude_from_check=None):
    """
    This method patches the master performance check to return False and thus
    ensures that all given nodes do a view change
    """
    old_view_no = checkViewNoForNodes(nodes)

    old_meths = {}
    view_changes = {}
    for node in nodes:
        old_meths[node.name] = node.monitor.isMasterDegraded
        view_changes[node.name] = node.monitor.totalViewChanges

        def slow_master(self):
            # Only allow one view change
            rv = self.totalViewChanges == view_changes[self.name]
            if rv:
                logger.info('{} making master look slow'.format(self))
            return rv

        node.monitor.isMasterDegraded = types.MethodType(slow_master, node.monitor)

    perf_check_freq = next(iter(nodes)).config.PerfCheckFreq
    timeout = waits.expectedPoolViewChangeStartedTimeout(len(nodes)) + \
              perf_check_freq
    nodes_to_check = nodes if exclude_from_check is None else [n for n in nodes
                                                               if n not in exclude_from_check]
    looper.run(eventually(checkViewNoForNodes, nodes_to_check, old_view_no+1,
                          retryWait=1, timeout=timeout))
    for node in nodes:
        node.monitor.isMasterDegraded = old_meths[node.name]
    return old_view_no + 1


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
        vals[node.name] = node.replicas[0].view_ends_at[view_no-1]

    arg = list(args.values())[0]
    for a in args.values():
        assert a == arg

    val = list(args.values())[0]
    for v in vals.values():
        assert v == val


def do_vc(looper, nodes, client, wallet, old_view_no=None):
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    new_view_no = ensure_view_change(looper, nodes)
    if old_view_no:
        assert new_view_no - old_view_no >= 1
    return new_view_no


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
