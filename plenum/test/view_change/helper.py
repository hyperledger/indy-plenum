import types

from plenum.test.helper import checkViewNoForNodes, sendRandomRequests, \
    sendReqsToNodesAndVerifySuffReplies
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


def ensure_view_change(looper, nodes, client, wallet):
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 2)
    old_view_no = checkViewNoForNodes(nodes)

    old_meths = {}
    view_changes = {}
    for node in nodes:
        old_meths[node.name] = node.monitor.isMasterDegraded
        view_changes[node.name] = node.monitor.totalViewChanges

        def slow_master(self):
            # Only allow one view change
            return self.totalViewChanges == view_changes[self.name]

        node.monitor.isMasterDegraded = types.MethodType(slow_master, node.monitor)

    timeout = waits.expectedPoolViewChangeStartedTimeout(len(nodes)) + \
              client.config.PerfCheckFreq
    looper.run(eventually(checkViewNoForNodes, nodes, old_view_no+1,
                          retryWait=1, timeout=timeout))
    for node in nodes:
        node.monitor.isMasterDegraded = old_meths[node.name]
    return old_view_no + 1
