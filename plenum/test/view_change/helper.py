import types

from plenum.test.helper import checkViewNoForNodes, sendRandomRequests
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
    timeout = customTimeout or waits.expectedViewChangeTime(len(nodeSet))
    timeout *= 3
    return looper.run(eventually(provoke_and_check_view_change,
                                 nodeSet,
                                 expectedViewNo,
                                 wallet,
                                 client,
                                 retryWait=2,
                                 timeout=timeout))


def elongate_view_change_timeout(conf, request, by=10):
    old = conf.ViewChangeTimeout
    conf.ViewChangeTimeout = old + by

    def reset():
        conf.ViewChangeTimeout = old

    request.addfinalizer(reset)
    return conf


def ensure_view_change(looper, nodes, client, wallet):
    old_view_no = checkViewNoForNodes(nodes)

    old_meths = {}
    for node in nodes:
        old_meths[node.name] = node.monitor.isMasterDegraded

        def patched(self):
            return True

        node.monitor.isMasterDegraded = types.MethodType(patched, node.monitor)

    provoke_and_wait_for_view_change(looper,
                                     nodes,
                                     old_view_no + 1,
                                     wallet,
                                     client)
    for node in nodes:
        node.monitor.isMasterDegraded = old_meths[node.name]
    return old_view_no + 1
