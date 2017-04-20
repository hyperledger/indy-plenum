from plenum.test.helper import checkViewNoForNodes, sendRandomRequests
from stp_core.common.log import getlogger

logger = getlogger()


def chkViewChange(nodes, newViewNo, wallet, client):
    # verify all nodes have undergone an instance change
    if {n.viewNo for n in nodes} != {newViewNo}:
        tr = []
        for n in nodes:
            tr.append(n.monitor.isMasterThroughputTooLow())
        # If throughput of every node has gone down then check that
        # view has changed
        if all(tr):
            logger.debug('Throughput ratio gone down')
            checkViewNoForNodes(nodes, newViewNo)
        else:
            logger.debug('Master instance has not degraded yet, '
                         'sending more requests')
            sendRandomRequests(wallet, client, 1)
            assert False
    else:
        assert True
