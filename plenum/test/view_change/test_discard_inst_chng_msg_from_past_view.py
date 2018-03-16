from stp_core.loop.eventually import eventually
from plenum.server.view_change.view_changer import ViewChanger
from plenum.test import waits
from plenum.test.helper import checkDiscardMsg, waitForViewChange


# noinspection PyIncorrectDocstring
def testDiscardInstChngMsgFrmPastView(txnPoolNodeSet, looper, ensureView):
    """
    Once a view change is done, any further INSTANCE_CHANGE messages for that
    view must be discarded by the node.
    """

    curViewNo = ensureView

    # Send an instance change for an old instance message to all nodes
    icMsg = txnPoolNodeSet[0].view_changer._create_instance_change_msg(curViewNo, 0)
    txnPoolNodeSet[0].send(icMsg)

    # ensure every node but Alpha discards the invalid instance change request
    timeout = waits.expectedPoolViewChangeStartedTimeout(len(txnPoolNodeSet))

    # Check that that message is discarded.
    looper.run(eventually(checkDiscardMsg, txnPoolNodeSet, icMsg,
                          'which is not more than its view no',
                          txnPoolNodeSet[0], timeout=timeout))

    waitForViewChange(looper, txnPoolNodeSet)


# noinspection PyIncorrectDocstring
def testDoNotSendInstChngMsgIfMasterDoesntSeePerformanceProblem(
        txnPoolNodeSet, looper, ensureView):
    """
    A node that received an INSTANCE_CHANGE message must not send an
    INSTANCE_CHANGE message if it doesn't observe too much difference in
    performance between its replicas.
    """

    curViewNo = ensureView

    # Count sent instance changes of all nodes
    sentInstChanges = {}
    instChngMethodName = ViewChanger.sendInstanceChange.__name__
    for n in txnPoolNodeSet:
        sentInstChanges[n.name] = n.view_changer.spylog.count(instChngMethodName)

    # Send an instance change message to all nodes
    icMsg = txnPoolNodeSet[0].view_changer._create_instance_change_msg(curViewNo, 0)
    txnPoolNodeSet[0].send(icMsg)

    # Check that that message is discarded.
    waitForViewChange(looper, txnPoolNodeSet)
    # No node should have sent a view change and thus must not have called
    # `sendInstanceChange`
    for n in txnPoolNodeSet:
        assert n.spylog.count(instChngMethodName) == \
               sentInstChanges.get(n.name, 0)
