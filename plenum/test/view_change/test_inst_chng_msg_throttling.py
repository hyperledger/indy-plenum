import types


def testInstChngMsgThrottling(nodeSet, looper, up, viewNo):
    """
    2 nodes out of 4 keep on sending INSTANCE_CHANGE messages as they
    find the master to be slow but since we need 3 out of 4 (n-f) to say that
    master is slow for a view change to happen, a view change does not happen
    but the nodes finding the master to be slow should not send INSTANCE_CHANGE
    messages to often. So nodes should throttle sending INSTANCE_CHANGE messages

    THE TEST BELOW SHOULD TERMINATE. IF IT DOES NOT TERMINATE THEN THE BUG IS
    STILL PRESENT
    """
    nodeA = nodeSet.Alpha
    nodeB = nodeSet.Beta
    # Nodes that always find master as degraded
    for node in (nodeA, nodeB):
        node.monitor.isMasterDegraded = types.MethodType(
            lambda x: True, node.monitor)
    for node in (nodeA, nodeB):
        for i in range(5):
            node.view_changer.sendInstanceChange(viewNo)
        looper.runFor(.2)
