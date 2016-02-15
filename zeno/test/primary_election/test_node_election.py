# TODO: Complete this test after changing delay to allow delaying all messages
# def testPrimaryReElectionWhenNotAllNodesVote(tdir_for_func):
#     nodeNames = genNodeNames(7)
#     with TestNodeSet(names=nodeNames, tmpdir=tdir_for_func) as nodeSet:
#         with Looper(nodeSet) as looper:
#             prepareNodeSet(looper, nodeSet)
#             nodeA = addNodeBack(nodeSet, looper, nodeNames[0])
#             nodeB = addNodeBack(nodeSet, looper, nodeNames[1])
#             nodeC = addNodeBack(nodeSet, looper, nodeNames[2])
#
#             # Node B and C delay self nomination and nomination from nodes E, F and G
#             for node in [nodeB, nodeC]:
#                 node.delaySelfNomination(20)
#                 for nodeNo in (4, 5, 6):
#                     node.nodestack.delay(delayer(20, NOMINATE, nodeNames[nodeNo]))
#
#             # Node D delays self nomination and nomination from all nodes for a long time. Simulating a condition
#             # where D never nominates itself or anyone else.
#             nodeD = addNodeBack(nodeSet, looper, nodeNames[3])
#             nodeD.delaySelfNomination(120)
#             nodeD.nodestack.delay(delayer(120, NOMINATE))
#
#             nodeE = addNodeBack(nodeSet, looper, nodeNames[4])
#             nodeF = addNodeBack(nodeSet, looper, nodeNames[5])
#             nodeG = addNodeBack(nodeSet, looper, nodeNames[6])
#
#             # Node F and G delay self nomination and nomination from nodes A, B and C
#             for node in [nodeF, nodeG]:
#                 node.delaySelfNomination(20)
#                 for nodeNo in (0, 1, 2):
#                     node.nodestack.delay(delayer(20, NOMINATE, nodeNames[nodeNo]))
#
#             checkPoolReady(looper, nodeSet.nodes.values())
#
#             for node in nodeSet.nodes.values():
#                 for replica in node.replicas:
#                     logging.debug("replica {} {} with votes {}".format(replica.name, replica.instId,
#                                                                        replica.nominations))
#
#             # Checking whether Node A nominated itself
#             looper.runCoros(eventually(checkNomination, nodeA, nodeA.name, retryWait=1, timeout=10))
#
#             # Checking whether Node E nominated itself
#             looper.runCoros(eventually(checkNomination, nodeE, nodeE.name, retryWait=1, timeout=10))


def testPrimaryElectionCase3():
    """
    Case 3 - A node making nominations for a multiple other nodes. Consider 7 nodes A, B, C, D, E, F, G and G.
    Lets say node B is malicious and nominates node B(itself) to node A, C and D and Node F to nodes E, F and G.
    Also say node F is malicious and nominates node F(itself) to node A, C and D and Node B to nodes E, F and G.
    """
    # TODO Implement this

# TODO Add test for checking invalid reelection round number