def testHasMasterPrimary(nodeSet, up):
    masterPrimaryCount = 0
    for node in nodeSet:
        masterPrimaryCount += int(node.monitor.hasMasterPrimary)
    assert masterPrimaryCount == 1
