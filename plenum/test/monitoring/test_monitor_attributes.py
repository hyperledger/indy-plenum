def testHasMasterPrimary(txnPoolNodeSet):
    masterPrimaryCount = 0
    for node in txnPoolNodeSet:
        masterPrimaryCount += int(node.monitor.hasMasterPrimary)
    assert masterPrimaryCount == 1
