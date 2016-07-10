from plenum.test.cli.helper import assertAllNodesCreated


def testNodeCreateAll(cli, validNodeNames, createAllNodes):
    assertAllNodesCreated(cli, validNodeNames)
