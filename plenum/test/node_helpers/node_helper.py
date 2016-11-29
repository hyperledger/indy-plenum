from plenum.test.test_node import TestNode


def getProtocolInstanceNums(node: TestNode):
    return [node.instances.masterId, *node.instances.backupIds]
