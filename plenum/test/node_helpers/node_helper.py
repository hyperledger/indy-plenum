from plenum.test.helper import TestNode


def getProtocolInstanceNums(node: TestNode):
    return [node.instances.masterId, *node.instances.backupIds]
