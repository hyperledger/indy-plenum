from plenum.test.storage.helper import checkReplyIsPersisted


def testReplyPersistedInLedger(nodeSet, looper, replied1):
    checkReplyIsPersisted(nodeSet, looper, replied1)
