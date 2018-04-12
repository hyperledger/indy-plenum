from plenum.test.storage.helper import checkReplyIsPersisted


def testReplyPersistedInLedger(txnPoolNodeSet, looper, replied1):
    checkReplyIsPersisted(txnPoolNodeSet, looper, replied1)
