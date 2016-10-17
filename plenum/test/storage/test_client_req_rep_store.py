def testReqAcks(replied1, client1):
    reqId = replied1.reqId
    assert len(client1.nodeReg) == len(client1.reqRepStore.getAcks(reqId))
    assert set(client1.nodeReg.keys()) == \
           set(client1.reqRepStore.getAcks(reqId))
