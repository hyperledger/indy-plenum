import pytest

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies


def test_observer_registration(looper, nodeSet, up, client1):
    def callable1(*args, **kwargs):
        print(1)
        print(args)
        print(kwargs)

    def callable2(*args, **kwargs):
        print(2)
        print(args)
        print(kwargs)

    client1.registerObserver(callable1, name='first')
    assert len(client1._observers) == 1
    assert client1.hasObserver(callable1)

    # Error when registering callable again
    with pytest.raises(RuntimeError):
        client1.registerObserver(callable1)
    assert len(client1._observers) == 1

    # Error when registering different callable with same name
    with pytest.raises(RuntimeError):
        client1.registerObserver(callable2, name='first')
    assert len(client1._observers) == 1

    # Register observer without name
    client1.registerObserver(callable2)
    assert len(client1._observers) == 2

    with pytest.raises(RuntimeError):
        client1.registerObserver(callable2)
    assert len(client1._observers) == 2

    client1.deregisterObserver('first')
    assert len(client1._observers) == 1
    assert not client1.hasObserver(callable1)

    with pytest.raises(RuntimeError):
        client1.deregisterObserver('first')
    assert len(client1._observers) == 1
    with pytest.raises(RuntimeError):
        client1.deregisterObserver('random_name')
    assert len(client1._observers) == 1


def test_observer_execution(looper, nodeSet, up, client1, wallet1):
    resp1 = []
    resp2 = []

    def callable1(name, reqId, frm, result, numReplies):
        resp1.append(reqId)
        return reqId

    def callable2(name, reqId, frm, result, numReplies):
        resp2.append(reqId)
        return reqId

    client1.registerObserver(callable1, name='first')
    client1.registerObserver(callable2)

    # Send 1 request
    req, = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    # Each observer is called only once
    assert len(resp1) == 1
    assert len(resp2) == 1
    assert resp1[0] == req.reqId
    assert resp2[0] == req.reqId

    client1.deregisterObserver('first')
    # Send another request
    req1, = sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 1)
    # Only 1 observer is called
    assert len(resp1) == 1
    assert len(resp2) == 2
    assert resp1[-1] == req.reqId
    assert resp2[-1] == req1.reqId
