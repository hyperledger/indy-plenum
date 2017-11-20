import pytest

from plenum.test.delayers import reset_delays_and_process_delayeds, lsDelay, \
    reset_delays_and_process_delayeds_for_client
from plenum.test.exceptions import TestException
from plenum.test.helper import random_requests
from plenum.test.pool_transactions.helper import buildPoolClientAndWallet
from stp_core.loop.eventually import eventually
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected, steward1, stewardWallet, stewardAndWallet1


def new_client(poolTxnClientData, tdirWithPoolTxns):
    client, wallet = buildPoolClientAndWallet(poolTxnClientData,
                                              tdirWithPoolTxns)
    return (client, wallet)


def slow_catch_up(nodes, timeout):
    for node in nodes:
        node.nodeIbStasher.delay(lsDelay(timeout))
        node.clientIbStasher.delay(lsDelay(timeout))


def cancel_slow_catch_up(nodes):
    reset_delays_and_process_delayeds(nodes)
    reset_delays_and_process_delayeds_for_client(nodes)


def can_send_write_requests(client):
    if not client.can_send_write_requests():
        raise TestException('Client must be able to send write requests')


def can_send_read_requests(client):
    if not client.can_send_read_requests():
        raise TestException('Client must be able to send read requests')


def can_send_request(client, req):
    if not client.can_send_request(req):
        raise TestException('Client must be able to send a request {}'.format(req))


def test_client_can_not_send_write_requests_not_started(poolTxnClientData, tdirWithPoolTxns):
    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    assert not client.can_send_write_requests()


def test_client_can_not_send_read_requests_not_started(poolTxnClientData, tdirWithPoolTxns):
    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    req = random_requests(1)[0]
    assert not client.can_send_request(req)


def test_client_can_not_send_requests_not_started(poolTxnClientData, tdirWithPoolTxns):
    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    assert not client.can_send_read_requests()


def test_client_can_send_write_requests_no_catchup(looper,
                                                   poolTxnClientData, tdirWithPoolTxns,
                                                   txnPoolNodeSet):
    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    looper.add(client)
    looper.run(eventually(can_send_write_requests, client))


def test_client_can_send_read_requests_no_catchup(looper,
                                                  poolTxnClientData, tdirWithPoolTxns,
                                                  txnPoolNodeSet):
    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    looper.add(client)
    looper.run(eventually(can_send_read_requests, client))


def test_client_can_send_request_no_catchup(looper,
                                            poolTxnClientData, tdirWithPoolTxns,
                                            txnPoolNodeSet):
    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    looper.add(client)
    req = random_requests(1)[0]
    looper.run(eventually(can_send_request, client, req))


def test_client_can_not_send_write_requests_until_catchup(looper,
                                                          poolTxnClientData, tdirWithPoolTxns,
                                                          txnPoolNodeSet):
    slow_catch_up(txnPoolNodeSet, 60)

    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    looper.add(client)

    with pytest.raises(TestException):
        looper.run(eventually(can_send_write_requests, client, timeout=4))

    cancel_slow_catch_up(txnPoolNodeSet)
    looper.run(eventually(can_send_write_requests, client))


def test_client_can_send_read_requests_until_catchup(looper,
                                                     poolTxnClientData, tdirWithPoolTxns,
                                                     txnPoolNodeSet):
    slow_catch_up(txnPoolNodeSet, 60)

    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    looper.add(client)

    with pytest.raises(TestException):
        looper.run(eventually(can_send_read_requests, client, timeout=4))

    cancel_slow_catch_up(txnPoolNodeSet)
    looper.run(eventually(can_send_read_requests, client))


def test_client_can_send_request_until_catchup(looper,
                                               poolTxnClientData, tdirWithPoolTxns,
                                               txnPoolNodeSet):
    slow_catch_up(txnPoolNodeSet, 60)

    client, _ = new_client(poolTxnClientData, tdirWithPoolTxns)
    looper.add(client)
    req = random_requests(1)[0]

    with pytest.raises(TestException):
        looper.run(eventually(can_send_request, client, req, timeout=4))

    cancel_slow_catch_up(txnPoolNodeSet)
    looper.run(eventually(can_send_request, client, req))
