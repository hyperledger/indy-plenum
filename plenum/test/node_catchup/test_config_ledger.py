import json

import pytest

from plenum.test.node_catchup.helper import waitNodeDataInequality, \
    waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected

from plenum.common.util import randomString
from plenum.test.helper import send_signed_requests, sign_requests, \
    waitForSufficientRepliesForRequests, check_sufficient_replies_received

from plenum.common.constants import CONFIG_LEDGER_ID, DATA
from plenum.test.test_config_req_handler import write_conf_op, \
    TestConfigReqHandler, WRITE_CONF, READ_CONF, read_conf_op
from plenum.test.test_node import TestNode
from stp_core.loop.eventually import eventually


class NewTestNode(TestNode):
    def getConfigReqHandler(self):
        return TestConfigReqHandler(self.configLedger,
                                    self.states[CONFIG_LEDGER_ID])


def write(key, val, looper, client, wallet):
    reqs = send_signed_requests(client,
                                sign_requests(wallet,
                                              [write_conf_op(key, val)]))
    waitForSufficientRepliesForRequests(looper, client,
                                        requests=reqs)


def read(key, looper, client, wallet):
    req, = send_signed_requests(client, sign_requests(wallet,
                                                      [read_conf_op(key)]))
    resp = looper.run(eventually(check_sufficient_replies_received, client,
                                 req.identifier, req.reqId, retryWait=1))
    return json.loads(resp[DATA])[key]


def send_some_config_txns(looper, client, wallet, keys):
    for i in range(5):
        key, val = 'key_{}'.format(i+1), randomString()
        write(key, val, looper, client, wallet)
        keys[key] = val
    return keys


@pytest.fixture(scope="module")
def testNodeClass(patchPluginManager):
    return NewTestNode


@pytest.fixture(scope="module")
def setup(testNodeClass, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        ca = node.clientAuthNr.core_authenticator
        ca.write_types.add(WRITE_CONF)
        ca.query_types.add(READ_CONF)


def test_config_ledger_txns(looper, setup, txnPoolNodeSet, wallet1,
                            client1, client1Connected):
    """
    Do some writes and reads on the config ledger
    """
    old_config_ledger_size = None
    for node in txnPoolNodeSet:
        if old_config_ledger_size is None:
            old_config_ledger_size = len(node.getLedger(CONFIG_LEDGER_ID))
        else:
            assert len(node.getLedger(CONFIG_LEDGER_ID)) == old_config_ledger_size

    # Do a write txn
    key, val = 'test_key', 'test_val'
    write(key, val, looper, client1, wallet1)

    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    assert read(key, looper, client1, wallet1) == val
    old_config_ledger_size += 1

    key, val = 'test_key', 'test_val1'
    write(key, val, looper, client1, wallet1)
    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    assert read(key, looper, client1, wallet1) == val
    old_config_ledger_size += 1

    key, val = 'test_key1', 'test_val11'
    write(key, val, looper, client1, wallet1)
    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    assert read(key, looper, client1, wallet1) == val


@pytest.fixture(scope="module")
def keys():
    return {}


@pytest.fixture(scope="module")
def some_config_txns_done(looper, setup, txnPoolNodeSet, wallet1,
                          client1, client1Connected, keys):
    return send_some_config_txns(looper, client1, wallet1, keys)


def test_new_node_catchup_config_ledger(looper, some_config_txns_done,
                                        txnPoolNodeSet, wallet1, client1,
                                        client1Connected, newNodeCaughtUp):
    """
    A new node catches up the config ledger too
    """
    assert len(newNodeCaughtUp.getLedger(CONFIG_LEDGER_ID)) >= \
           len(some_config_txns_done)


def test_disconnected_node_catchup_config_ledger_txns(looper,
                                                      some_config_txns_done,
                                                      txnPoolNodeSet, wallet1,
                                                      client1, client1Connected,
                                                      newNodeCaughtUp, keys):
    """
    A node gets disconnected, a few config ledger txns happen,
    the disconnected node comes back up and catches up the config ledger
    """
    new_node = newNodeCaughtUp
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, new_node, stopNode=False)
    looper.removeProdable(new_node)

    # Do some config txns; using a fixture as a method, passing some arguments
    # as None as they only make sense for the fixture (pre-requisites)
    send_some_config_txns(looper, client1, wallet1, keys)

    # Make sure new node got out of sync
    waitNodeDataInequality(looper, new_node, *txnPoolNodeSet[:-1])

    looper.add(new_node)
    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, new_node)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
