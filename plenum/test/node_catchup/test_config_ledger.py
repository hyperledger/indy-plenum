import pytest
from plenum.test.helper import send_signed_requests, sign_requests, \
    waitForSufficientRepliesForRequests

from plenum.common.constants import CONFIG_LEDGER_ID
from plenum.test.test_config_req_handler import write_conf_op, \
    TestConfigReqHandler, WRITE_CONF, READ_CONF
from plenum.test.test_node import TestNode


class NewTestNode(TestNode):
    def getConfigReqHandler(self):
        return TestConfigReqHandler(self.configLedger,
                                self.states[CONFIG_LEDGER_ID])


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
                            client1, client1Connected, tconf, tdirWithPoolTxns):
    old_config_ledger_size = None
    for node in txnPoolNodeSet:
        if old_config_ledger_size is None:
            old_config_ledger_size = len(node.getLedger(CONFIG_LEDGER_ID))
        else:
            assert len(node.getLedger(CONFIG_LEDGER_ID)) == old_config_ledger_size

    # Do a write txn
    key, val = 'test_key', 'test_val'
    reqs = send_signed_requests(client1,
                               sign_requests(wallet1, [write_conf_op(key, val)]))
    waitForSufficientRepliesForRequests(looper, client1,
                                        requests=reqs)

    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    # Do a read txn
    #TODO:


# TODO: DO catchup txns