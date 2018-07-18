import json

import pytest

from common.serializers.serialization import state_roots_serializer
from plenum.common.config_helper import PNodeConfigHelper
from plenum.test.node_catchup.helper import waitNodeDataInequality, \
    waitNodeDataEquality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected

from plenum.common.util import randomString
from plenum.test.helper import sdk_gen_request, sdk_sign_request_objects, \
    sdk_send_signed_requests, sdk_get_replies

from plenum.common.constants import CONFIG_LEDGER_ID, DATA
from plenum.test.test_config_req_handler import write_conf_op, \
    TestConfigReqHandler, WRITE_CONF, READ_CONF, read_conf_op
from plenum.test.test_node import TestNode, checkNodesConnected
from stp_core.loop.eventually import eventually
from stp_core.types import HA


class NewTestNode(TestNode):
    def getConfigReqHandler(self):
        return TestConfigReqHandler(self.configLedger,
                                    self.states[CONFIG_LEDGER_ID])


def write(key, val, looper, sdk_pool_handle, sdk_wallet):
    _, idr = sdk_wallet
    reqs_obj = [sdk_gen_request(op, identifier=idr)
                for op in [write_conf_op(key, val)]]
    reqs = sdk_sign_request_objects(looper, sdk_wallet, reqs_obj)
    sent_reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    sdk_get_replies(looper, sent_reqs, timeout=10)


def read(key, looper, sdk_pool_handle, sdk_wallet):
    _, idr = sdk_wallet
    reqs_obj = [sdk_gen_request(op, identifier=idr)
                for op in [read_conf_op(key)]]
    reqs = sdk_sign_request_objects(looper, sdk_wallet, reqs_obj)
    sent_reqs = sdk_send_signed_requests(sdk_pool_handle, reqs)
    (req, resp), = sdk_get_replies(looper, sent_reqs, timeout=10)
    return json.loads(resp['result'][DATA])[key]


def send_some_config_txns(looper, sdk_pool_handle, sdk_wallet_client, keys):
    for i in range(5):
        key, val = 'key_{}'.format(i + 1), randomString()
        write(key, val, looper, sdk_pool_handle, sdk_wallet_client)
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


def test_config_ledger_txns(looper, setup, txnPoolNodeSet, sdk_wallet_client,
                            sdk_pool_handle):
    """
    Do some writes and reads on the config ledger
    """
    old_config_ledger_size = None
    old_bls_store_size = None
    state_root_hashes = set()
    state = txnPoolNodeSet[0].getState(CONFIG_LEDGER_ID)
    for node in txnPoolNodeSet:
        if old_config_ledger_size is None:
            old_config_ledger_size = len(node.getLedger(CONFIG_LEDGER_ID))
            old_bls_store_size = node.bls_bft.bls_store._kvs.size
        else:
            assert len(node.getLedger(CONFIG_LEDGER_ID)) == old_config_ledger_size
            assert node.bls_bft.bls_store._kvs.size == old_bls_store_size

    # Do a write txn
    key, val = 'test_key', 'test_val'
    write(key, val, looper, sdk_pool_handle, sdk_wallet_client)

    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    state_root_hashes.add(state_roots_serializer.serialize(state.committedHeadHash))

    assert read(key, looper, sdk_pool_handle, sdk_wallet_client) == val
    old_config_ledger_size += 1

    key, val = 'test_key', 'test_val1'
    write(key, val, looper, sdk_pool_handle, sdk_wallet_client)
    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    state_root_hashes.add(state_roots_serializer.serialize(state.committedHeadHash))

    assert read(key, looper, sdk_pool_handle, sdk_wallet_client) == val
    old_config_ledger_size += 1

    key, val = 'test_key1', 'test_val11'
    write(key, val, looper, sdk_pool_handle, sdk_wallet_client)
    for node in txnPoolNodeSet:
        assert len(node.getLedger(CONFIG_LEDGER_ID)) == (old_config_ledger_size + 1)

    state_root_hashes.add(state_roots_serializer.serialize(state.committedHeadHash))

    assert read(key, looper, sdk_pool_handle, sdk_wallet_client) == val

    for node in txnPoolNodeSet:
        # Not all batches might have BLS-sig but at least one of them will have
        assert node.bls_bft.bls_store._kvs.size > old_bls_store_size

        # At least one state root hash should be in the BLS store
        found = False
        for root_hash in state_root_hashes:
            if node.bls_bft.bls_store.get(root_hash) is not None:
                found = True
                break
        assert found


@pytest.fixture(scope="module")
def keys():
    return {}


@pytest.fixture(scope="module")
def some_config_txns_done(looper, setup, txnPoolNodeSet, keys,
                          sdk_wallet_client, sdk_pool_handle):
    return send_some_config_txns(looper, sdk_pool_handle, sdk_wallet_client, keys)


def start_stopped_node(stopped_node, looper, tconf,
                       tdir, allPluginsPath,
                       delay_instance_change_msgs=True):
    nodeHa, nodeCHa = HA(*
                         stopped_node.nodestack.ha), HA(*
                                                        stopped_node.clientstack.ha)
    config_helper = PNodeConfigHelper(stopped_node.name, tconf, chroot=tdir)
    restarted_node = NewTestNode(stopped_node.name,
                                 config_helper=config_helper,
                                 config=tconf,
                                 ha=nodeHa, cliha=nodeCHa,
                                 pluginPaths=allPluginsPath)
    looper.add(restarted_node)
    return restarted_node


def test_new_node_catchup_config_ledger(looper, some_config_txns_done,
                                        txnPoolNodeSet, sdk_new_node_caught_up):
    """
    A new node catches up the config ledger too
    """
    assert len(sdk_new_node_caught_up.getLedger(CONFIG_LEDGER_ID)) >= \
           len(some_config_txns_done)


def test_restarted_node_catches_up_config_ledger_txns(looper,
                                                      some_config_txns_done,
                                                      txnPoolNodeSet,
                                                      sdk_wallet_client,
                                                      sdk_pool_handle,
                                                      sdk_new_node_caught_up,
                                                      keys,
                                                      tconf,
                                                      tdir,
                                                      allPluginsPath):
    """
    A node is stopped, a few config ledger txns happen,
    the stopped node is started and catches up the config ledger
    """
    new_node = sdk_new_node_caught_up
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, new_node, stopNode=True)
    looper.removeProdable(new_node)

    # Do some config txns; using a fixture as a method, passing some arguments
    # as None as they only make sense for the fixture (pre-requisites)
    send_some_config_txns(looper, sdk_pool_handle, sdk_wallet_client, keys)

    # Make sure new node got out of sync
    for node in txnPoolNodeSet[:-1]:
        assert new_node.configLedger.size < node.configLedger.size

    restarted_node = start_stopped_node(new_node, looper, tconf, tdir,
                                        allPluginsPath)
    txnPoolNodeSet[-1] = restarted_node
    looper.run(checkNodesConnected(txnPoolNodeSet))

    waitNodeDataEquality(looper, restarted_node, *txnPoolNodeSet[:-1])
