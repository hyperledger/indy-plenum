import pytest
from plenum.bls.bls import create_default_bls_factory
from plenum.bls.bls_key_register_pool_ledger import BlsKeyRegisterPoolLedger


@pytest.fixture()
def bls_key_register_ledger(bls_serializer):
    return BlsKeyRegisterPoolLedger(bls_serializer)


@pytest.fixture()
def bls_key_register_ledger_loaded(bls_key_register_ledger, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        ledger = node.poolLedger
        bls_key_register_ledger.load_latest_keys(ledger)
    return bls_key_register_ledger


def test_load_keys_bls_keys(bls_key_register_ledger_loaded):
    assert bls_key_register_ledger_loaded._bls_key_registry


def test_get_key(bls_key_register_ledger_loaded, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        bls_key = bls_key_register_ledger_loaded.get_latest_key(node.name)
        assert bls_key
        # check that returned value is deserialized
        assert not isinstance(bls_key, str)
        assert not isinstance(bls_key, bytes)
        assert not isinstance(bls_key, bytearray)


def test_get_unknown(bls_key_register_ledger_loaded):
    bls_key = bls_key_register_ledger_loaded.get_latest_key('UnknownNode')
    assert not bls_key


def test_get_key_for_multisig(bls_key_register_ledger_loaded, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        bls_key = bls_key_register_ledger_loaded.get_latest_key(node.name)
        assert bls_key
        # check that returned value is deserialized
        assert not isinstance(bls_key, str)
        assert not isinstance(bls_key, bytes)
        assert not isinstance(bls_key, bytearray)


def test_replace_key(bls_key_register_ledger_loaded, txnPoolNodeSet):
    node = txnPoolNodeSet[0]

    old_bls_key = bls_key_register_ledger_loaded.get_latest_key(node.name)
    new_bls_key = create_default_bls_factory().generate_bls_keys_as_str()

    bls_key_register_ledger_loaded.add_latest_key(node.name, new_bls_key)

    new_bls_key = bls_key_register_ledger_loaded.get_latest_key(node.name)
    assert old_bls_key != new_bls_key


def test_add_key(bls_key_register_ledger_loaded):
    new_node_name = 'NewNode1'
    new_bls_key = create_default_bls_factory().generate_bls_keys_as_str()
    bls_key_register_ledger_loaded.add_latest_key(new_node_name, new_bls_key)
    assert bls_key_register_ledger_loaded.get_latest_key(new_node_name)


def test_remove_key(bls_key_register_ledger_loaded, txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    bls_key_register_ledger_loaded.remove_latest_key(node.name)
    assert not bls_key_register_ledger_loaded.get_latest_key(node.name)


def test_remove_unknown_key(bls_key_register_ledger_loaded):
    node_name = 'UnknownNode1'
    bls_key_register_ledger_loaded.remove_latest_key(node_name)
    assert not bls_key_register_ledger_loaded.get_latest_key(node_name)
