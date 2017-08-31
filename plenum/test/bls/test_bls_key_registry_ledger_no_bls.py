import pytest
from plenum.bls.bls_key_register_pool_ledger import BlsKeyRegisterPoolLedger

nodeCount = 4
nodes_wth_bls = 0


@pytest.fixture()
def bls_key_register_ledger():
    return BlsKeyRegisterPoolLedger()


def test_load_keys_no_bls_keys(bls_key_register_ledger, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        ledger = node.poolLedger
        bls_key_register_ledger.load_latest_keys(ledger)
        assert len(bls_key_register_ledger._bls_key_registry) == 0
