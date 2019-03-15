import pytest


@pytest.yield_fixture(scope="function")
def cons_proof_service(txnPoolNodeSet):
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    service = ledger_manager._node_leecher._leechers[1]._cons_proof_service
    service.start(request_ledger_statuses=False)
    return service


def test_empty(cons_proof_service):
    assert cons_proof_service._get_last_txn_3PC_key() is None


def test_1_none(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (None, None)
    assert cons_proof_service._get_last_txn_3PC_key() is None


def test_non_quorum(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (None, None)
    cons_proof_service._last_txn_3PC_key['2'] = (None, None)
    assert cons_proof_service._get_last_txn_3PC_key() is None


def test_semi_none(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (1, None)
    cons_proof_service._last_txn_3PC_key['2'] = (None, 1)
    assert cons_proof_service._get_last_txn_3PC_key() is None


def test_quorum_1_value(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (1, 1)
    assert cons_proof_service._get_last_txn_3PC_key() is None

    cons_proof_service._last_txn_3PC_key['2'] = (1, 1)
    assert (1, 1) == cons_proof_service._get_last_txn_3PC_key()


def test_quorum_2_values(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (1, 1)
    assert cons_proof_service._get_last_txn_3PC_key() is None

    cons_proof_service._last_txn_3PC_key['2'] = (2, 1)
    assert cons_proof_service._get_last_txn_3PC_key() is None

    cons_proof_service._last_txn_3PC_key['3'] = (1, 2)
    assert cons_proof_service._get_last_txn_3PC_key() is None

    cons_proof_service._last_txn_3PC_key['4'] = (1, 1)
    assert (1, 1) == cons_proof_service._get_last_txn_3PC_key()


def test_quorum_min_value1(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (2, 1)
    cons_proof_service._last_txn_3PC_key['2'] = (2, 1)
    cons_proof_service._last_txn_3PC_key['3'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    assert (1, 3) == cons_proof_service._get_last_txn_3PC_key()


def test_quorum_min_value2(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (1, 1)
    cons_proof_service._last_txn_3PC_key['2'] = (1, 1)
    cons_proof_service._last_txn_3PC_key['3'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    assert (1, 1) == cons_proof_service._get_last_txn_3PC_key()


def test_quorum_min_value3(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (1, 1)
    cons_proof_service._last_txn_3PC_key['2'] = (1, 1)
    cons_proof_service._last_txn_3PC_key['3'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['5'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['6'] = (1, 3)
    assert (1, 1) == cons_proof_service._get_last_txn_3PC_key()


def test_quorum_with_none1(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (None, None)
    cons_proof_service._last_txn_3PC_key['2'] = (1, None)
    cons_proof_service._last_txn_3PC_key['3'] = (None, 1)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    assert cons_proof_service._get_last_txn_3PC_key() is None

    cons_proof_service._last_txn_3PC_key['5'] = (1, 3)
    assert (1, 3) == cons_proof_service._get_last_txn_3PC_key()


def test_quorum_with_none2(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (None, None)
    cons_proof_service._last_txn_3PC_key['2'] = (1, None)
    cons_proof_service._last_txn_3PC_key['3'] = (None, 1)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['5'] = (1, None)
    assert cons_proof_service._get_last_txn_3PC_key() is None


def test_quorum_with_none3(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (None, None)
    cons_proof_service._last_txn_3PC_key['2'] = (1, None)
    cons_proof_service._last_txn_3PC_key['3'] = (None, 1)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['5'] = (None, 1)
    assert cons_proof_service._get_last_txn_3PC_key() is None


def test_quorum_with_none4(cons_proof_service):
    cons_proof_service._last_txn_3PC_key['1'] = (None, None)
    cons_proof_service._last_txn_3PC_key['2'] = (1, None)
    cons_proof_service._last_txn_3PC_key['3'] = (None, 1)
    cons_proof_service._last_txn_3PC_key['4'] = (1, 3)
    cons_proof_service._last_txn_3PC_key['5'] = (None, None)
    assert cons_proof_service._get_last_txn_3PC_key() is None
