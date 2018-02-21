import pytest


@pytest.yield_fixture(scope="function")
def ledger_manager_and_info(txnPoolNodeSet):
    ledger_manager = txnPoolNodeSet[0].ledgerManager

    ledger_info = ledger_manager.getLedgerInfoByType(1)
    ledger_info.set_defaults()

    return ledger_manager, ledger_info


def test_empty(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None


def test_1_none(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info
    ledger_info.last_txn_3PC_key['1'] = (None, None)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None


def test_non_quorum(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info
    ledger_info.last_txn_3PC_key['1'] = (None, None)
    ledger_info.last_txn_3PC_key['2'] = (None, None)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None


def test_semi_none(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info
    ledger_info.last_txn_3PC_key['1'] = (1, None)
    ledger_info.last_txn_3PC_key['2'] = (None, 1)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None


def test_quorum_1_value(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (1, 1)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None

    ledger_info.last_txn_3PC_key['2'] = (1, 1)
    assert (1, 1) == ledger_manager._get_last_txn_3PC_key(ledger_info)


def test_quorum_2_values(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (1, 1)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None

    ledger_info.last_txn_3PC_key['2'] = (2, 1)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None

    ledger_info.last_txn_3PC_key['3'] = (1, 2)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None

    ledger_info.last_txn_3PC_key['4'] = (1, 1)
    assert (1, 1) == ledger_manager._get_last_txn_3PC_key(ledger_info)


def test_quorum_min_value1(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (2, 1)
    ledger_info.last_txn_3PC_key['2'] = (2, 1)
    ledger_info.last_txn_3PC_key['3'] = (1, 3)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    assert (1, 3) == ledger_manager._get_last_txn_3PC_key(ledger_info)


def test_quorum_min_value2(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (1, 1)
    ledger_info.last_txn_3PC_key['2'] = (1, 1)
    ledger_info.last_txn_3PC_key['3'] = (1, 3)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    assert (1, 1) == ledger_manager._get_last_txn_3PC_key(ledger_info)


def test_quorum_min_value3(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (1, 1)
    ledger_info.last_txn_3PC_key['2'] = (1, 1)
    ledger_info.last_txn_3PC_key['3'] = (1, 3)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    ledger_info.last_txn_3PC_key['5'] = (1, 3)
    ledger_info.last_txn_3PC_key['6'] = (1, 3)
    assert (1, 1) == ledger_manager._get_last_txn_3PC_key(ledger_info)


def test_quorum_with_none1(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (None, None)
    ledger_info.last_txn_3PC_key['2'] = (1, None)
    ledger_info.last_txn_3PC_key['3'] = (None, 1)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None

    ledger_info.last_txn_3PC_key['5'] = (1, 3)
    assert (1, 3) == ledger_manager._get_last_txn_3PC_key(ledger_info)


def test_quorum_with_none2(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (None, None)
    ledger_info.last_txn_3PC_key['2'] = (1, None)
    ledger_info.last_txn_3PC_key['3'] = (None, 1)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    ledger_info.last_txn_3PC_key['5'] = (1, None)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None


def test_quorum_with_none3(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (None, None)
    ledger_info.last_txn_3PC_key['2'] = (1, None)
    ledger_info.last_txn_3PC_key['3'] = (None, 1)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    ledger_info.last_txn_3PC_key['5'] = (None, 1)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None


def test_quorum_with_none4(ledger_manager_and_info):
    ledger_manager, ledger_info = ledger_manager_and_info

    ledger_info.last_txn_3PC_key['1'] = (None, None)
    ledger_info.last_txn_3PC_key['2'] = (1, None)
    ledger_info.last_txn_3PC_key['3'] = (None, 1)
    ledger_info.last_txn_3PC_key['4'] = (1, 3)
    ledger_info.last_txn_3PC_key['5'] = (None, None)
    assert ledger_manager._get_last_txn_3PC_key(ledger_info) is None
