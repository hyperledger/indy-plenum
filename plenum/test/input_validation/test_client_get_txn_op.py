import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.types import f
from plenum.test.input_validation.constants import \
    TEST_SEQ_SMALL, TEST_SEQ_ONE, TEST_SEQ_NORMAL

from plenum.common.messages.client_request import ClientGetTxnOperation, \
    TXN_TYPE, GET_TXN, DATA

op_get_txn = ClientGetTxnOperation()


def test_no_ledgerId_passes():
    op_get_txn.validate({
        TXN_TYPE: GET_TXN,
        DATA: TEST_SEQ_NORMAL
    })


def test_invalid_ledgerId_fails():
    with pytest.raises(TypeError) as ex_info:
        op_get_txn.validate({
            TXN_TYPE: GET_TXN,
            f.LEDGER_ID.nm: 300,
            DATA: TEST_SEQ_NORMAL
        })
    ex_info.match(r'expected one of')


def test_small_seq_no_fails():
    op = {
        TXN_TYPE: GET_TXN,
        DATA: TEST_SEQ_SMALL
    }
    with pytest.raises(TypeError) as ex_info:
        op_get_txn.validate(op)
    ex_info.match(r'cannot be smaller than 1')


def test_one_seq_no_passes():
    op = {
        TXN_TYPE: GET_TXN,
        DATA: TEST_SEQ_ONE
    }
    op_get_txn.validate(op)
    op[f.LEDGER_ID.nm] = DOMAIN_LEDGER_ID
    op_get_txn.validate(op)


def test_normal_seq_no_passes():
    op = {
        TXN_TYPE: GET_TXN,
        DATA: TEST_SEQ_NORMAL
    }
    op_get_txn.validate(op)
    op[f.LEDGER_ID.nm] = DOMAIN_LEDGER_ID
    op_get_txn.validate(op)
