import pytest

from plenum.test.input_validation.constants import \
    TEST_SEQ_NEGATIVE

from plenum.common.messages.client_request import ClientGetTxnOperation, \
    TXN_TYPE, GET_TXN, DATA

op_get_txn = ClientGetTxnOperation()


def test_negative_seq_no_fails():
    with pytest.raises(TypeError) as ex_info:
        op_get_txn.validate({
            TXN_TYPE: GET_TXN,
            DATA: TEST_SEQ_NEGATIVE

        })
    ex_info.match(r'cannot be smaller than 1')

