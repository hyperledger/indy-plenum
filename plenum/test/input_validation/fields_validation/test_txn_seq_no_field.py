from plenum.common.messages.fields import TxnSeqNoField

validator = TxnSeqNoField()


def test_valid_txn_seq_no():
    assert validator.validate(-1) == "cannot be smaller than 1"
    assert validator.validate(0) == "cannot be smaller than 1"
    assert validator.validate(2.2) == "expected types 'int', got 'float'"
    assert validator.validate('') == "expected types 'int', got 'str'"
    assert validator.validate(1) is None
    assert validator.validate(200) is None
