from plenum.common.messages.fields import BlsMultiSignatureField, TimestampField
from plenum.common.util import get_utc_epoch
from plenum.test.input_validation.utils import b58_by_len

validator = BlsMultiSignatureField()

state_root_hash = b58_by_len(32)
pool_state_root_hash = b58_by_len(32)
txn_root_hash = b58_by_len(32)
ledger_id = 1
timestamp = get_utc_epoch()

value = (ledger_id,
         state_root_hash,
         pool_state_root_hash,
         txn_root_hash,
         timestamp)
invalid_value1 = (-1,
                  state_root_hash,
                  pool_state_root_hash,
                  txn_root_hash,
                  timestamp)
invalid_value2 = (ledger_id,
                  b58_by_len(31),
                  pool_state_root_hash,
                  txn_root_hash,
                  timestamp)
invalid_value3 = (ledger_id,
                  state_root_hash,
                  pool_state_root_hash,
                  txn_root_hash,
                  TimestampField._oldest_time - 1)

participants = ["Node1", "Node2", "Node3"]
signature = "somefakesignaturesomefakesignaturesomefakesignature"


def test_valid():
    assert not validator.validate((signature, participants, value))
    assert not validator.validate(('1' * 512, participants, value))


def test_invalid_participants():
    assert validator.validate((signature, "[]", value))
    assert validator.validate((signature, None, value))
    assert validator.validate((signature, [1], value))


def test_invalid_signature():
    assert validator.validate(("", participants, value))
    assert validator.validate((None, participants, value))
    assert validator.validate((123, participants, value))
    assert validator.validate(('1' * 513, participants, value))


def test_invalid_value():
    assert validator.validate((signature, participants, invalid_value1))
    assert validator.validate((signature, participants, invalid_value2))
    assert validator.validate((signature, participants, invalid_value3))
