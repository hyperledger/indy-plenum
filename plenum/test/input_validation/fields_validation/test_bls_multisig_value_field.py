from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.fields import BlsMultiSignatureValueField, TimestampField
from plenum.common.util import get_utc_epoch
from plenum.test.input_validation.utils import b58_by_len

validator = BlsMultiSignatureValueField()
state_root_hash = b58_by_len(32)
pool_state_root_hash = b58_by_len(32)
txn_root_hash = b58_by_len(32)
ledger_id = DOMAIN_LEDGER_ID
timestamp = get_utc_epoch()


def test_valid():
    assert not validator.validate((ledger_id,
                                   state_root_hash,
                                   pool_state_root_hash,
                                   txn_root_hash,
                                   timestamp))


def test_invalid_ledger_id():
    assert validator.validate((100,
                               state_root_hash,
                               pool_state_root_hash,
                               txn_root_hash,
                               timestamp))


def test_invalid_state_root_hash():
    assert validator.validate((ledger_id,
                               b58_by_len(31),
                               pool_state_root_hash,
                               txn_root_hash,
                               timestamp))


def test_invalid_pool_state_root_hash():
    assert validator.validate((ledger_id,
                               state_root_hash,
                               b58_by_len(31),
                               txn_root_hash,
                               timestamp))


def test_invalid_txn_root_hash():
    assert validator.validate((ledger_id,
                               state_root_hash,
                               pool_state_root_hash,
                               b58_by_len(31),
                               timestamp))


def test_invalid_timestamp():
    assert validator.validate((ledger_id,
                               state_root_hash,
                               pool_state_root_hash,
                               txn_root_hash,
                               TimestampField._oldest_time - 1))
