from collections import OrderedDict

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.messages.fields import IterableField, \
    LedgerIdField, NonNegativeNumberField, MerkleRootField, TimestampField
from plenum.common.messages.node_messages import BatchCommittedMsgData, BatchCommitted
from plenum.common.util import get_utc_epoch
from plenum.test.helper import sdk_random_request_objects, generate_state_root

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("requests", IterableField),
    ("ledgerId", LedgerIdField),
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ppSeqNo", NonNegativeNumberField),
    ("ppTime", TimestampField),
    ("stateRootHash", MerkleRootField),
    ("txnRootHash", MerkleRootField),
    ("seqNoStart", NonNegativeNumberField),
    ("seqNoEnd", NonNegativeNumberField),
    ("auditTxnRootHash", MerkleRootField),
    ("primaries", IterableField)
])


def create_valid_batch_committed():
    reqs = [req.as_dict for req in
            sdk_random_request_objects(10, identifier="1" * 16, protocol_version=CURRENT_PROTOCOL_VERSION)]
    return BatchCommitted(reqs,
                          DOMAIN_LEDGER_ID,
                          0,
                          0,
                          1,
                          get_utc_epoch(),
                          generate_state_root(),
                          generate_state_root(),
                          1,
                          2,
                          generate_state_root(),
                          ['Alpha', 'Beta'])


def create_invalid_batch_committed():
    return BatchCommitted(["aaaa", "bbbb"],
                          DOMAIN_LEDGER_ID,
                          0,
                          0,
                          1,
                          get_utc_epoch(),
                          generate_state_root(),
                          generate_state_root(),
                          1,
                          2,
                          generate_state_root(),
                          ['Alpha', 'Beta'])


def create_valid_batch_committed_as_dict():
    return dict(create_valid_batch_committed().__dict__)


def create_invalid_batch_committed_as_dict():
    return dict(create_invalid_batch_committed().__dict__)


def test_hash_expected_type():
    assert BatchCommittedMsgData.typename == "BATCH_COMMITTED"


def test_has_expected_fields():
    actual_field_names = OrderedDict(BatchCommittedMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(BatchCommittedMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)


def test_valid_batch_committed():
    assert create_valid_batch_committed()


def test_invalid_batch_committed():
    with pytest.raises(TypeError) as ex_info:
        create_invalid_batch_committed()
        ex_info.match("validation error [ClientMessageValidator]: invalid type <class 'str'>, dict expected")
