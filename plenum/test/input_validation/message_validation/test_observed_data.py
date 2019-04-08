from collections import OrderedDict

import pytest

from plenum.common.constants import BATCH
from plenum.common.messages.fields import AnyValueField, ChooseField
from plenum.common.messages.node_messages import ObservedDataMsgData, ObservedData
from plenum.test.input_validation.message_validation.test_batch_committed import create_valid_batch_committed, \
    create_valid_batch_committed_as_dict, create_invalid_batch_committed, create_invalid_batch_committed_as_dict

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("msg_type", ChooseField),
    ("msg", AnyValueField)
])


def test_hash_expected_type():
    assert ObservedDataMsgData.typename == "OBSERVED_DATA"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ObservedDataMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ObservedDataMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)


def test_allowed_msg_type():
    assert ObservedData(BATCH, create_valid_batch_committed())

    with pytest.raises(TypeError) as ex_info:
        assert ObservedData("Unknown", create_valid_batch_committed())
        ex_info.match("expected one of 'BATCH'")


def test_allowed_msg():
    assert ObservedData(BATCH, create_valid_batch_committed())
    assert ObservedData(BATCH, create_valid_batch_committed_as_dict())

    with pytest.raises(TypeError) as ex_info:
        assert ObservedData(BATCH, [])
        ex_info.match("expected one of 'BATCH'")

    with pytest.raises(TypeError) as ex_info:
        assert ObservedData(BATCH, None)
        ex_info.match("expected one of 'BATCH'")

    with pytest.raises(TypeError) as ex_info:
        assert ObservedData(BATCH, create_invalid_batch_committed())
        ex_info.match("expected one of 'BATCH'")

    with pytest.raises(TypeError) as ex_info:
        assert ObservedData(BATCH, create_invalid_batch_committed_as_dict())
        ex_info.match("expected one of 'BATCH'")
