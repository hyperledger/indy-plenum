import pytest

from collections import OrderedDict
from plenum.common.messages.fields import IterableField, \
    SignatureField
from plenum.common.messages.node_messages import Batch

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("messages", IterableField),
    ("signature", SignatureField)

])


def test_hash_expected_type():
    assert Batch.typename == "BATCH"


def test_has_expected_fields():
    actual_field_names = OrderedDict(Batch.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(Batch.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
