from collections import OrderedDict

from plenum.common.messages.fields import (
    LimitedLengthStringField, Sha256HexField, TimestampField
)
from plenum.common.messages.client_request import ClientTAAAcceptance


EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("taaDigest", Sha256HexField),
    ("mechanism", LimitedLengthStringField),
    ("time", TimestampField),
])


def test_has_expected_fields():
    actual_field_names = OrderedDict(ClientTAAAcceptance.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ClientTAAAcceptance.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
