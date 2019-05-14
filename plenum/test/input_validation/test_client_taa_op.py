from collections import OrderedDict

from plenum.common.constants import TXN_AUTHOR_AGREEMENT
from plenum.common.messages.fields import (
    LimitedLengthStringField, ConstantField)
from plenum.common.messages.client_request import ClientTxnAuthorAgreementOperation


EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("type", ConstantField),
    ("text", LimitedLengthStringField),
    ("version", LimitedLengthStringField)
])


def test_has_expected_fields():
    actual_field_names = OrderedDict(ClientTxnAuthorAgreementOperation.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ClientTxnAuthorAgreementOperation.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
    assert schema["type"].value == TXN_AUTHOR_AGREEMENT
