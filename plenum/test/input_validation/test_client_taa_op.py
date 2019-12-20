from collections import OrderedDict

from plenum.common.constants import TXN_AUTHOR_AGREEMENT, GET_TXN_AUTHOR_AGREEMENT
from plenum.common.messages.fields import (
    LimitedLengthStringField, ConstantField, NonEmptyStringField, NonNegativeNumberField, BooleanField)
from plenum.common.messages.client_request import ClientTxnAuthorAgreementOperation, \
    ClientGetTxnAuthorAgreementOperation

TAA_EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("type", ConstantField),
    ("text", LimitedLengthStringField),
    ("version", LimitedLengthStringField),
    ("ratification_ts", NonNegativeNumberField),
    ("retirement_ts", NonNegativeNumberField),
])

GET_TAA_EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("type", ConstantField),
    ("version", NonEmptyStringField),
    ("digest", NonEmptyStringField),
    ("timestamp", NonNegativeNumberField)
])


def check_schema(actual, expected):
    assert list(actual.keys()) == list(expected.keys())
    for field, validator in expected.items():
        assert isinstance(actual[field], validator)


def test_taa_has_expected_schema():
    schema = OrderedDict(ClientTxnAuthorAgreementOperation.schema)
    check_schema(schema, TAA_EXPECTED_ORDERED_FIELDS)
    assert schema["type"].value == TXN_AUTHOR_AGREEMENT


def test_get_taa_has_expected_schema():
    schema = OrderedDict(ClientGetTxnAuthorAgreementOperation.schema)
    check_schema(schema, GET_TAA_EXPECTED_ORDERED_FIELDS)
    assert schema["type"].value == GET_TXN_AUTHOR_AGREEMENT
