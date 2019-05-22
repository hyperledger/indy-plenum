from collections import OrderedDict

from plenum.common.constants import GET_TXN_AUTHOR_AGREEMENT_AML, \
    TXN_AUTHOR_AGREEMENT_AML
from plenum.common.messages.fields import (
    LimitedLengthStringField, ConstantField, NonEmptyStringField, NonNegativeNumberField, AnyMapField)
from plenum.common.messages.client_request import ClientTxnAuthorAgreementOperationAML, \
    ClientGetTxnAuthorAgreementAMLOperation

TAA_AML_EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("type", ConstantField),
    ("version", LimitedLengthStringField),
    ("aml", AnyMapField),
    ("amlContext", LimitedLengthStringField)
])

GET_TAA_AML_EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("type", ConstantField),
    ("version", NonEmptyStringField),
    ("timestamp", NonNegativeNumberField)
])


def check_schema(actual, expected):
    assert list(actual.keys()) == list(expected.keys())
    for field, validator in expected.items():
        assert isinstance(actual[field], validator)


def test_taa_has_expected_schema():
    schema = OrderedDict(ClientTxnAuthorAgreementOperationAML.schema)
    check_schema(schema, TAA_AML_EXPECTED_ORDERED_FIELDS)
    assert schema["type"].value == TXN_AUTHOR_AGREEMENT_AML


def test_get_taa_has_expected_schema():
    schema = OrderedDict(ClientGetTxnAuthorAgreementAMLOperation.schema)
    check_schema(schema, GET_TAA_AML_EXPECTED_ORDERED_FIELDS)
    assert schema["type"].value == GET_TXN_AUTHOR_AGREEMENT_AML
