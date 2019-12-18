from collections import OrderedDict

from plenum.common.constants import TXN_AUTHOR_AGREEMENT_DISABLE
from plenum.common.messages.client_request import ClientTxnAuthorAgreementDisableOperation
from plenum.common.messages.fields import ConstantField

TAA_DISABLE_EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("type", ConstantField),
])


def check_schema(actual, expected):
    assert list(actual.keys()) == list(expected.keys())
    for field, validator in expected.items():
        assert isinstance(actual[field], validator)


def test_taa_has_expected_schema():
    schema = OrderedDict(ClientTxnAuthorAgreementDisableOperation.schema)
    check_schema(schema, TAA_DISABLE_EXPECTED_ORDERED_FIELDS)
    assert schema["type"].value == TXN_AUTHOR_AGREEMENT_DISABLE
