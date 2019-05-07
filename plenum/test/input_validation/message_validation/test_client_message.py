from collections import OrderedDict

from plenum.common.messages.client_request import (
    ClientMessageValidator, ClientOperationField, ClientTAAAcceptance
)
from plenum.common.messages.fields import LimitedLengthStringField, \
    SignatureField, NonNegativeNumberField, \
    IdentifierField, ProtocolVersionField, MapField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("identifier", IdentifierField),
    ("reqId", NonNegativeNumberField),
    ("operation", ClientOperationField),
    ("signature", SignatureField),
    ("digest", LimitedLengthStringField),
    ("protocolVersion", ProtocolVersionField),
    ("taaAcceptance", ClientTAAAcceptance),
    ('signatures', MapField)
])


def test_has_expected_fields():
    actual_field_names = OrderedDict(ClientMessageValidator.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ClientMessageValidator.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
