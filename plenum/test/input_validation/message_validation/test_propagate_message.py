from collections import OrderedDict
from plenum.common.messages.fields import LimitedLengthStringField
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.messages.node_messages import Propagate

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("request", ClientMessageValidator),
    ("senderClient", LimitedLengthStringField),
])


def test_hash_expected_type():
    assert Propagate.typename == "PROPAGATE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(Propagate.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(Propagate.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
