from collections import OrderedDict

from plenum.common.messages.fields import IterableField, NonNegativeNumberField
from plenum.common.messages.node_messages import OldViewPrePrepareRequest

EXPECTED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("batch_ids", IterableField),
])


def test_has_expected_type():
    assert OldViewPrePrepareRequest.typename == "OLD_VIEW_PREPREPARE_REQ"


def test_has_expected_fields():
    actual_field_names = OrderedDict(OldViewPrePrepareRequest.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(OldViewPrePrepareRequest.schema)
    for field, validator in EXPECTED_FIELDS.items():
        assert isinstance(schema[field], validator)
