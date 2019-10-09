from collections import OrderedDict

from plenum.common.messages.fields import IterableField, NonNegativeNumberField
from plenum.common.messages.node_messages import OldViewPrePrepareReply

EXPECTED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("preprepares", IterableField),
])


def test_has_expected_type():
    assert OldViewPrePrepareReply.typename == "OLD_VIEW_PREPREPARE_REP"


def test_has_expected_fields():
    actual_field_names = OrderedDict(OldViewPrePrepareReply.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(OldViewPrePrepareReply.schema)
    for field, validator in EXPECTED_FIELDS.items():
        assert isinstance(schema[field], validator)
