from plenum.common.messages.fields import TimestampField
from plenum.common.util import get_utc_epoch

validator = TimestampField()
timestamp = get_utc_epoch()


def test_valid_value():
    assert not validator.validate(timestamp)


def test_invalid_value():
    assert validator.validate(-1)
    assert validator.validate(validator._oldest_time - 1)
