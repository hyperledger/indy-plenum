from plenum.common.messages.fields import ProtocolVersionField
from plenum.common.plenum_protocol_version import PlenumProtocolVersion

validator = ProtocolVersionField()


def test_valid():
    assert not validator.validate(1)
    assert not validator.validate(PlenumProtocolVersion.STATE_PROOF_SUPPORT.value)


def test_invalid():
    assert validator.validate(2)
    assert validator.validate("1")
    assert validator.validate(0)
    assert validator.validate(None)
    assert validator.validate(1.0)
    assert validator.validate(0.1)
