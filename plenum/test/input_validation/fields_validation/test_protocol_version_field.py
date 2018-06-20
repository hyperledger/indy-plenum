from plenum.common.messages.fields import ProtocolVersionField
from plenum.common.plenum_protocol_version import PlenumProtocolVersion

validator = ProtocolVersionField()


def test_valid():
    assert not validator.validate(2)
    assert not validator.validate(PlenumProtocolVersion.TXN_FORMAT_1_0_SUPPORT.value)


def test_invalid():
    assert validator.validate(None)
    assert validator.validate(PlenumProtocolVersion.STATE_PROOF_SUPPORT.value)
    assert validator.validate(1)
    assert validator.validate(3)
    assert validator.validate("1")
    assert validator.validate("")
    assert validator.validate(0)
    assert validator.validate(1.0)
    assert validator.validate(0.1)
