from _sha256 import sha256

from common.serializers.serialization import domain_state_serializer
from plenum.common.constants import STEWARD, ROLE, TRUSTEE

LAST_SEQ_NO = "lsn"
VALUE = "val"
LAST_UPDATE_TIME = "lut"


def is_steward(state, nym, is_committed: bool = False):
    role = get_role(state, nym, is_committed)
    return role == STEWARD


def is_trustee(state, nym, is_committed: bool = False):
    role = get_role(state, nym, is_committed)
    return role == TRUSTEE


def get_role(state, nym, is_committed: bool = False):
    nymData = get_nym_details(state, nym, is_committed)
    if not nymData:
        return {}
    else:
        return nymData.get(ROLE)


def get_nym_details(state, nym, is_committed: bool = False):
    key = nym_to_state_key(nym)
    data = state.get(key, is_committed)
    if not data:
        return {}
    return domain_state_serializer.deserialize(data)


def nym_to_state_key(nym: str) -> bytes:
    return sha256(nym.encode()).digest()


def encode_state_value(
    value, seqNo, txnTime, serializer=domain_state_serializer
):
    return serializer.serialize({
        LAST_SEQ_NO: seqNo,
        LAST_UPDATE_TIME: txnTime,
        VALUE: value
    })


def decode_state_value(encoded_value, serializer=domain_state_serializer):
    decoded = serializer.deserialize(encoded_value)
    value = decoded.get(VALUE)
    last_seq_no = decoded.get(LAST_SEQ_NO)
    last_update_time = decoded.get(LAST_UPDATE_TIME)
    return value, last_seq_no, last_update_time
