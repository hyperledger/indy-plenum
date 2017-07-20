from typing import Mapping

from common.serializers.msgpack_serializer import MsgPackSerializer
from common.serializers.signing_serializer import SigningSerializer

signing_serializer = SigningSerializer()
ledger_txn_serializer = MsgPackSerializer()
ledger_hash_serializer = MsgPackSerializer()

def serialize_msg_for_signing(msg: Mapping, topLevelKeysToIgnore=None):
    """
    Serialize a message for signing.

    :param msg: the message to sign
    :param topLevelKeysToIgnore: the top level keys of the Mapping that should
    not be included in the serialized form
    :return: a uft-8 encoded version of `msg`
    """
    return signing_serializer.serialize(msg, topLevelKeysToIgnore=topLevelKeysToIgnore)