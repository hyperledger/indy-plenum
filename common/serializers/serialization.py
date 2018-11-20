from typing import Mapping

from common.serializers.base58_serializer import Base58Serializer
from common.serializers.base64_serializer import Base64Serializer
from common.serializers.json_serializer import JsonSerializer
from common.serializers.msgpack_serializer import MsgPackSerializer
from common.serializers.signing_serializer import SigningSerializer

signing_serializer = SigningSerializer()
ledger_txn_serializer = MsgPackSerializer()
ledger_hash_serializer = MsgPackSerializer()
domain_state_serializer = JsonSerializer()
pool_state_serializer = JsonSerializer()
node_status_db_serializer = JsonSerializer()
client_req_rep_store_serializer = JsonSerializer()
multi_sig_store_serializer = JsonSerializer()
state_roots_serializer = Base58Serializer()
txn_root_serializer = Base58Serializer()
proof_nodes_serializer = Base64Serializer()
multi_signature_value_serializer = MsgPackSerializer()
invalid_index_serializer = JsonSerializer()


# TODO: separate data, metadata and signature, so that we don't need to have topLevelKeysToIgnore
def serialize_msg_for_signing(msg: Mapping, topLevelKeysToIgnore=None):
    """
    Serialize a message for signing.

    :param msg: the message to sign
    :param topLevelKeysToIgnore: the top level keys of the Mapping that should
    not be included in the serialized form
    :return: a uft-8 encoded version of `msg`
    """
    return signing_serializer.serialize(msg, topLevelKeysToIgnore=topLevelKeysToIgnore)
