from common.serializers.serialization import multi_sig_store_serializer
from plenum.persistence.storage import initKeyValueStorage
from crypto.bls.bls_multi_signature import MultiSignature
from typing import Optional


class BlsStore:

    def __init__(self,
                 key_value_type,
                 data_location,
                 key_value_storage_name,
                 serializer=None):
        self._kvs = initKeyValueStorage(key_value_type,
                                        data_location,
                                        key_value_storage_name)
        self._serializer = serializer or multi_sig_store_serializer

    def put(self, multi_sig: MultiSignature):
        assert multi_sig is not None
        state_root_hash = multi_sig.value.state_root_hash
        serialized_multi_sign = self._serializer.serialize(multi_sig.as_dict())
        self._kvs.put(state_root_hash, serialized_multi_sign)

    def get(self, state_root_hash: str) -> Optional[MultiSignature]:
        try:
            ser_data = self._kvs.get(state_root_hash)
        except KeyError:
            return None
        data = self._serializer.deserialize(ser_data)
        multi_sig = MultiSignature.from_dict(**data)
        return multi_sig

    def close(self):
        self._kvs.close()
