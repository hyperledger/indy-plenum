from plenum.persistence.storage import initKeyValueStorage
from crypto.bls.bls_multi_signature import MultiSignature
from typing import Optional


class BlsStore:

    def __init__(self,
                 keyValueType,
                 dataLocation,
                 keyValueStorageName,
                 serializer):
        self._kvs = initKeyValueStorage(keyValueType,
                                        dataLocation,
                                        keyValueStorageName)
        self._serializer = serializer

    def put(self, root_hash: str, sign: MultiSignature):
        assert root_hash is not None
        assert sign is not None
        serialized_multi_sign = self._serializer.serialize(sign.as_dict())
        self._kvs.put(root_hash, serialized_multi_sign)

    def get(self, root_hash: str) -> Optional[MultiSignature]:
        try:
            ser_data = self._kvs.get(root_hash)
        except KeyError:
            return None
        data = self._serializer.deserialize(ser_data)
        multi_sig = MultiSignature(**data)
        return multi_sig

    def close(self):
        self._kvs.close()
