from plenum.persistence.storage import KeyValueStorage, initKeyValueStorage
from common.serializers.compact_serializer import CompactSerializer
from collections import OrderedDict


class BlsStore:
    def __init__(self, keyValueType, dataLocation, keyValueStorageName):
        self._kvs = initKeyValueStorage(keyValueType, dataLocation, keyValueStorageName)
        self._serializer = CompactSerializer(OrderedDict([("s", (str, str)),("p", (str, str))]))

    def put(self, root_hash: str, sign: str, participants: list):
        assert root_hash is not None
        assert sign is not None
        assert participants is not None and participants

        data = {"s": sign, "p": '\t'.join(participants)}
        ser_data = self._serializer.serialize(data)
        self._kvs.put(root_hash, ser_data)

    def get(self, root_hash: str):
        try:
            ser_data = self._kvs.get(root_hash)
        except KeyError:
            return None, None

        data = self._serializer.deserialize(ser_data)
        return data.get("s", None), list(data.get("p", ).split('\t'))

    def close(self):
        self._kvs.close()
