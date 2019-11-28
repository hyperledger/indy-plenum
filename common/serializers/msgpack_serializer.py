from collections import OrderedDict
from typing import Dict, List

import msgpack
from common.serializers.mapping_serializer import MappingSerializer
from common.serializers.stream_serializer import StreamSerializer


def decode_to_sorted(obj):
    return OrderedDict(obj)


class MsgPackSerializer(MappingSerializer, StreamSerializer):
    """
    http://msgpack.org/ serializer

    The serializer preserves the order (in sorted order)
    '"""

    def serialize(self, data: Dict, fields=None, toBytes=True):
        """
        Serializes a dict to bytes preserving the order (in sorted order)
        :param data: the data to be serialized
        :return: serialized data as bytes
        """
        if isinstance(data, Dict):
            data = self._sort_dict(data)
        return msgpack.packb(data, use_bin_type=True)

    def deserialize(self, data, fields=None):
        """
        Deserializes msgpack bytes to OrderedDict (in the same sorted order as for serialize)
        :param data: the data in bytes
        :return: sorted OrderedDict
        """
        # TODO: it can be that we returned data by `get_lines`, that is already deserialized
        if not isinstance(data, (bytes, bytearray)):
            return data
        return msgpack.unpackb(data, encoding='utf-8', object_pairs_hook=decode_to_sorted)

    def get_lines(self, stream):
        return msgpack.Unpacker(stream, encoding='utf-8', object_pairs_hook=decode_to_sorted)

    def _sort_dict(self, d) -> OrderedDict:
        if not isinstance(d, Dict):
            return d
        d = OrderedDict(sorted(d.items()))
        for k, v in d.items():
            if isinstance(v, Dict):
                d[k] = self._sort_dict(v)
            if isinstance(v, List):
                d[k] = [self._sort_dict(sub_v) for sub_v in v]
        return d
