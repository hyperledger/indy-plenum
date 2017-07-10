from collections import OrderedDict
from typing import Dict

from ledger.serializers.mapping_serializer import MappingSerializer


class CompactSerializer(MappingSerializer):
    """
    Serializes a `Mapping` to string. Unlike JSON, does not use field(key)
    names. Instead store an ordered dictionary of fields to serialize and
    deserialize data. The ordered dictionary specifies in which order the values
     of the `Mapping` will appear in the string and also how to convert each type
      of value to and from string
    """
    def __init__(self, fields: OrderedDict=None):
        # TODO: add a special type (class) for fields

        self.fields = fields
        self.delimiter = "|"

    def _stringify(self, name, record, fields=None):
        fields = fields or self.fields
        if record is None or record == {}:
            return ""
        encoder = fields[name][0] or str
        return encoder(record)

    def _destringify(self, name, string, fields=None):
        if not string:
            return None
        fields = fields or self.fields
        decoder = fields[name][1] or str
        return decoder(string)

    def serialize(self, data: Dict, fields=None, toBytes=True):
        fields = fields or self.fields
        records = []

        def _addToRecords(name, record):
            records.append(self._stringify(name, record, fields))

        for name in fields:
            if "." in name:
                nameParts = name.split(".")
                record = data.get(nameParts[0], {})
                for part in nameParts[1:]:
                    record = record.get(part, {})
            else:
                record = data.get(name)
            _addToRecords(name, record)

        encoded = self.delimiter.join(records)
        if toBytes:
            encoded = encoded.encode()
        return encoded

    def deserialize(self, data, fields=None):
        fields = fields or self.fields
        if isinstance(data, (bytes, bytearray)):
            data = data.decode()
        items = data.split(self.delimiter)
        result = {}
        for name in fields:
            if "." in name:
                nameParts = name.split(".")
                ref = result
                for part in nameParts[:-1]:
                    if part not in ref:
                        ref[part] = {}
                    ref = ref[part]
                ref[nameParts[-1]] = self._destringify(name, items.pop(0), fields)
            elif items:
                result[name] = self._destringify(name, items.pop(0), fields)
            else:
                # if we have more fields than data available, assume that all missed fields are None
                result[name] = None
        return result
