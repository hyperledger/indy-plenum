import string
from typing import Optional

from storage.kv_store import KeyValueStorage


class ReqIdrToTxn:
    """
    Stores a map from client identifier, request id tuple to transaction
    sequence number
    """

    def __init__(self, keyValueStorage: KeyValueStorage):
        self.delimiter = "~"
        self._keyValueStorage = keyValueStorage

    def add(self, digest, ledge_id, seq_no):
        self._keyValueStorage.put(digest, ledge_id + self.delimiter + seq_no)

    def addBatch(self, batch):
        self._keyValueStorage.setBatch([(digest, self._get_value(ledge_id,
                                                                 seq_no))
                                        for digest, ledge_id, seq_no in batch])

    def get(self, digest) -> Optional[(int, int)]:  # leger_id : int?
        """
        Return leger_id, seq_no of transaction that was a result
        of last request with this digest
        :param digest: digest of request
        :return: leger_id, seq_no
        """
        try:
            val = self._keyValueStorage.get(digest)
            return self._parse_value(val)
        except (KeyError, ValueError):
            return None

    def _parse_value(self, val: string):
        parse_data = val.split(self.delimiter)
        return int(parse_data[0]), int(parse_data[1])

    def _get_value(self, ledge_id, seq_no):
        return ledge_id + self.delimiter + seq_no

    @property
    def size(self):
        return self._keyValueStorage.size

    def close(self):
        self._keyValueStorage.close()
