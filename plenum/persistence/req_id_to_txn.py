import string

from storage.kv_store import KeyValueStorage


class ReqIdrToTxn:
    """
    Stores a map from client identifier, request id tuple to transaction
    sequence number
    """
    delimiter = "~"

    def __init__(self, keyValueStorage: KeyValueStorage):
        self._keyValueStorage = keyValueStorage

    def add(self, digest, ledger_id, seq_no):
        self._keyValueStorage.put(digest, self._create_value(ledger_id, seq_no))

    def addBatch(self, batch):
        self._keyValueStorage.setBatch([(digest, self._create_value(ledger_id,
                                                                    seq_no))
                                        for digest, ledger_id, seq_no in batch])

    def get(self, digest):
        """
        Return leger_id, seq_no of transaction that was a result
        of last request with this digest
        :param digest: digest of request
        :return: leger_id, seq_no
        """
        try:
            val = self._keyValueStorage.get(digest)
            return self._parse_value(val.decode())
        except (KeyError, ValueError):
            return None, None

    def _parse_value(self, val: string):
        parse_data = val.split(self.delimiter)
        return int(parse_data[0]), int(parse_data[1])

    def _create_value(self, ledger_id, seq_no):
        return str(ledger_id) + self.delimiter + str(seq_no)

    @property
    def size(self):
        return self._keyValueStorage.size

    def close(self):
        self._keyValueStorage.close()
