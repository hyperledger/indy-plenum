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

    def add(self, payload_digest, ledger_id, seq_no, digest):
        self._keyValueStorage.put(payload_digest, self._create_value(ledger_id, seq_no, digest))

    def addBatch(self, batch):
        self._keyValueStorage.setBatch([(payload_digest, self._create_value(ledger_id,
                                                                            seq_no,
                                                                            digest))
                                        for payload_digest, ledger_id, seq_no, digest in batch])

    def get(self, payload_digest):
        """
        Return leger_id, seq_no of transaction that was a result
        of last request with this digest
        :param digest: digest of request
        :return: leger_id, seq_no
        """
        try:
            val = self._keyValueStorage.get(payload_digest)
            return self._parse_value(val.decode())
        except (KeyError, ValueError):
            return None, None, None

    def _parse_value(self, val: string):
        parse_data = val.split(self.delimiter)
        return int(parse_data[0]), int(parse_data[1]), str(parse_data[2])

    def _create_value(self, ledger_id, seq_no, payload_digest):
        return str(ledger_id) + self.delimiter + str(seq_no) + self.delimiter + str(payload_digest)

    @property
    def size(self):
        return self._keyValueStorage.size

    def close(self):
        self._keyValueStorage.close()
