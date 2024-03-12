import string

from collections.abc import Iterable

from common.exceptions import LogicError
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
        self._keyValueStorage.put(payload_digest, self._create_value(ledger_id, seq_no))
        if digest is not None:
            self._keyValueStorage.put(self._full_digest_key(digest), payload_digest)

    def addBatch(self, batch, full_digest: bool = True):
        payload = []
        full = []
        for payload_digest, ledger_id, seq_no, digest in batch:
            payload.append((payload_digest, self._create_value(ledger_id,
                                                               seq_no)))
            if digest is not None:
                full.append((self._full_digest_key(digest), str(payload_digest)))
        self._keyValueStorage.setBatch(payload)
        if full_digest:
            self._keyValueStorage.setBatch(full)

    def get_by_payload_digest(self, payload_digest):
        try:
            val = self._keyValueStorage.get(payload_digest)
            val = val.decode()
            result = self._parse_value(val)
            if not isinstance(result, Iterable) or len(result) != 2:
                raise LogicError('SeqNoDB must store payload_digest => ledger_id and seq_no')
            return result
        except (KeyError, ValueError):
            return None, None

    def get_by_full_digest(self, full_digest):
        try:
            val = self._keyValueStorage.get(self._full_digest_key(full_digest))
            result = val.decode()
            if not isinstance(result, str):
                raise LogicError('SeqNoDB must store full_digest => payload_digest')
            return result
        except (KeyError, ValueError):
            return None

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

    @staticmethod
    def _full_digest_key(digest: str):
        return "full:{}".format(digest)
