from collections import OrderedDict
from typing import Tuple, List, Iterable

from storage.kv_store import KeyValueStorage


class OptimisticKVStore:
    """
    A key value storage where entries can be stored optimistically; meaning
    that data can be written to it but it will not be persisted on to disk
    until its committed. Entries are written in batches so the usual flow would
    be: do 1 or more writes, mark the batch as created and commit/revert batch.
    Each batch can be assigned an identifier on creation
    """
    def __init__(self, kv_store: KeyValueStorage):
        self._store = kv_store
        # List of Tuples where first items is the state root after batch and
        # second item is a dictionary similar to cache which can be queried
        # like the database, i.e `self._db`. Keys (state roots are purged)
        # when they get committed or reverted.
        self.un_committed = []  # type: List[Tuple[bytes, OrderedDict]]

        # Relevant NYMs operation done in current batch, in order
        self.current_batch_ops = []  # type: List[Tuple]

    def create_batch_from_current(self, batch_idr):
        self.un_committed.append((batch_idr, OrderedDict(self.current_batch_ops)))
        self.current_batch_ops = []

    def reject_batch(self):
        # Batches are always rejected from end of `self.unCommitted`
        self.current_batch_ops = []
        self.un_committed = self.un_committed[:-1]

    def commit_batch(self):
        # Commit an already created batch
        if self.un_committed:
            batch_idr = self.first_batch_idr
            self._store.setBatch([(key, val) for key, val in
                                  self.un_committed[0][1].items()])
            self.un_committed = self.un_committed[1:]
            return batch_idr
        else:
            raise ValueError

    def get(self, key, is_committed=False):
        if is_committed:
            return self._store.get(key)
        # Looking for uncommitted values, iterating over `current_batch_ops`
        # and `un_committed` in reverse to get the latest value
        for k, value in reversed(self.current_batch_ops):
            if k == key:
                return value
        for _, cache in reversed(self.un_committed):
            if key in cache:
                return cache[key]
        return self._store.get(key)

    def set(self, key, value, is_committed=False):
        if is_committed:
            self._store.put(key, value)
        else:
            self.current_batch_ops.append((key, value))

    def remove(self, key, is_committed=False):
        if isinstance(key, str):
            key = key.encode()
        if is_committed:
            self._store.remove(key)
        else:
            self.current_batch_ops = [(k, v) for k, v in
                                      self.current_batch_ops if k == key]

    @property
    def first_batch_idr(self):
        if self.un_committed:
            return self.un_committed[0][0]
        else:
            return None

    def setBatch(self, batch: Iterable[Tuple], is_committed=False):
        if is_committed:
            self._store.setBatch(batch)
        else:
            for k, v in batch:
                self.set(k, v, is_committed=False)

    def do_ops_in_batch(self, batch: Iterable[Tuple], is_committed=False):
        if is_committed:
            self._store.do_ops_in_batch(batch)
        else:
            for op, key, value in batch:
                if op == KeyValueStorage.WRITE_OP:
                    self.set(key, value, is_committed=False)
                elif op == KeyValueStorage.REMOVE_OP:
                    self.remove(key, is_committed=False)
                else:
                    raise ValueError('Unknown operation')
