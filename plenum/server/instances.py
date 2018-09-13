import time
from typing import Optional, Sequence


class Instances:
    def __init__(self):

        # Started time for each replica on the node. The value at index `i` in
        # the start time of the `i`th protocol instance
        self.started = dict()

    def add(self, inst_id):
        """
        Add one protocol instance.
        """
        self.started[inst_id] = time.perf_counter()

    def remove(self, inst_id):
        self.started.pop(inst_id, None)

    @property
    def ids(self) -> set:
        """
        Return the list of ids of all the protocol instances
        """
        return set(self.started.keys())

    @property
    def masterId(self) -> Optional[int]:
        """
        Return the index of the replica that belongs to the master protocol
        instance
        """
        return 0 if 0 in self.started.keys() else None

    @property
    def backupIds(self) -> Sequence[int]:
        """
        Return the list of replicas that don't belong to the master protocol
        instance
        """
        return [id for id in self.started.keys() if id != 0]

    @property
    def count(self) -> int:
        return len(self.started)
