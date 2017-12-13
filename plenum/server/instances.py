import time
from typing import Optional, Sequence


class Instances:
    def __init__(self):
        self.count = 0

        # Started time for each replica on the node. The value at index `i` in
        # the start time of the `i`th protocol instance
        self.started = []

    def add(self):
        """
        Add one protocol instance.
        """
        self.count += 1
        self.started.append(time.perf_counter())

    def remove(self, index):
        if 0 <= index < len(self.started):
            self.count -= 1
            del self.started[index]

    @property
    def ids(self) -> Sequence[int]:
        """
        Return the list of ids of all the protocol instances
        """
        return range(self.count)

    @property
    def masterId(self) -> Optional[int]:
        """
        Return the index of the replica that belongs to the master protocol
        instance
        """
        return 0 if self.count > 0 else None

    @property
    def backupIds(self) -> Sequence[int]:
        """
        Return the list of replicas that don't belong to the master protocol
        instance
        """
        return range(1, self.count)
