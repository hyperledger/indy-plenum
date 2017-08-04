from plenum.server.replica import Replica
from typing import List, Deque, Optional
from collections import deque
from plenum.server.monitor import Monitor

from stp_core.common.log import getlogger
logger = getlogger()


class Replicas:

    def __init__(self, onwer_name: str, monitor: Monitor):
        self._onwer_name = onwer_name
        self._monitor = monitor
        self._replicas = []  # type: List[Replica]
        self._messages_to_replicas = []  # type: List[Deque]

    def grow(self) -> int:
        instance_id = len(self._replicas)
        is_master = instance_id == 0
        description = "master" if is_master else "backup"
        replica = self._new_replica(instance_id, is_master)
        self._replicas.append(replica)
        self._messages_to_replicas.append(deque())
        self._monitor.addInstance()

        logger.display("{} added replica {} to instance {} ({})"
                       .format(self._onwer_name, replica, instance_id, description),
                       extra={"tags": ["node-replica"]})
        return instance_id

    def shrink(self) -> int:
        replica = self._replicas[-1]
        self._replicas = self._replicas[:-1]
        self._messages_to_replicas = self._messages_to_replicas[:-1]
        self._monitor.removeInstance()
        logger.display("{} removed replica {} from instance {}".
                       format(self._onwer_name, replica, replica.instId),
                       extra={"tags": ["node-replica"]})
        return len(self)

    @property
    def some_replica_has_primary(self) -> bool:
        return self.primary_replica_id is not None

    @property
    def primary_replica_id(self) -> Optional[int]:
        for instance_id, replica in enumerate(self._replicas):
            if replica.isPrimary:
                return instance_id

    def _new_replica(self, instance_id: int, is_master: bool) -> Replica:
        """
        Create a new replica with the specified parameters.
        """
        return Replica(self, instance_id, is_master)

    def __len__(self):
        return len(self._replicas)
