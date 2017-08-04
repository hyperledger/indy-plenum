from plenum.server.replica import Replica
from typing import List, Deque
from collections import deque
from stp_core.common.log import getlogger

logger = getlogger()


class Replicas:

    # TODO: enhance logging

    def __init__(self):
        self.replicas = []  # type: List[Replica]
        self.messages_to_replicas = []  # type: List[Deque]

    def grow(self) -> int:
        instance_id = len(self.replicas)
        is_master = instance_id == 0
        description = "master" if is_master else "backup"
        replica = self.createReplica(instance_id, is_master)
        self.replicas.append(replica)
        self.messages_to_replicas.append(deque())
        self.monitor.addInstance()

        logger.display("{} added replica {} to instance {} ({})"
                       .format(self, replica, instance_id, description),
                       extra={"tags": ["node-replica"]})
        return instance_id

    def shrink(self) -> int:
        replica = self.replicas[-1]
        self.replicas = self.replicas[:-1]
        self.messages_to_replicas = self.messages_to_replicas[:-1]
        self.monitor.removeInstance()
        logger.display("{} removed replica {} from instance {}".
                       format(self, replica, replica.instId),
                       extra={"tags": ["node-replica"]})
        return len(self)

    def __len__(self):
        return len(self.replicas)
