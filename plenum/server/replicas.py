from collections import deque
from typing import Generator

from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_key_manager import LoadBLSKeyError
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.constants import BLS_PREFIX
from plenum.server.monitor import Monitor
from plenum.server.replica import Replica
from stp_core.common.log import getlogger

logger = getlogger()

MASTER_REPLICA_INDEX = 0


class Replicas:
    def __init__(self, node, monitor: Monitor, config=None):
        # passing full node because Replica requires it
        self._node = node
        self._monitor = monitor
        self._config = config
        self._replicas = []  # type: List[Replica]
        self._messages_to_replicas = []  # type: List[deque]

    def grow(self) -> int:
        instance_id = self.num_replicas
        is_master = instance_id == 0
        description = "master" if is_master else "backup"
        bls_bft = self._create_bls_bft_replica(is_master)
        replica = self._new_replica(instance_id, is_master, bls_bft)
        self._replicas.append(replica)
        self._messages_to_replicas.append(deque())
        self._monitor.addInstance()

        logger.display("{} added replica {} to instance {} ({})"
                       .format(self._node.name,
                               replica,
                               instance_id,
                               description),
                       extra={"tags": ["node-replica"]})
        return self.num_replicas

    def shrink(self) -> int:
        replica = self._replicas[-1]
        self._replicas = self._replicas[:-1]
        self._messages_to_replicas = self._messages_to_replicas[:-1]
        self._monitor.removeInstance()
        logger.display("{} removed replica {} from instance {}".
                       format(self._node.name, replica, replica.instId),
                       extra={"tags": ["node-replica"]})
        return self.num_replicas

    @property
    def some_replica_has_primary(self) -> bool:
        for replica in self._replicas:
            if replica.isPrimary:
                return replica.instId

    @property
    def master_replica_is_primary(self):
        if self.num_replicas > 0:
            return self._master_replica.isPrimary

    @property
    def _master_replica(self):
        return self._replicas[MASTER_REPLICA_INDEX]

    def service_inboxes(self, limit: int = None):
        number_of_processed_messages = \
            sum(replica.serviceQueues(limit) for replica in self._replicas)
        return number_of_processed_messages

    def pass_message(self, message, instance_id=None):
        replicas = self._replicas
        if instance_id is not None:
            replicas = replicas[instance_id:instance_id + 1]
        for replica in replicas:
            replica.inBox.append(message)

    def get_output(self, limit: int = None) -> Generator:
        if limit is None:
            per_replica = None
        else:
            per_replica = round(limit / self.num_replicas)
            if per_replica == 0:
                logger.debug("{} forcibly setting replica "
                             "message limit to {}"
                             .format(self._node.name,
                                     per_replica))
                per_replica = 1
        for replica in self._replicas:
            num = 0
            while replica.outBox:
                yield replica.outBox.popleft()
                num += 1
                if per_replica and num >= per_replica:
                    break

    def take_ordereds_out_of_turn(self) -> tuple:
        """
        Takes all Ordered messages from outbox out of turn
        """
        for replica in self._replicas:
            yield replica.instId, replica._remove_ordered_from_queue()

    def _new_replica(self, instance_id: int, is_master: bool, bls_bft: BlsBft) -> Replica:
        """
        Create a new replica with the specified parameters.
        """
        return Replica(self._node, instance_id, self._config, is_master, bls_bft)

    def _create_bls_bft_replica(self, is_master):
        bls_factory = create_default_bls_bft_factory(self._node)
        bls_bft_replica = bls_factory.create_bls_bft_replica(is_master)
        return bls_bft_replica

    @property
    def num_replicas(self):
        return len(self._replicas)

    @property
    def sum_inbox_len(self):
        return sum(len(replica.inBox) for replica in self._replicas)

    @property
    def all_instances_have_primary(self) -> bool:
        return all(replica.primaryName is not None
                   for replica in self._replicas)

    def register_new_ledger(self, ledger_id):
        for replica in self._replicas:
            replica.register_ledger(ledger_id)

    def __getitem__(self, item):
        assert isinstance(item, int)
        return self._replicas[item]

    def __len__(self):
        return self.num_replicas

    def __iter__(self):
        return self._replicas.__iter__()
