from collections import deque
from typing import Generator, Type, Callable

from common.exceptions import PlenumTypeError
from crypto.bls.bls_bft import BlsBft
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.util import SortedDict
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.monitor import Monitor
from plenum.server.replica import Replica
from stp_core.common.log import getlogger

logger = getlogger()

MASTER_REPLICA_INDEX = 0


class Replicas:
    _replica_class = Replica

    def __init__(self, node, monitor: Monitor, config=None, metrics: MetricsCollector = NullMetricsCollector()):
        # passing full node because Replica requires it
        self._node = node
        self._monitor = monitor
        self._metrics = metrics
        self._config = config
        self._replicas = SortedDict()  # type: SortedDict[int, Replica]
        self._messages_to_replicas = dict()  # type: Dict[deque]
        self.register_monitor_handler()

    def add_replica(self, instance_id) -> int:
        is_master = instance_id == 0
        description = "master" if is_master else "backup"
        bls_bft = self._create_bls_bft_replica(is_master)
        replica = self._new_replica(instance_id, is_master, bls_bft)
        replica.set_view_no(self._node.viewNo if self._node.viewNo is not None else 0)
        self._replicas[instance_id] = replica
        self._messages_to_replicas[instance_id] = deque()
        self._monitor.addInstance(instance_id)

        logger.display("{} added replica {} to instance {} ({})"
                       .format(self._node.name,
                               replica,
                               instance_id,
                               description),
                       extra={"tags": ["node-replica"]})

        logger.info('reset monitor due to replica addition')
        self._monitor.reset()

    def remove_replica(self, inst_id: int):
        if inst_id not in self._replicas:
            return
        replica = self._replicas.pop(inst_id)
        replica.cleanup()

        self._messages_to_replicas.pop(inst_id, None)
        self._monitor.removeInstance(inst_id)
        logger.display("{} removed replica {} from instance {}".
                       format(self._node.name, replica, replica.instId),
                       extra={"tags": ["node-replica"]})

    def send_to_internal_bus(self, msg, inst_id: int=None):
        if inst_id is None:
            for replica in self._replicas.values():
                replica.internal_bus.send(msg)
        else:
            if inst_id in self._replicas:
                self._replicas[inst_id].internal_bus.send(msg)
            else:
                logger.info("Cannot send msg ({}) to the replica {} "
                            "because it does not exist.".format(msg, inst_id))

    def subscribe_to_internal_bus(self, message_type: Type, handler: Callable, inst_id: int=None):
        if inst_id is None:
            for replica in self._replicas.values():
                replica.internal_bus.subscribe(message_type, handler)
        else:
            if inst_id in self._replicas:
                self._replicas[inst_id].internal_bus.subscribe(message_type, handler)
            else:
                logger.info("Cannot subscribe for {} for the replica {} "
                            "because it does not exist.".format(message_type, inst_id))

    # TODO unit test
    @property
    def some_replica_is_primary(self) -> bool:
        return any([r.isPrimary for r in self._replicas.values()])

    @property
    def master_replica_is_primary(self):
        if self.num_replicas > 0:
            return self._master_replica.isPrimary

    @property
    def _master_replica(self):
        return self._replicas[MASTER_REPLICA_INDEX]

    def service_inboxes(self, limit: int = None):
        number_of_processed_messages = \
            sum(replica.serviceQueues(limit) for replica in self._replicas.values())
        return number_of_processed_messages

    def pass_message(self, message, instance_id=None):
        if instance_id is not None:
            if instance_id not in self._replicas.keys():
                return
            self._replicas[instance_id].inBox.append(message)
        else:
            for replica in self._replicas.values():
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
        for replica in list(self._replicas.values()):
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
        for replica in self._replicas.values():
            yield replica.instId, replica._remove_ordered_from_queue()

    def _new_replica(self, instance_id: int, is_master: bool, bls_bft: BlsBft) -> Replica:
        """
        Create a new replica with the specified parameters.
        """
        return self._replica_class(self._node, instance_id, self._config, is_master, bls_bft, self._metrics)

    def _create_bls_bft_replica(self, is_master):
        bls_factory = create_default_bls_bft_factory(self._node)
        bls_bft_replica = bls_factory.create_bls_bft_replica(is_master)
        return bls_bft_replica

    @property
    def num_replicas(self):
        return len(self._replicas)

    @property
    def sum_inbox_len(self):
        return sum(len(replica.inBox) for replica in self._replicas.values())

    @property
    def all_instances_have_primary(self) -> bool:
        return all(replica.primaryName is not None
                   for replica in self._replicas.values())

    @property
    def primary_name_by_inst_id(self) -> dict:
        return {r.instId: replica_name_to_node_name(r.primaryName)
                for r in self._replicas.values()}

    @property
    def inst_id_by_primary_name(self) -> dict:
        return {replica_name_to_node_name(r.primaryName): r.instId
                for r in self._replicas.values() if r.primaryName}

    def register_new_ledger(self, ledger_id):
        for replica in self._replicas.values():
            replica.register_ledger(ledger_id)

    def register_monitor_handler(self):
        # attention: handlers will work over unordered request only once
        self._monitor.unordered_requests_handlers.append(
            self.unordered_request_handler_logging)

    def unordered_request_handler_logging(self, unordereds):
        replica = self._master_replica
        for unordered in unordereds:
            reqId, duration = unordered

            # get ppSeqNo and viewNo
            preprepares = replica._ordering_service.sent_preprepares if replica.isPrimary else replica._ordering_service.prePrepares
            ppSeqNo = None
            viewNo = None
            for key in preprepares:
                if any([pre_pre_req == reqId for pre_pre_req in preprepares[key].reqIdr]):
                    ppSeqNo = preprepares[key].ppSeqNo
                    viewNo = preprepares[key].viewNo
                    break
            if ppSeqNo is None or viewNo is None:
                logger.warning('Unordered request with reqId: {} was not found in prePrepares. '
                               'Prepares count: {}, Commits count: {}'.format(reqId,
                                                                              len(replica._ordering_service.prepares),
                                                                              len(replica._ordering_service.commits)))
                continue

            # get pre-prepare sender
            prepre_sender = replica.primaryNames.get(viewNo, 'UNKNOWN')

            # get prepares info
            prepares = replica._ordering_service.prepares[(viewNo, ppSeqNo)][0] \
                if (viewNo, ppSeqNo) in replica._ordering_service.prepares else []
            n_prepares = len(prepares)
            str_prepares = 'noone'
            if n_prepares:
                str_prepares = ', '.join(prepares)

            # get commits info
            commits = replica._ordering_service.commits[(viewNo, ppSeqNo)][0] \
                if (viewNo, ppSeqNo) in replica._ordering_service.commits else []
            n_commits = len(commits)
            str_commits = 'noone'
            if n_commits:
                str_commits = ', '.join(commits)

            # get txn content
            content = replica.requests[reqId].request.as_dict \
                if reqId in replica.requests else 'no content saved'

            logger.warning('Consensus for digest {} was not achieved within {} seconds. '
                           'Primary node is {}. '
                           'Received Pre-Prepare from {}. '
                           'Received {} valid Prepares from {}. '
                           'Received {} valid Commits from {}. '
                           'Transaction contents: {}. '
                           .format(reqId, duration,
                                   replica_name_to_node_name(replica.primaryName),
                                   prepre_sender,
                                   n_prepares, str_prepares, n_commits, str_commits, content))

    def keys(self):
        return self._replicas.keys()

    def values(self):
        return self._replicas.values()

    def items(self):
        return self._replicas.items()

    def __getitem__(self, item):
        if not isinstance(item, int):
            raise PlenumTypeError('item', item, int)
        return self._replicas[item]

    def __len__(self):
        return self.num_replicas

    def __iter__(self):
        return self._replicas.__iter__()
