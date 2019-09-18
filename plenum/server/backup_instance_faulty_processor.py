from typing import List

from plenum.common.messages.internal_messages import RequestPropagates
from plenum.common.messages.node_messages import BackupInstanceFaulty
from plenum.common.types import f
from plenum.server.suspicion_codes import Suspicions, Suspicion
from stp_core.common.log import getlogger

logger = getlogger()


class BackupInstanceFaultyProcessor:

    def __init__(self, node):
        self.node = node
        self.backup_instances_faulty = {}

    def restore_replicas(self) -> None:
        '''
        Restore removed replicas to requiredNumberOfInstances
        :return:
        '''
        self.backup_instances_faulty.clear()
        for inst_id in range(self.node.requiredNumberOfInstances):
            if inst_id not in self.node.replicas.keys():
                self.node.replicas.add_replica(inst_id)
                self.node.replicas.subscribe_to_internal_bus(RequestPropagates, self.node.request_propagates, inst_id)

    def on_backup_degradation(self, degraded_backups) -> None:
        '''
        The method for sending BackupInstanceFaulty messages
        if backups instances performance were degraded
        :param degraded_backups: list of backup instances
        ids which performance were degraded
        :return:
        '''
        self.__remove_replicas(degraded_backups,
                               Suspicions.BACKUP_PRIMARY_DEGRADED,
                               self.node.config.REPLICAS_REMOVING_WITH_DEGRADATION)

    def on_backup_primary_disconnected(self, degraded_backups) -> None:
        '''
        The method for sending BackupInstanceFaulty messages
        if backup primary disconnected
        :param degraded_backups: list of backup instances
        ids which performance were degraded
        :return:
        '''
        self.__remove_replicas(degraded_backups,
                               Suspicions.BACKUP_PRIMARY_DISCONNECTED,
                               self.node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED)

    def process_backup_instance_faulty_msg(self,
                                           backup_faulty: BackupInstanceFaulty,
                                           frm: str) -> None:
        '''
        The method for processing BackupInstanceFaulty from nodes
        and removing replicas with performance were degraded
        :param backup_faulty: BackupInstanceFaulty message with instances for removing
        :param frm:
        :return:
        '''
        logger.debug("{} receive BackupInstanceFaulty "
                     "from {}: {}".format(self.node, frm, backup_faulty))
        instances = getattr(backup_faulty, f.INSTANCES.nm)
        if getattr(backup_faulty, f.VIEW_NO.nm) != self.node.viewNo or \
                self.node.master_replica.instId in instances:
            return

        # Don't process BackupInstanceFaulty if strategy for this reason is not need quorum
        reason = Suspicions.get_by_code(getattr(backup_faulty, f.REASON.nm))
        if (
                reason == Suspicions.BACKUP_PRIMARY_DISCONNECTED and
                not self.is_quorum_strategy(self.node.config.REPLICAS_REMOVING_WITH_PRIMARY_DISCONNECTED)
        ) or (
                reason == Suspicions.BACKUP_PRIMARY_DEGRADED and
                not self.is_quorum_strategy(self.node.config.REPLICAS_REMOVING_WITH_DEGRADATION)
        ):
            return

        for inst_id in getattr(backup_faulty, f.INSTANCES.nm):
            if inst_id not in self.node.replicas.keys():
                continue
            self.backup_instances_faulty.setdefault(inst_id, dict()).setdefault(frm, 0)
            self.backup_instances_faulty[inst_id][frm] += 1
            all_nodes_condition = self.node.quorums.backup_instance_faulty.is_reached(
                len(self.backup_instances_faulty[inst_id].keys()))
            this_node_condition = (self.node.name in self.backup_instances_faulty[inst_id] and
                                   self.node.quorums.backup_instance_faulty.is_reached(
                                       self.backup_instances_faulty[inst_id][self.node.name]))
            if all_nodes_condition or this_node_condition:
                self.node.replicas.remove_replica(inst_id)
                self.backup_instances_faulty.pop(inst_id)

    def __send_backup_instance_faulty(self, instances: List[int],
                                      reason: Suspicion):
        if not self.node.view_change_in_progress and not instances:
            return
        logger.info("{} sending a backup instance faulty message with view_no {} "
                    "and reason '{}' for instances: {}".format(self.node.name,
                                                               self.node.viewNo,
                                                               reason.reason,
                                                               instances))
        msg = BackupInstanceFaulty(self.node.viewNo,
                                   instances,
                                   reason.code)
        self.process_backup_instance_faulty_msg(msg, self.node.name)
        self.node.send(msg)

    def __remove_replicas(self, degraded_backups, reason: Suspicion, removing_strategy):

        if self.is_quorum_strategy(removing_strategy):
            self.__send_backup_instance_faulty(degraded_backups,
                                               reason)
        elif self.is_local_remove_strategy(removing_strategy):
            for inst_id in degraded_backups:
                self.node.replicas.remove_replica(inst_id)

    def is_quorum_strategy(self, removing_strategy):
        return removing_strategy == "quorum"

    def is_local_remove_strategy(self, removing_strategy):
        return removing_strategy == "local"
