import logging
from typing import Optional, List, Tuple, Set

from base58 import b58encode

from plenum.common.config_util import getConfig
from plenum.common.startable import Mode
from plenum.server.quorums import Quorums
from plenum.server.view_change.view_changer import ViewChangerDataProvider, ViewChanger
from stp_core.common.log import getlogger

config = getConfig()
logger = getlogger()


class ViewChangerNodeModelDataProvider(ViewChangerDataProvider):
    def __init__(self, node):
        self._node = node
        self._has_primary = True

    def name(self) -> str:
        return self._node.name

    def config(self) -> object:
        return config

    def quorums(self) -> Quorums:
        return self._node._quorum

    def has_pool_ledger(self) -> bool:
        return True

    def ledger_summary(self) -> List[Tuple[int, int, str]]:
        return [(0, 42, b58encode('A' * 32).decode()),
                (1, 73, b58encode('B' * 32).decode()),
                (2, 37, b58encode('C' * 32).decode())]

    def node_registry(self, size):
        raise NotImplemented()

    def is_node_synced(self) -> bool:
        return True

    def node_mode(self) -> Mode:
        return Mode.synced

    def next_primary_name(self) -> str:
        return self._node.next_primary_name

    def current_primary_name(self) -> str:
        return self._node.current_primary_name

    def has_primary(self) -> bool:
        return self._has_primary

    def is_primary(self) -> Optional[bool]:
        if self._has_primary:
            return self._node.is_primary

    def is_primary_disconnected(self) -> bool:
        return self._node.is_primary_disconnected

    def is_master_degraded(self) -> bool:
        return self._node._corrupted_name == self._node.primary_name

    def pretty_metrics(self) -> str:
        return ""

    def state_freshness(self) -> float:
        return 0

    def connected_nodes(self) -> Set[str]:
        return set(self._node.connected_nodes)

    def notify_view_change_start(self):
        self._has_primary = False

    def notify_view_change_complete(self):
        pass

    def notify_initial_propose_view_change(self):
        pass

    def start_catchup(self):
        self._node.schedule_finish_catchup()

    def restore_backup_replicas(self):
        pass

    def select_primaries(self, node_reg):
        self._has_primary = True

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        logMethod("Discarding {} because of {}".format(msg, reason))

    @property
    def node_status_db(self):
        return None


def create_view_changer(node):
    return ViewChanger(ViewChangerNodeModelDataProvider(node), node._timer)
