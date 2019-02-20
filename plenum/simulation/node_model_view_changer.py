import logging
from typing import Optional, List, Tuple, Set

from plenum.common.config_util import getConfig
from plenum.common.startable import Mode
from plenum.server.quorums import Quorums
from plenum.server.view_change.view_changer import ViewChangerDataProvider, ViewChanger

config = getConfig()


class ViewChangerNodeModelDataProvider(ViewChangerDataProvider):
    def __init__(self, node):
        self._node = node

    def name(self) -> str:
        return str(self._node.id)

    def config(self) -> object:
        return config

    def quorums(self) -> Quorums:
        return self._node._quorum

    def has_pool_ledger(self) -> bool:
        return True

    def ledger_summary(self) -> List[Tuple[int, int, str]]:
        raise NotImplemented()

    def node_registry(self, size):
        raise NotImplemented()

    def is_node_synced(self) -> bool:
        return True

    def node_mode(self) -> Mode:
        return Mode.synced

    def next_primary_name(self) -> str:
        raise NotImplemented()

    def current_primary_name(self) -> str:
        raise NotImplemented()

    def has_primary(self) -> bool:
        return True

    def is_primary(self) -> Optional[bool]:
        raise NotImplemented()

    def is_primary_disconnected(self) -> bool:
        raise NotImplemented()

    def is_master_degraded(self) -> bool:
        raise NotImplemented()

    def pretty_metrics(self) -> str:
        return ""

    def state_freshness(self) -> float:
        return 0

    def connected_nodes(self) -> Set[str]:
        raise NotImplemented()

    def notify_view_change_start(self):
        pass

    def notify_view_change_complete(self):
        pass

    def notify_initial_propose_view_change(self):
        pass

    def start_catchup(self):
        pass

    def restore_backup_replicas(self):
        pass

    def select_primaries(self, node_reg):
        pass

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        pass

    @property
    def node_status_db(self):
        return None


def create_view_changer(node):
    return ViewChanger(ViewChangerNodeModelDataProvider(node), node._timer)
