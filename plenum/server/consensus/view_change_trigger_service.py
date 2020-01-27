from typing import Callable

from plenum.common.config_util import getConfig
from plenum.common.constants import NODE_STATUS_DB_LABEL, VIEW_CHANGE_PREFIX
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import VoteForViewChange, NodeNeedViewChange, NewViewAccepted
from plenum.common.messages.node_messages import InstanceChange
from plenum.common.metrics_collector import MetricsCollector, NullMetricsCollector
from plenum.common.router import Subscription
from plenum.common.stashing_router import StashingRouter, DISCARD
from plenum.common.timer import TimerService
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.database_manager import DatabaseManager
from plenum.server.replica_validator_enums import STASH_CATCH_UP, CATCHING_UP
from plenum.server.suspicion_codes import Suspicions, Suspicion
from plenum.server.view_change.instance_change_provider import InstanceChangeProvider
from stp_core.common.log import getlogger

logger = getlogger()


class ViewChangeTriggerService:
    def __init__(self,
                 data: ConsensusSharedData,
                 timer: TimerService,
                 bus: InternalBus,
                 network: ExternalBus,
                 db_manager: DatabaseManager,
                 stasher: StashingRouter,
                 is_master_degraded: Callable[[], bool],
                 metrics: MetricsCollector = NullMetricsCollector()):
        self._data = data
        self._timer = timer
        self._bus = bus
        self._network = network
        self._stasher = stasher
        self._is_master_degraded = is_master_degraded
        self.metrics = metrics

        self._config = getConfig()

        self._instance_changes = \
            InstanceChangeProvider(outdated_ic_interval=self._config.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                   node_status_db=db_manager.get_store(NODE_STATUS_DB_LABEL),
                                   time_provider=timer.get_current_time)

        self._subscription = Subscription()
        self._subscription.subscribe(bus, VoteForViewChange, self.process_vote_for_view_change)
        self._subscription.subscribe(bus, NewViewAccepted, self.process_new_view_accepted)
        self._subscription.subscribe(stasher, InstanceChange, self.process_instance_change)

    def cleanup(self):
        self._subscription.unsubscribe_all()

    @property
    def name(self):
        return replica_name_to_node_name(self._data.name)

    def __repr__(self):
        return self.name

    def process_vote_for_view_change(self, msg: VoteForViewChange):
        proposed_view_no = self._data.view_no
        # TODO: Some time ago it was proposed that view_no should not be increased during proposal
        #  if view change is already in progress, unless suspicion code is "view change is taking too long".
        #  Idea was to improve stability of view change triggering, however for some reason this change lead
        #  to lots of failing/flaky tests. This still needs to be investigated.
        # if suspicion == Suspicions.INSTANCE_CHANGE_TIMEOUT or not self.view_change_in_progress:
        if msg.suspicion != Suspicions.STATE_SIGS_ARE_NOT_UPDATED or not self._data.waiting_for_new_view:
            proposed_view_no += 1
        if msg.view_no is not None:
            proposed_view_no = msg.view_no
        self._send_instance_change(proposed_view_no, msg.suspicion)

    def process_instance_change(self, msg: InstanceChange, frm: str):
        frm = replica_name_to_node_name(frm)

        # TODO: Do we really need this?
        if frm not in self._network.connecteds:
            return DISCARD, "instance change request: {} from {} which is not in connected list: {}".\
                format(msg, frm, self._network.connecteds)

        if not self._data.is_participating:
            return STASH_CATCH_UP, CATCHING_UP

        logger.info("{} received instance change request: {} from {}".format(self, msg, frm))

        if msg.viewNo <= self._data.view_no:
            return DISCARD, "instance change request with view no {} which is not more than its view no {}".\
                format(msg.viewNo, self._data.view_no)

        # Record instance changes for views but send instance change
        # only when found master to be degraded. if quorum of view changes
        #  found then change view even if master not degraded
        self._on_verified_instance_change_msg(msg, frm)

        if self._instance_changes.has_inst_chng_from(msg.viewNo, self.name):
            logger.info("{} received instance change message {} "
                        "but has already sent an instance change message".format(self, msg))
        elif not self._is_master_degraded():
            logger.info("{} received instance change message {} "
                        "but did not find the master to be slow".format(self, msg))
        else:
            logger.display("{}{} found master degraded after "
                           "receiving instance change message from {}".format(VIEW_CHANGE_PREFIX, self, frm))
            self._send_instance_change(msg.viewNo, Suspicions.PRIMARY_DEGRADED)

    def process_new_view_accepted(self, msg: NewViewAccepted):
        self._instance_changes.remove_view(self._data.view_no)

    def _send_instance_change(self, view_no: int, suspicion: Suspicion):
        logger.info("{}{} sending an instance change with view_no {} since {}".
                    format(VIEW_CHANGE_PREFIX, self, view_no, suspicion.reason))
        msg = InstanceChange(view_no, suspicion.code)
        self._network.send(msg)
        # record instance change vote for self and try to change the view if quorum is reached
        self._on_verified_instance_change_msg(msg, self.name)

    def _on_verified_instance_change_msg(self, msg: InstanceChange, frm: str):
        view_no = msg.viewNo

        if not self._instance_changes.has_inst_chng_from(view_no, frm):
            self._instance_changes.add_vote(msg, frm)
            if view_no > self._data.view_no:
                self._try_start_view_change_by_instance_change(view_no)

    def _try_start_view_change_by_instance_change(self, proposed_view_no: int) -> bool:
        # TODO: Need to handle skewed distributions which can arise due to
        #  malicious nodes sending messages early on
        can, why_not = self._can_view_change(proposed_view_no)
        if can:
            logger.display("{}{} initiating a view change to {} from {}".
                           format(VIEW_CHANGE_PREFIX, self, proposed_view_no, self._data.view_no))
            self._bus.send(NodeNeedViewChange(view_no=proposed_view_no))
        else:
            logger.info(why_not)
        return can

    def _can_view_change(self, proposed_view_no: int) -> (bool, str):
        quorum = self._data.quorums.view_change.value
        if not self._instance_changes.has_quorum(proposed_view_no, quorum):
            return False, '{} has no quorum for view {}'.format(self, proposed_view_no)
        if not proposed_view_no > self._data.view_no:
            return False, '{} is in higher view more than {}'.format(self, proposed_view_no)
        return True, ''
