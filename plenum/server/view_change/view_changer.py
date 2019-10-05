import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional, Set
from functools import partial

from plenum.common.startable import Mode
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.quorums import Quorums
from plenum.server.view_change.instance_change_provider import InstanceChangeProvider
from storage.kv_store import KeyValueStorage
from stp_core.common.log import getlogger

from plenum.common.constants import VIEW_CHANGE_PREFIX, MONITORING_PREFIX
from plenum.common.messages.node_messages import InstanceChange
from plenum.server.suspicion_codes import Suspicions
from plenum.server.router import Router

logger = getlogger()


# TODO docs and types
# TODO logging


class ViewChangerDataProvider(ABC):
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def config(self) -> object:
        pass

    @abstractmethod
    def quorums(self) -> Quorums:
        pass

    @abstractmethod
    def node_mode(self) -> Mode:
        pass

    @abstractmethod
    def has_primary(self) -> bool:
        pass

    @abstractmethod
    def is_primary(self) -> Optional[bool]:
        pass

    @abstractmethod
    def is_primary_disconnected(self) -> bool:
        pass

    @abstractmethod
    def is_master_degraded(self) -> bool:
        pass

    @abstractmethod
    def pretty_metrics(self) -> str:
        pass

    @abstractmethod
    def state_freshness(self) -> float:
        pass

    @abstractmethod
    def connected_nodes(self) -> Set[str]:
        pass

    @abstractmethod
    def notify_view_change_start(self):
        pass

    @abstractmethod
    def notify_view_change_complete(self):
        pass

    @abstractmethod
    def select_primaries(self):
        pass

    @abstractmethod
    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        pass

    @property
    @abstractmethod
    def node_status_db(self) -> Optional[KeyValueStorage]:
        pass

    @abstractmethod
    def schedule_resend_inst_chng(self):
        pass

    @abstractmethod
    def start_view_change(self, proposed_view_no: int):
        pass

    @abstractmethod
    def view_no(self):
        pass

    @abstractmethod
    def view_change_in_progress(self):
        pass


class ViewChanger():

    def __init__(self, provider: ViewChangerDataProvider, timer: TimerService):
        self.provider = provider
        self._timer = timer

        self.inBox = deque()
        self.outBox = deque()
        self.inBoxRouter = Router(
            (InstanceChange, self.process_instance_change_msg)
        )

        self.instance_changes = InstanceChangeProvider(self.config.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                                       node_status_db=self.provider.node_status_db)

        self.previous_view_no = None

        # Action for _schedule instanceChange messages
        self.instance_change_action = None

        # Count of instance change rounds
        self.instance_change_rounds = 0

        # Time for view_change_starting
        self.start_view_change_ts = 0

        # Last successful viewNo.
        # In some cases view_change process can be uncompleted in time.
        # In that case we want to know, which viewNo was successful (last completed view_change)
        self.last_completed_view_no = 0

        # Force periodic view change if enabled in config
        force_view_change_freq = self.config.ForceViewChangeFreq
        if force_view_change_freq > 0:
            RepeatingTimer(self._timer, force_view_change_freq, self.on_master_degradation)

        # Start periodic freshness check
        state_freshness_update_interval = self.config.STATE_FRESHNESS_UPDATE_INTERVAL
        if state_freshness_update_interval > 0:
            RepeatingTimer(self._timer, state_freshness_update_interval, self.check_freshness)

    def __repr__(self):
        return "{}".format(self.name)

    # PROPERTIES

    @property
    def view_no(self):
        return self.provider.view_no()

    @property
    def name(self) -> str:
        return self.provider.name()

    @property
    def config(self) -> object:
        return self.provider.config()

    @property
    def quorums(self) -> Quorums:
        return self.provider.quorums()

    @property
    def view_change_in_progress(self) -> bool:
        return self.provider.view_change_in_progress()

    @property
    def quorum(self) -> int:
        return self.quorums.view_change_done.value

    # __ PROPERTIES __

    # EXTERNAL EVENTS

    def on_master_degradation(self):
        self.propose_view_change()

    def check_freshness(self):
        if self.is_state_fresh_enough():
            logger.debug("{} not sending instance change because found state to be fresh enough".format(self))
            return
        self.propose_view_change(Suspicions.STATE_SIGS_ARE_NOT_UPDATED)

    def send_instance_change_if_needed(self, proposed_view_no, reason):
        can, whyNot = self._canViewChange(proposed_view_no)
        # if scheduled action will be awakened after view change completed,
        # then this action must be stopped also.
        if not can and self.view_no < proposed_view_no and self.provider.is_primary_disconnected():
            # Resend the same instance change message if we are not archive
            # InstanceChange quorum
            logger.info("Resend instance change message to all recipients")
            self.sendInstanceChange(proposed_view_no, reason)
            self._timer.schedule(self.config.INSTANCE_CHANGE_TIMEOUT,
                                 self.instance_change_action)
            logger.info("Count of rounds without quorum of "
                        "instance change messages: {}".format(self.instance_change_rounds))
            self.instance_change_rounds += 1
        else:
            # ViewChange procedure was started, therefore stop scheduling
            # resending instanceChange messages
            logger.info("Stop scheduling instance change resending")
            self._timer.cancel(self.instance_change_action)
            self.instance_change_action = None
            self.instance_change_rounds = 0

    def on_primary_loss(self):
        view_no = self.propose_view_change(Suspicions.PRIMARY_DISCONNECTED)
        if self.instance_change_action:
            # It's an action, scheduled for previous instanceChange round
            logger.info("Stop previous instance change resending schedule")
            self._timer.cancel(self.instance_change_action)
            self.instance_change_rounds = 0
        self.instance_change_action = partial(self.send_instance_change_if_needed,
                                              view_no,
                                              Suspicions.PRIMARY_DISCONNECTED)
        self._timer.schedule(self.config.INSTANCE_CHANGE_TIMEOUT,
                             self.instance_change_action)

    # TODO we have `on_primary_loss`, do we need that one?
    def on_primary_about_to_be_disconnected(self):
        self.propose_view_change(Suspicions.PRIMARY_ABOUT_TO_BE_DISCONNECTED)

    def on_suspicious_primary(self, suspicion: Suspicions):
        self.propose_view_change(suspicion)

    def on_view_change_not_completed_in_time(self):
        self.propose_view_change(Suspicions.INSTANCE_CHANGE_TIMEOUT)
        self.provider.schedule_resend_inst_chng()

    def on_replicas_count_changed(self):
        self.propose_view_change(Suspicions.REPLICAS_COUNT_CHANGED)

    # __ EXTERNAL EVENTS __

    def process_instance_change_msg(self, instChg: InstanceChange, frm: str) -> None:
        """
        Validate and process an instance change request.

        :param instChg: the instance change request
        :param frm: the name of the node that sent this `msg`
        """
        if frm not in self.provider.connected_nodes():
            self.provider.discard(
                instChg,
                "received instance change request: {} from {} "
                "which is not in connected list: {}".format(
                    instChg, frm, self.provider.connected_nodes()), logger.info)
            return

        logger.info("{} received instance change request: {} from {}".format(self, instChg, frm))

        # TODO: add sender to blacklist?
        if not isinstance(instChg.viewNo, int):
            self.provider.discard(
                instChg, "{}field view_no has incorrect type: {}".format(
                    VIEW_CHANGE_PREFIX, type(instChg.viewNo)))
        elif instChg.viewNo <= self.view_no:
            self.provider.discard(
                instChg,
                "Received instance change request with view no {} "
                "which is not more than its view no {}".format(
                    instChg.viewNo, self.view_no), logger.info)
        else:
            # Record instance changes for views but send instance change
            # only when found master to be degraded. if quorum of view changes
            #  found then change view even if master not degraded
            self._on_verified_instance_change_msg(instChg, frm)

            if self.instance_changes.has_inst_chng_from(instChg.viewNo, self.name):
                logger.info("{} received instance change message {} but has already "
                            "sent an instance change message".format(self, instChg))
            elif not self.provider.is_master_degraded():
                logger.info("{} received instance change message {} but did not "
                            "find the master to be slow".format(self, instChg))
            else:
                logger.display("{}{} found master degraded after receiving instance change"
                               " message from {}".format(VIEW_CHANGE_PREFIX, self, frm))
                self.sendInstanceChange(instChg.viewNo)

    def send(self, msg):
        """
        Send a message to the node.

        :param msg: the message to send
        """
        logger.debug("{}'s view_changer sending {}".format(self.name, msg))
        self.outBox.append(msg)

    async def serviceQueues(self, limit=None) -> int:
        """
        Service at most `limit` messages from the inBox.

        :param limit: the maximum number of messages to service
        :return: the number of messages successfully processed
        """
        # do not start any view changes until catch-up is finished!
        if not Mode.is_done_syncing(self.provider.node_mode()):
            return 0
        return await self.inBoxRouter.handleAll(self.inBox, limit)

    def sendInstanceChange(self, view_no: int,
                           suspicion=Suspicions.PRIMARY_DEGRADED):
        """
        Broadcast an instance change request to all the remaining nodes

        :param view_no: the view number when the instance change is requested
        """

        # If not found any sent instance change messages in last
        # `ViewChangeWindowSize` seconds or the last sent instance change
        # message was sent long enough ago then instance change message can be
        # sent otherwise no.

        logger.info(
            "{}{} sending an instance change with view_no {}"
            " since {}".format(
                VIEW_CHANGE_PREFIX,
                self,
                view_no,
                suspicion.reason))
        logger.info("{}{} metrics for monitor: {}"
                    .format(MONITORING_PREFIX, self,
                            self.provider.pretty_metrics()))
        msg = self._create_instance_change_msg(view_no, suspicion.code)
        self.send(msg)
        # record instance change vote for self and try to change the view
        # if quorum is reached
        self._on_verified_instance_change_msg(msg, self.name)

    def _create_instance_change_msg(self, view_no, suspicion_code):
        return InstanceChange(view_no, suspicion_code)

    def _on_verified_instance_change_msg(self, msg, frm):
        view_no = msg.viewNo

        if not self.instance_changes.has_inst_chng_from(view_no, frm):
            self.instance_changes.add_vote(msg, frm)
            if view_no > self.view_no:
                self._start_view_change_by_instance_change(view_no)

    def _start_view_change_by_instance_change(self, view_no):
        # TODO: Need to handle skewed distributions which can arise due to
        # malicious nodes sending messages early on
        can, whyNot = self._canViewChange(view_no)
        if can:
            logger.display("{}{} initiating a view change to {} from {}".
                           format(VIEW_CHANGE_PREFIX, self, view_no, self.view_no))
            self.start_view_change(view_no)
        else:
            logger.info(whyNot)
        return can

    def _canViewChange(self, proposedViewNo: int) -> (bool, str):
        """
        Return whether there's quorum for view change for the proposed view
        number and its view is less than or equal to the proposed view
        """
        msg = None
        quorum = self.quorums.view_change.value
        if not self.instance_changes.has_quorum(proposedViewNo, quorum):
            msg = '{} has no quorum for view {}'.format(self, proposedViewNo)
        elif not proposedViewNo > self.view_no:
            msg = '{} is in higher view more than {}'.format(
                self, proposedViewNo)

        return not bool(msg), msg

    def start_view_change(self, proposed_view_no: int, continue_vc=False):
        """
        Trigger the view change process.

        :param proposed_view_no: the new view number after view change.
        """
        self.previous_view_no = self.view_no

        self.provider.notify_view_change_start()
        self.provider.start_view_change(proposed_view_no)

    # TODO: Check whether these still need to be called somewhere after view change:
    #  - self.provider.select_primaries()
    #  - self.provider.notify_view_change_complete()
    #  - self.instance_changes.remove_view(self.view_no)

    def propose_view_change(self, suspicion=Suspicions.PRIMARY_DEGRADED):
        proposed_view_no = self.view_no
        # TODO: For some reason not incrementing view_no in most cases leads to lots of failing/flaky tests
        # if suspicion == Suspicions.INSTANCE_CHANGE_TIMEOUT or not self.view_change_in_progress:
        if suspicion != Suspicions.STATE_SIGS_ARE_NOT_UPDATED or not self.view_change_in_progress:
            proposed_view_no += 1
        self.sendInstanceChange(proposed_view_no, suspicion)
        return proposed_view_no

    def is_state_fresh_enough(self):
        threshold = self.config.ACCEPTABLE_FRESHNESS_INTERVALS_COUNT * self.config.STATE_FRESHNESS_UPDATE_INTERVAL
        return self.provider.state_freshness() < threshold or (not self.view_change_in_progress and
                                                               not Mode.is_done_syncing(self.provider.node_mode()))
