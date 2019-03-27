import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import List, Optional, Tuple, Set
from functools import partial

from plenum.common.startable import Mode
from plenum.common.timer import TimerService, RepeatingTimer
from plenum.server.quorums import Quorums
from plenum.server.view_change.instance_change_provider import InstanceChangeProvider
from storage.kv_store import KeyValueStorage
from stp_core.common.log import getlogger
from stp_core.ratchet import Ratchet

from plenum.common.throttler import Throttler
from plenum.common.constants import PRIMARY_SELECTION_PREFIX, \
    VIEW_CHANGE_PREFIX, MONITORING_PREFIX, POOL_LEDGER_ID
from plenum.common.messages.node_messages import InstanceChange, ViewChangeDone, FutureViewChangeDone
from plenum.common.util import mostCommonElement
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
    def has_pool_ledger(self) -> bool:
        pass

    @abstractmethod
    def ledger_summary(self) -> List[Tuple[int, int, str]]:
        pass

    @abstractmethod
    def node_registry(self, size):
        pass

    @abstractmethod
    def is_node_synced(self) -> bool:
        pass

    @abstractmethod
    def node_mode(self) -> Mode:
        pass

    @abstractmethod
    def next_primary_name(self) -> str:
        pass

    @abstractmethod
    def current_primary_name(self) -> str:
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
    def notify_initial_propose_view_change(self):
        pass

    @abstractmethod
    def start_catchup(self):
        pass

    @abstractmethod
    def restore_backup_replicas(self):
        pass

    @abstractmethod
    def select_primaries(self, node_reg):
        pass

    @abstractmethod
    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        pass

    @property
    @abstractmethod
    def node_status_db(self) -> Optional[KeyValueStorage]:
        pass


class ViewChanger():

    def __init__(self, provider: ViewChangerDataProvider, timer: TimerService):
        self.provider = provider
        self._timer = timer
        self.pre_vc_strategy = None

        self._view_no = 0  # type: int

        self.inBox = deque()
        self.outBox = deque()
        self.inBoxRouter = Router(
            (InstanceChange, self.process_instance_change_msg),
            (ViewChangeDone, self.process_vchd_msg),
            (FutureViewChangeDone, self.process_future_view_vchd_msg)
        )

        self.instance_changes = InstanceChangeProvider(self.config.OUTDATED_INSTANCE_CHANGES_CHECK_INTERVAL,
                                                       node_status_db=self.provider.node_status_db)

        # The quorum of `ViewChangeDone` msgs is different depending on whether we're doing a real view change,
        # or just propagating view_no and Primary from `CurrentState` messages sent to a newly joined Node.
        # TODO: separate real view change and Propagation of Primary
        # TODO: separate catch-up, view-change and primary selection so that
        # they are really independent.
        self.propagate_primary = False

        # Tracks if other nodes are indicating that this node is in lower view
        # than others. Keeps a map of view no to senders
        # TODO: Consider if sufficient ViewChangeDone for 2 different (and
        # higher views) are received, should one view change be interrupted in
        # between.
        self._next_view_indications = {}

        self._view_change_in_progress = False

        self.previous_view_no = None
        self.previous_master_primary = None

        self.set_defaults()

        self.initInsChngThrottling()

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
        return self._view_no

    @view_no.setter
    def view_no(self, value):
        logger.info("{} setting view no to {}".format(self.name, value))
        self._view_no = value

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
        return self._view_change_in_progress

    @view_change_in_progress.setter
    def view_change_in_progress(self, value: bool):
        self._view_change_in_progress = value

    @property
    def quorum(self) -> int:
        # TODO: re-factor this, separate this two states (selection of a new
        # primary and propagation of existing one)
        if not self.view_change_in_progress:
            return self.quorums.propagate_primary.value
        if self.propagate_primary:
            return self.quorums.propagate_primary.value
        return self.quorums.view_change_done.value

    @property
    def _hasViewChangeQuorum(self):
        # This method should just be present for master instance.
        """
        Checks whether n-f nodes completed view change and whether one
        of them is the next primary
        """
        num_of_ready_nodes = len(self._view_change_done)
        diff = self.quorum - num_of_ready_nodes
        if diff > 0:
            logger.info('{} needs {} ViewChangeDone messages'.format(self, diff))
            return False

        logger.info("{} got view change quorum ({} >= {})".
                    format(self.name, num_of_ready_nodes, self.quorum))
        return True

    @property
    def _is_propagated_view_change_completed(self):
        if not self._propagated_view_change_completed and \
                self.provider.has_pool_ledger() and \
                self.propagate_primary:

            accepted = self.get_sufficient_same_view_change_done_messages()
            if accepted is not None:
                accepted_pool_ledger_i = \
                    next(filter(lambda x: x[0] == POOL_LEDGER_ID,
                                accepted[1]))
                self_pool_ledger_i = \
                    next(filter(lambda x: x[0] == POOL_LEDGER_ID,
                                self.provider.ledger_summary()))
                logger.info("{} Primary selection has been already completed "
                            "on pool ledger info = {}, primary {}, self pool ledger info {}".
                            format(self, accepted_pool_ledger_i, accepted[0], self_pool_ledger_i))
                self._propagated_view_change_completed = True

        return self._propagated_view_change_completed

    @property
    def has_view_change_from_primary(self) -> bool:
        if not self._has_view_change_from_primary:
            next_primary_name = self.provider.next_primary_name()

            if next_primary_name not in self._view_change_done:
                logger.info("{} has not received ViewChangeDone from the next "
                            "primary {} (view_no: {}, totalNodes: {})".
                            format(self.name, next_primary_name, self.view_no, self.quorums.n))
            else:
                logger.info('{} received ViewChangeDone from primary {}'.format(self, next_primary_name))
                self._has_view_change_from_primary = True

        return self._has_view_change_from_primary

    @property
    def has_acceptable_view_change_quorum(self):
        if not self._has_acceptable_view_change_quorum:
            self._has_acceptable_view_change_quorum = (
                self._hasViewChangeQuorum and
                (self._is_propagated_view_change_completed or
                 self.has_view_change_from_primary)
            )
        return self._has_acceptable_view_change_quorum

    @property
    def is_behind_for_view(self) -> bool:
        # Checks if the node is currently behind the accepted state for this
        # view, only makes sense to call when the node has an acceptable
        # view change quorum
        _, accepted_ledger_summary = self.get_sufficient_same_view_change_done_messages()
        for (ledgerId, own_ledger_size, _), (_, accepted_ledger_size, _) in \
                zip(self.provider.ledger_summary(), accepted_ledger_summary):
            if own_ledger_size < accepted_ledger_size:
                logger.info("{} ledger {} sizes are differ: own {} accepted {}".
                            format(self, ledgerId, own_ledger_size, accepted_ledger_size))
                return True
        return False

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

    def on_catchup_complete(self):
        if self.provider.is_node_synced() \
                and self.provider.is_primary() is None \
                and not self._is_propagated_view_change_completed:
            self._send_view_change_done_message()

        self._start_selection()

    def process_future_view_vchd_msg(self, future_vcd_msg: FutureViewChangeDone, frm):
        # if we already started a view change then do not decide on a new one
        if self.view_change_in_progress:
            return

        from_current_state = future_vcd_msg.from_current_state
        view_no = future_vcd_msg.vcd_msg.viewNo
        # ToDo maybe we should compare with last_completed_view_no instead of viewNo.
        if not ((view_no > self.view_no) or
                (self.view_no == 0 and from_current_state)):
            # it means we already processed this future View Change Done
            return

        if self.view_no == 0 and from_current_state:
            # This is the first Propagate Primary,
            # so we need to make sure that we connected to the real primary for the proposed view
            # see test_view_change_after_back_to_quorum_with_disconnected_primary
            self.provider.notify_initial_propose_view_change()

        if view_no not in self._next_view_indications:
            self._next_view_indications[view_no] = {}
        self._next_view_indications[view_no][frm] = future_vcd_msg.vcd_msg
        self._start_view_change_if_possible(view_no, propagate_primary=from_current_state)

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
                "which is not in connected list: {}".
                format(instChg, frm, self.provider.connected_nodes()), logger.info)
            return

        logger.info("{} received instance change request: {} from {}".format(self, instChg, frm))

        # TODO: add sender to blacklist?
        if not isinstance(instChg.viewNo, int):
            self.provider.discard(
                instChg, "{}field view_no has incorrect type: {}".
                format(VIEW_CHANGE_PREFIX, type(instChg.viewNo)))
        elif instChg.viewNo <= self.view_no:
            self.provider.discard(
                instChg,
                "Received instance change request with view no {} "
                "which is not more than its view no {}".
                format(instChg.viewNo, self.view_no), logger.info)
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

    def process_vchd_msg(self, msg: ViewChangeDone, sender: str) -> bool:
        """
        Processes ViewChangeDone messages. Once n-f messages have been
        received, decides on a primary for specific replica.

        :param msg: ViewChangeDone message
        :param sender: the name of the node from which this message was sent
        """
        logger.info("{}'s primary selector started processing of ViewChangeDone msg from {} : {}".
                    format(self.name, sender, msg))
        view_no = msg.viewNo
        if self.view_no != view_no:
            self.provider.discard(msg, '{} got Primary from {} for view no {} '
                                  'whereas current view no is {}'.
                                  format(self, sender, view_no, self.view_no),
                                  logMethod=logger.info)
            return False

        new_primary_name = msg.name
        if new_primary_name == self.previous_master_primary:
            self.provider.discard(msg, '{} got Primary from {} for {} who was primary of '
                                  'master in previous view too'.
                                  format(self, sender, new_primary_name),
                                  logMethod=logger.info)
            return False

        # Since a node can send ViewChangeDone more than one time
        self._on_verified_view_change_done_msg(msg, sender)
        # TODO why do we check that after the message tracking
        if self.provider.has_primary():
            self.provider.discard(msg, "it already decided primary which is {}".
                                  format(self.provider.current_primary_name()), logger.info)
            return False

        self._start_selection()

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
        canSendInsChange, cooldown = self.insChngThrottler.acquire()
        if canSendInsChange:
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
        else:
            logger.info("{} cannot send instance change sooner then {} seconds".format(self, cooldown))

    # noinspection PyAttributeOutsideInit
    def initInsChngThrottling(self):
        windowSize = self.config.ViewChangeWindowSize
        ratchet = Ratchet(a=2, b=0.05, c=1, base=2, peak=windowSize)
        self.insChngThrottler = Throttler(windowSize, ratchet.get, self._timer.get_current_time)

    def _create_instance_change_msg(self, view_no, suspicion_code):
        return InstanceChange(view_no, suspicion_code)

    def _on_verified_instance_change_msg(self, msg, frm):
        view_no = msg.viewNo

        if not self.instance_changes.has_inst_chng_from(view_no, frm):
            self.instance_changes.add_vote(msg, frm)
            if view_no > self.view_no:
                self.do_view_change_if_possible(view_no)

    def do_view_change_if_possible(self, view_no):
        # TODO: Need to handle skewed distributions which can arise due to
        # malicious nodes sending messages early on
        can, whyNot = self._canViewChange(view_no)
        if can:
            logger.display("{}{} initiating a view change to {} from {}".
                           format(VIEW_CHANGE_PREFIX, self, view_no, self.view_no))
            self.propagate_primary = False
            self.startViewChange(view_no)
        else:
            logger.info(whyNot)
        return can

    def _qourum_is_reached(self, count, propagate_primary):
        if propagate_primary:
            return self.quorums.propagate_primary.is_reached(count)
        else:
            return self.quorums.view_change_done.is_reached(count)

    def _start_view_change_if_possible(self, view_no, propagate_primary=False) -> bool:
        ind_count = len(self._next_view_indications[view_no])
        if self._qourum_is_reached(ind_count, propagate_primary):
            logger.display('{}{} starting view change for {} after {} view change '
                           'indications from other nodes'.format(VIEW_CHANGE_PREFIX, self, view_no, ind_count))
            self.propagate_primary = propagate_primary
            self.startViewChange(view_no)
            return True
        return False

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

    def startViewChange(self, proposed_view_no: int, continue_vc=False):
        """
        Trigger the view change process.

        :param proposed_view_no: the new view number after view change.
        """
        # TODO: consider moving this to pool manager
        # TODO: view change is a special case, which can have different
        # implementations - we need to make this logic pluggable

        if self.pre_vc_strategy and (not self.propagate_primary) and (not continue_vc):
            self.pre_vc_strategy.prepare_view_change(proposed_view_no)
            return
        elif self.pre_vc_strategy:
            self.pre_vc_strategy.on_strategy_complete()

        self.previous_view_no = self.view_no
        self.view_no = proposed_view_no
        self.view_change_in_progress = True
        self.previous_master_primary = self.provider.current_primary_name()
        self.set_defaults()
        self._process_vcd_for_future_view()

        self.initInsChngThrottling()

        self.provider.notify_view_change_start()
        self.provider.start_catchup()

    def _process_vcd_for_future_view(self):
        # make sure that all received VCD messages for future view
        # (including the current view) are stored, as they will be needed for a quorum
        # to finish the View Change and start selection.
        # This is especially critical for Propagate Primary mode (on receiving CURRENT_STATE by a new node).
        if self.view_no in self._next_view_indications:
            for frm, vcd in self._next_view_indications[self.view_no].items():
                # we call _on_verified_view_change_done_msg, not process_vchd_msg,
                # since we may be in propagate primary mode where some of validation inside process_vchd_msg
                # is not correct (for example, checking that the new primary differs from the current one)
                self._on_verified_view_change_done_msg(vcd, frm)

        # remove all for previous views
        for view_no in tuple(self._next_view_indications.keys()):
            if view_no <= self.view_no:
                del self._next_view_indications[view_no]

    def _on_verified_view_change_done_msg(self, msg, frm):
        new_primary_name = msg.name
        ledger_summary = msg.ledgerInfo

        # TODO what is the case when node sends several different
        # view change done messages
        data = (new_primary_name, ledger_summary)
        self._view_change_done[frm] = data

    def _start_selection(self):

        error = None

        if not self.provider.is_node_synced():
            error = "mode is {}".format(self.provider.node_mode())
        elif not self.has_acceptable_view_change_quorum:
            error = "has no view change quorum or no message from next primary"
        else:
            rv = self.get_sufficient_same_view_change_done_messages()
            if rv is None:
                error = "there are not sufficient same ViewChangeDone messages"
            elif not (self._is_propagated_view_change_completed or
                      self._verify_primary(*rv)):
                error = "failed to verify primary"

        if error is not None:
            logger.info('{} cannot start primary selection because {}'.format(self, error))
            return

        if self.is_behind_for_view:
            logger.info('{} is synced and has an acceptable view change quorum '
                        'but is behind the accepted state'.format(self))
            self.provider.start_catchup()
            return

        logger.info("{} starting selection".format(self))

        node_reg = None
        # in case of already completed view change
        # use node registry actual for the moment when it happened
        if self._is_propagated_view_change_completed:
            assert self._accepted_view_change_done_message is not None
            ledger_summary = self._accepted_view_change_done_message[1]
            pool_ledger_size = ledger_summary[POOL_LEDGER_ID][1]
            node_reg = self.provider.node_registry(pool_ledger_size)

        if self.view_change_in_progress:
            self.provider.restore_backup_replicas()

        self.provider.select_primaries(node_reg)

        if self.view_change_in_progress:
            self.view_change_in_progress = False
            self.provider.notify_view_change_complete()
            # when we had INSTANCE_CHANGE message, they added into instanceChanges
            # by msg.view_no. When view change was occured and view_no is changed,
            # then we should delete all INSTANCE_CHANGE messages with current (already changed)
            # view_no (which used in corresponded INSTANCE_CHANGE messages)
            # Therefore we delete all INSTANCE_CHANGE messages from previous and current view number
            self.instance_changes.remove_view(self.view_no)
            self.previous_view_no = None
            self.previous_master_primary = None
            self.propagate_primary = False

    def set_defaults(self):
        # Tracks view change done message
        self._view_change_done = {}  # replica name -> data

        # Set when an appropriate view change quorum is found which has
        # sufficient same ViewChangeDone messages
        self._primary_verified = False

        self._has_view_change_from_primary = False

        self._has_acceptable_view_change_quorum = False

        self._accepted_view_change_done_message = None

        # accept any primary if propagating view change done msgs
        # for already completed view change
        self._propagated_view_change_completed = False

    def get_sufficient_same_view_change_done_messages(self) -> Optional[Tuple]:
        # Returns whether has a quorum of ViewChangeDone messages that are same
        # TODO: Does not look like optimal implementation.
        if self._accepted_view_change_done_message is None and \
                self._view_change_done:
            votes = self._view_change_done.values()
            votes = [(nm, tuple(tuple(i) for i in info)) for nm, info in votes]
            (new_primary, ledger_info), vote_count = mostCommonElement(votes)
            if vote_count >= self.quorum:
                logger.info('{} found acceptable primary {} and ledger info {}'.
                            format(self, new_primary, ledger_info))
                self._accepted_view_change_done_message = (new_primary,
                                                           ledger_info)
            else:
                logger.info('{} does not have acceptable primary, only {} votes for {}'.
                            format(self, vote_count, (new_primary, ledger_info)))

        return self._accepted_view_change_done_message

    def _verify_primary(self, new_primary, ledger_info):
        """
        This method is called when sufficient number of ViewChangeDone
        received and makes steps to switch to the new primary
        """
        expected_primary = self.provider.next_primary_name()
        if new_primary != expected_primary:
            logger.error("{}{} expected next primary to be {}, but majority "
                         "declared {} instead for view {}"
                         .format(PRIMARY_SELECTION_PREFIX, self.name,
                                 expected_primary, new_primary, self.view_no))
            return False

        self._primary_verified = True
        return True
        # TODO: check if ledger status is expected

    def _send_view_change_done_message(self):
        """
        Sends ViewChangeDone message to other protocol participants
        """
        new_primary_name = self.provider.next_primary_name()
        ledger_summary = self.provider.ledger_summary()
        message = ViewChangeDone(self.view_no,
                                 new_primary_name,
                                 ledger_summary)

        logger.info("{} is sending ViewChangeDone msg to all : {}".format(self, message))

        self.send(message)
        self._on_verified_view_change_done_msg(message, self.name)

    # overridden method of PrimaryDecider
    def get_msgs_for_lagged_nodes(self) -> List[ViewChangeDone]:
        # Should not return a list, only done for compatibility with interface
        """
        Returns the last accepted `ViewChangeDone` message.
        If no view change has happened returns ViewChangeDone
        with view no 0 to a newly joined node
        """
        # TODO: Consider a case where more than one node joins immediately,
        # then one of the node might not have an accepted
        # ViewChangeDone message
        messages = []
        accepted = self._accepted_view_change_done_message
        if accepted:
            messages.append(ViewChangeDone(self.last_completed_view_no, *accepted))
        elif self.name in self._view_change_done:
            messages.append(ViewChangeDone(self.last_completed_view_no,
                                           *self._view_change_done[self.name]))
        else:
            logger.info('{} has no ViewChangeDone message to send for view {}'.
                        format(self, self.view_no))
        return messages

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
        return self.provider.state_freshness() < threshold
