import logging
from typing import Dict, Any, Optional, Tuple, Callable
from abc import ABCMeta, abstractmethod

from plenum.common.constants import THREE_PC_PREFIX
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import MismatchedMessageReplyException
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_messages import MessageReq, MessageRep, \
    LedgerStatus, PrePrepare, ConsistencyProof, Propagate, Prepare, Commit
from plenum.common.txn_util import TxnUtilConfig
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from stp_core.common.log import getlogger


logger = getlogger()


class BaseHandler(metaclass=ABCMeta):

    fields = NotImplemented

    @abstractmethod
    def validate(self, **kwargs) -> bool:
        pass

    @abstractmethod
    def create(self, msg: Dict, **kwargs) -> Any:
        pass

    @abstractmethod
    def requestor(self, params: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def processor(self, validated_msg: object, params: Dict[str, Any], frm: str) -> Optional:
        pass

    def serve(self, msg: MessageReq):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self.validate(**params):
            self.discard(msg, 'cannot serve request',
                              logMethod=logger.debug)
            return None

        return self.requestor(params)

    def process(self, msg: MessageRep, frm: str):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self.validate(**params):
            self.discard(msg, 'cannot process message reply',
                              logMethod=logger.debug)
            return

        try:
            valid_msg = self.create(msg.msg, **params)
            return self.processor(valid_msg, params, frm)
        except TypeError:
            self.discard(msg, 'replied message has invalid structure',
                              logMethod=logger.warning)
        except MismatchedMessageReplyException:
            self.discard(msg, 'replied message does not satisfy query criteria',
                              logMethod=logger.warning)

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        """
        Discard a message and log a reason using the specified `logMethod`.

        :param msg: the message to discard
        :param reason: the reason why this message is being discarded
        :param logMethod: the logging function to be used
        :param cliOutput: if truthy, informs a CLI that the logged msg should
        be printed
        """
        reason = "" if not reason else " because {}".format(reason)
        logMethod("{} discarding message {}{}".format(self, msg, reason),
                  extra={"cli": cliOutput})



class LedgerStatusHandler(BaseHandler):
    fields = {
        'ledger_id': f.LEDGER_ID.nm
    }

    def __init__(self, node):
        super().__init__()
        self.node = node

    def validate(self, **kwargs) -> bool:
        return kwargs['ledger_id'] in self.node.ledger_ids

    def create(self, msg: Dict, **kwargs) -> LedgerStatus:
        ls = LedgerStatus(**msg)
        if ls.ledgerId != kwargs['ledger_id']:
            raise MismatchedMessageReplyException
        return ls

    def requestor(self, params: Dict[str, Any]) -> LedgerStatus:
        return self.node.getLedgerStatus(params['ledger_id'])

    def processor(self, validated_msg: LedgerStatus, params: Dict[str, Any], frm: str) -> None:
        self.node.ledgerManager.processLedgerStatus(validated_msg, frm=frm)


class ConsistencyProofHandler(BaseHandler):
    fields = {
        'ledger_id': f.LEDGER_ID.nm,
        'seq_no_start': f.SEQ_NO_START.nm,
        'seq_no_end': f.SEQ_NO_END.nm
    }

    def __init__(self, node):
        super().__init__()
        self.node = node

    def validate(self, **kwargs) -> bool:
        return kwargs['ledger_id'] in self.node.ledger_ids and \
            (isinstance(kwargs['seq_no_start'], int) and kwargs[
             'seq_no_start'] > 0) and \
            (isinstance(kwargs['seq_no_end'], int) and kwargs[
             'seq_no_end'] > 0)

    def create(self, msg: Dict, **kwargs) -> ConsistencyProof:
        cp = ConsistencyProof(**msg)
        if cp.ledgerId != kwargs['ledger_id'] \
                or cp.seqNoStart != kwargs['seq_no_start'] \
                or cp.seqNoEnd != kwargs['seq_no_end']:
            raise MismatchedMessageReplyException
        return cp

    def requestor(self, params: Dict[str, Any]) -> ConsistencyProof:
        return self.node.ledgerManager._node_seeder._build_consistency_proof(
            params['ledger_id'],
            params['seq_no_start'],
            params['seq_no_end'])

    def processor(self, validated_msg: ConsistencyProof, params: Dict[str, Any], frm: str) -> None:
        self.node.ledgerManager.processConsistencyProof(validated_msg, frm=frm)


class PropagateHandler(BaseHandler):
    fields = {
        'digest': f.DIGEST.nm
    }

    def __init__(self, node):
        super().__init__()
        self.node = node

    def validate(self, **kwargs) -> bool:
        return kwargs['digest'] is not None

    def create(self, msg: Dict, **kwargs) -> Propagate:
        ppg = Propagate(**msg)
        request = TxnUtilConfig.client_request_class(**ppg.request)
        if request.digest != kwargs['digest']:
            raise MismatchedMessageReplyException
        return ppg

    def requestor(self, params: Dict[str, Any]) -> Optional[Propagate]:
        req_key = params[f.DIGEST.nm]
        if req_key in self.node.requests and self.node.requests[req_key].finalised:
            sender_client = self.node.requestSender.get(req_key)
            req = self.node.requests[req_key].finalised
            return self.node.createPropagate(req, sender_client)
        return None

    def processor(self, validated_msg: Propagate, params: Dict[str, Any], frm: str) -> None:
        self.node.processPropagate(validated_msg, frm)


class ThreePhaseMessagesHandler(BaseHandler, metaclass=ABCMeta):
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }
    msg_cls = NotImplemented

    def __init__(self,
                 data: ConsensusSharedData):
        super().__init__()
        self._data = data
        self.requested_messages = {}  # Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self._logger = logging.getLogger()

    def validate(self, **kwargs) -> bool:
        return kwargs['inst_id'] == self._data.inst_id and \
               kwargs['view_no'] == self._data.view_no and \
               isinstance(kwargs['pp_seq_no'], int) and \
               kwargs['pp_seq_no'] > 0

    def create(self, msg: Dict, **kwargs):
        message = self.msg_cls(**msg)
        if message.instId != kwargs['inst_id'] \
                or message.viewNo != kwargs['view_no'] \
                or message.ppSeqNo != kwargs['pp_seq_no']:
            raise MismatchedMessageReplyException
        return message

    def processor(self,
                  validated_msg: MessageBase,
                  params: Dict[str, Any],
                  frm: str) -> Optional[Tuple[MessageBase, str]]:
        # TODO: remove this method after BaseHandler refactoring
        pass

    def process(self, msg: MessageRep, frm: str):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)
        self._logger.debug('{} received requested msg ({}) from {}'.format(self, msg, frm))
        result, error_msg = self._validate_reply_msg(**params)
        if not result:
            self.discard(msg, '{} cannot process message reply "{}" because {}'.format(self._data.name, msg, error_msg),
                              logMethod=logger.debug)
            return

        try:
            return self.create(msg.msg, **params)
        except TypeError:
            self.discard(msg, 'replied message has invalid structure',
                              logMethod=logger.warning)
        except MismatchedMessageReplyException:
            self.discard(msg, 'replied message does not satisfy query criteria',
                              logMethod=logger.warning)

    def prepare_msg_to_request(self, three_pc_key: Tuple[int, int],
                                 stash_data: Optional[Tuple[str, str, str]] = None) -> Optional[Dict]:
        if three_pc_key in self.requested_messages:
            self._logger.debug('{} not requesting {} since already '
                               'requested for {}'.format(self._data.name, self.msg_cls, three_pc_key))
            return
        self.requested_messages[three_pc_key] = stash_data
        return {f.INST_ID.nm: self._data.inst_id,
                f.VIEW_NO.nm: three_pc_key[0],
                f.PP_SEQ_NO.nm: three_pc_key[1]}

    def gc(self):
        self.requested_messages.clear()

    def _validate_reply_msg(self, msg: object):
        if msg is None:
            return False, "received null"
        key = (msg.viewNo, msg.ppSeqNo)
        if key not in self.requested_messages:
            return False, 'Had either not requested this msg or already ' \
                          'received the msg for {}'.format(key)
        if self._has_already_ordered(*key):
            return False, 'already ordered msg ({})'.format(self, key)
        # There still might be stashed msg but not checking that
        # it is expensive, also reception of msgs is idempotent
        stashed_data = self.requested_messages[key]
        curr_data = (msg.digest, msg.stateRootHash, msg.txnRootHash) \
            if isinstance(msg, PrePrepare) or isinstance(msg, Prepare) \
            else None
        if stashed_data is None or curr_data == stashed_data:
            return True, None

        self.discard(msg, reason='{} does not have expected state {}'.
                     format(THREE_PC_PREFIX, stashed_data),
                     logMethod=self._logger.warning)
        return False, '{} does not have expected state {}'.format(THREE_PC_PREFIX, stashed_data)

    def _has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self._data.last_ordered_3pc) >= 0


class PreprepareHandler(ThreePhaseMessagesHandler):
    msg_cls = PrePrepare

    def requestor(self, params: Dict[str, Any]) -> Optional[PrePrepare]:
        key = (params['view_no'], params['pp_seq_no'])
        return self._data.sent_preprepares.get(key)

    def _validate_reply_msg(self, msg: object):
        result, error_msg = super()._validate_reply_msg(msg)
        key = (msg.viewNo, msg.ppSeqNo)
        if result:
            for pp in self._data.preprepared:
                if (pp.view_no, pp.pp_seq_no) == key:
                    return False, 'already received msg ({})'.format(self, key)
        return result, error_msg


class PrepareHandler(ThreePhaseMessagesHandler):
    msg_cls = Prepare

    def requestor(self, params: Dict[str, Any]) -> Optional[Prepare]:
        key = (params['view_no'], params['pp_seq_no'])
        if key in self._data.prepares:
            prepare = self._data.prepares[key].msg
            if self._data.prepares.hasPrepareFrom(prepare, self._data.name):
                return prepare
        return None


class CommitHandler(ThreePhaseMessagesHandler):
    msg_cls = Commit

    def requestor(self, params: Dict[str, Any]) -> Optional[Commit]:
        key = (params['view_no'], params['pp_seq_no'])
        if key in self._data.commits:
            commit = self._data.commits[key].msg
            if self._data.commits.hasCommitFrom(commit, self._data.name):
                return commit
        return None
