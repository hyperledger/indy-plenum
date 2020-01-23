import logging
from typing import Dict, Any, Optional, Tuple
from abc import ABCMeta, abstractmethod

from plenum.common.constants import THREE_PC_PREFIX
from plenum.common.exceptions import MismatchedMessageReplyException, IncorrectMessageForHandlingException
from plenum.common.messages.node_messages import MessageReq, MessageRep, PrePrepare, Prepare, Commit, \
    ViewChange, NewView
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.utils import replica_name_to_node_name
from plenum.server.consensus.view_change_storages import view_change_digest


class AbstractMessagesHandler(metaclass=ABCMeta):
    fields = NotImplemented
    msg_cls = NotImplemented

    def __init__(self,
                 data: ConsensusSharedData):
        super().__init__()
        self._data = data
        # Tracks for which keys 'self.msg_cls' have been requested.
        # Cleared in `gc`
        self.requested_messages = {}  # Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self._logger = logging.getLogger()

    @abstractmethod
    def _get_reply(self, params: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def _validate_message_req(self, **kwargs) -> bool:
        pass

    def _create(self, msg: Dict, **kwargs):
        return self.msg_cls(**msg)

    @abstractmethod
    def _create_params(self, key) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _validate_message_rep(self, msg: object) -> None:
        pass

    def extract_message(self, msg: MessageRep, frm: str):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)
        self._logger.debug('{} received requested msg ({}) from {}'.format(self, msg, frm))
        try:
            result = self._create(msg.msg, **params)
            self._validate_message_rep(result)
            return result, frm
        except TypeError:
            raise IncorrectMessageForHandlingException(msg, 'replied message has invalid structure',
                                                       self._logger.warning)
        except MismatchedMessageReplyException:
            raise IncorrectMessageForHandlingException(msg, 'replied message does not satisfy query criteria',
                                                       self._logger.warning)

    def prepare_msg_to_request(self, key,
                               stash_data: Optional[Tuple[str, str, str]] = None) -> Optional[Dict]:
        self.requested_messages[key] = stash_data
        return self._create_params(key)

    def process_message_req(self, msg: MessageReq):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)

        if not self._validate_message_req(**params):
            raise IncorrectMessageForHandlingException(msg, 'cannot serve request',
                                                       self._logger.debug)

        return self._get_reply(params)

    def cleanup(self):
        self.requested_messages.clear()


class ThreePhaseMessagesHandler(AbstractMessagesHandler, metaclass=ABCMeta):
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm,
        'pp_seq_no': f.PP_SEQ_NO.nm
    }

    def prepare_msg_to_request(self, key,
                               stash_data: Optional[Tuple[str, str, str]] = None) -> Optional[Dict]:
        if key in self.requested_messages:
            self._logger.debug('{} not requesting {} since already '
                               'requested for {}'.format(self._data.name, self.msg_cls, key))
            return
        return super().prepare_msg_to_request(key, stash_data)

    def _create_params(self, key) -> Dict[str, Any]:
        return {f.INST_ID.nm: self._data.inst_id,
                f.VIEW_NO.nm: key[0],
                f.PP_SEQ_NO.nm: key[1]}

    def _validate_message_req(self, **kwargs) -> bool:
        return kwargs['inst_id'] == self._data.inst_id and \
            kwargs['view_no'] == self._data.view_no and \
            isinstance(kwargs['pp_seq_no'], int) and \
            kwargs['pp_seq_no'] > 0

    def _create(self, msg: Dict, **kwargs):
        message = super()._create(msg)
        if message.instId != kwargs['inst_id'] \
                or message.viewNo != kwargs['view_no'] \
                or message.ppSeqNo != kwargs['pp_seq_no']:
            raise MismatchedMessageReplyException
        return message

    def _validate_message_rep(self, msg: object) -> None:
        if msg is None:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='received null',
                                                       log_method=self._logger.debug)
        key = (msg.viewNo, msg.ppSeqNo)
        if key not in self.requested_messages:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='Had either not requested this msg or already '
                                                              'received the msg for {}'.format(key),
                                                       log_method=self._logger.debug)
        if self._has_already_ordered(*key):
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='already ordered msg ({})'.format(self, key),
                                                       log_method=self._logger.debug)
        # There still might be stashed msg but not checking that
        # it is expensive, also reception of msgs is idempotent
        stashed_data = self.requested_messages[key]
        curr_data = (msg.digest, msg.stateRootHash, msg.txnRootHash) \
            if isinstance(msg, PrePrepare) or isinstance(msg, Prepare) \
            else None
        if stashed_data is None or curr_data == stashed_data:
            return

        raise IncorrectMessageForHandlingException(msg, reason='{} does not have expected state {}'.
                                                   format(THREE_PC_PREFIX, stashed_data),
                                                   log_method=self._logger.warning)

    def _has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self._data.last_ordered_3pc) >= 0


class PreprepareHandler(ThreePhaseMessagesHandler):
    msg_cls = PrePrepare

    def __init__(self, data: ConsensusSharedData):
        super().__init__(data)
        self._data.requested_pre_prepares = self.requested_messages

    def _get_reply(self, params: Dict[str, Any]) -> Optional[PrePrepare]:
        key = (params['view_no'], params['pp_seq_no'])
        return self._data.sent_preprepares.get(key)

    def _validate_message_rep(self, msg: object):
        super()._validate_message_rep(msg)
        key = (msg.viewNo, msg.ppSeqNo)
        for pp in self._data.preprepared:
            if (pp.view_no, pp.pp_seq_no) == key:
                raise IncorrectMessageForHandlingException(msg,
                                                           reason='already received msg ({})'.format(self, key),
                                                           log_method=self._logger.debug)


class PrepareHandler(ThreePhaseMessagesHandler):
    msg_cls = Prepare

    def _get_reply(self, params: Dict[str, Any]) -> Optional[Prepare]:
        key = (params['view_no'], params['pp_seq_no'])
        if key in self._data.prepares:
            prepare = self._data.prepares[key].msg
            if self._data.prepares.hasPrepareFrom(prepare, self._data.name):
                return prepare
        return None


class CommitHandler(ThreePhaseMessagesHandler):
    msg_cls = Commit

    def _get_reply(self, params: Dict[str, Any]) -> Optional[Commit]:
        key = (params['view_no'], params['pp_seq_no'])
        if key in self._data.commits:
            commit = self._data.commits[key].msg
            if self._data.commits.hasCommitFrom(commit, self._data.name):
                return commit
        return None


class ViewChangeHandler(AbstractMessagesHandler):
    msg_cls = ViewChange
    fields = {
        'inst_id': f.INST_ID.nm,
        'name': f.NAME.nm,
        'digest': f.DIGEST.nm
    }

    def __init__(self, data: ConsensusSharedData):
        super().__init__(data)
        self._received_vc = {}  # Dict[Tuple, Set[str]]

    def _create(self, msg: Dict, **kwargs):
        message = super()._create(msg)
        if view_change_digest(message) != kwargs[f.DIGEST.nm]:
            raise MismatchedMessageReplyException
        return message

    def _validate_message_req(self, **kwargs) -> bool:
        return kwargs['inst_id'] == self._data.inst_id

    def _create_params(self, key) -> Dict[str, Any]:
        return {f.INST_ID.nm: self._data.inst_id,
                f.NAME.nm: key[0],
                f.DIGEST.nm: key[1]}

    def _get_reply(self, params: Dict[str, Any]):
        result = self._data.view_change_votes.get_view_change(params[f.NAME.nm],
                                                              params[f.DIGEST.nm])
        if result:
            result = result._asdict()
        return result

    def _validate_view_change(self, msg: ViewChange, frm: str, params):
        if msg is None:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='received null',
                                                       log_method=self._logger.debug)

        key = (params["name"], view_change_digest(msg))
        if key not in self.requested_messages:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='Had either not requested this msg or already '
                                                              'received the msg for {}'.format(key),
                                                       log_method=self._logger.debug)
        self._received_vc.setdefault(key, set())
        self._received_vc[key].add(frm)
        if not self._data.quorums.weak.is_reached(len(self._received_vc[key])) and frm != params["name"]:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='Count of VIEW_CHANGE messages {} '
                                                              'is not enough for quorum.'.format(msg),
                                                       log_method=self._logger.trace)

    def extract_message(self, msg: MessageRep, frm: str):
        params = {}

        for field_name, type_name in self.fields.items():
            params[field_name] = msg.params.get(type_name)
        self._logger.debug('{} received requested msg ({}) from {}'.format(self, msg, frm))
        try:
            result = self._create(msg.msg, **params)
            self._validate_view_change(result, frm, params)
            self.requested_messages.pop((params[f.NAME.nm], params[f.DIGEST.nm]))
            return result, params["name"]
        except TypeError:
            raise IncorrectMessageForHandlingException(msg, 'replied message has invalid structure',
                                                       self._logger.warning)
        except MismatchedMessageReplyException:
            raise IncorrectMessageForHandlingException(msg, 'replied message does not satisfy query criteria',
                                                       self._logger.warning)

    def cleanup(self):
        super().cleanup()
        self._received_vc.clear()

    def _validate_message_rep(self, msg: object) -> None:
        pass


class NewViewHandler(AbstractMessagesHandler):
    msg_cls = NewView
    fields = {
        'inst_id': f.INST_ID.nm,
        'view_no': f.VIEW_NO.nm
    }

    def _create(self, msg: Dict, **kwargs):
        message = super()._create(msg)
        if message.viewNo != kwargs['view_no']:
            raise MismatchedMessageReplyException
        return message

    def _validate_message_req(self, **kwargs) -> bool:
        return kwargs['inst_id'] == self._data.inst_id and kwargs['view_no'] == self._data.view_no

    def _create_params(self, key) -> Dict[str, Any]:
        return {f.INST_ID.nm: self._data.inst_id,
                f.VIEW_NO.nm: key}

    def _get_reply(self, params: Dict[str, Any]):
        result = self._data.new_view
        if result:
            as_dict = result._asdict()
            as_dict.update({f.PRIMARY.nm: replica_name_to_node_name(self._data.primary_name)})
            result = as_dict
        return result

    def _validate_message_rep(self, msg: NewView) -> None:
        if msg is None:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='received null',
                                                       log_method=self._logger.debug)
        if self._data.new_view:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='already received msg ({})'.format(self, msg),
                                                       log_method=self._logger.debug)
        if msg.viewNo not in self.requested_messages:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='Had either not requested this msg or already '
                                                              'received the msg for view no {}'.format(msg.viewNo),
                                                       log_method=self._logger.debug)
        if msg.viewNo < self._data.view_no:
            raise IncorrectMessageForHandlingException(msg,
                                                       reason='View change for view {} is already '
                                                              'finished'.format(self, msg.viewNo),
                                                       log_method=self._logger.debug)
