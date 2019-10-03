from collections import OrderedDict
from typing import Tuple, Optional

import zmq

from plenum.common.exceptions import InvalidMessageExceedingSizeException
from plenum.common.timer import QueueTimer, RepeatingTimer
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.common.log import getlogger

logger = getlogger()


class ClientMessageProvider:
    def __init__(self, name, config, prepare_to_send, metrics, mt_outgoing_size,
                 timer=None, listener=None):
        self._name = name
        self.metrics = metrics
        self.listener = listener
        self._prepare_to_send = prepare_to_send
        self._mt_outgoing_size = mt_outgoing_size
        self._config = config
        self._timer = QueueTimer() if timer is None else timer
        self._pending_client_messages = OrderedDict()
        RepeatingTimer(self._timer, self._config.RESEND_CLIENT_MSG_TIMEOUT, self._send_pending_messages)
        RepeatingTimer(self._timer, self._config.REMOVE_CLIENT_MSG_TIMEOUT, self._remove_old_messages)

    def transmit_through_listener(self, msg, ident) -> Tuple[bool, Optional[str]]:
        self._pending_client_messages.setdefault(ident, []).append((self._timer.get_current_time(),
                                                                    msg))
        if len(self._pending_client_messages) > self._config.PENDING_CLIENT_LIMIT:
            self._pending_client_messages.popitem(last=False)
        if len(self._pending_client_messages[ident]) > self._config.PENDING_MESSAGES_FOR_ONE_CLIENT_LIMIT:
            self._pending_client_messages[ident].pop(0)
        result = True
        error_msg = None
        for timestamp, current_msg in list(self._pending_client_messages[ident]):
            result, error_msg, need_to_resend = self._transmit_one_msg_throughlistener(current_msg,
                                                                                       ident)
            if not need_to_resend:
                self._remove_message(ident, (timestamp, current_msg))
        return result, error_msg

    def _send_pending_messages(self):
        result = True
        error_msg = None
        for ident in list(self._pending_client_messages.keys()):
            for timestamp, current_msg in list(self._pending_client_messages[ident]):
                if self._timer.get_current_time() - timestamp >= self._config.RESEND_CLIENT_MSG_TIMEOUT:
                    result, error_msg, need_to_resend = self._transmit_one_msg_throughlistener(current_msg,
                                                                                               ident)
                    if not need_to_resend:
                        self._remove_message(ident, (timestamp, current_msg))
        return result, error_msg

    def _transmit_one_msg_throughlistener(self, msg, ident) -> Tuple[bool, Optional[str], bool]:

        def prepare_error_msg(ex):
            err_str = '{}{} got error {} while sending through listener to {}' \
                .format(CONNECTION_PREFIX, self, ex, ident)
            logger.debug(err_str)
            return err_str

        need_to_resend = False
        if isinstance(ident, str):
            ident = ident.encode()
        try:
            msg = self._prepare_to_send(msg)
            logger.trace('{} transmitting {} to {} through listener socket'.
                         format(self, msg, ident))
            self.metrics.add_event(self._mt_outgoing_size, len(msg))
            self.listener.send_multipart([ident, msg], flags=zmq.NOBLOCK)
        except InvalidMessageExceedingSizeException as ex:
            err_str = '{}Cannot transmit message. Error {}'.format(CONNECTION_PREFIX, ex)
            logger.debug(err_str)
            return False, err_str, need_to_resend
        except zmq.Again as ex:
            need_to_resend = True
            return False, prepare_error_msg(ex), need_to_resend
        except zmq.ZMQError as ex:
            need_to_resend = (ex.errno == 113)
            return False, prepare_error_msg(ex), need_to_resend
        except Exception as ex:
            return False, prepare_error_msg(ex), need_to_resend
        return True, None, need_to_resend

    def _remove_old_messages(self):
        for ident in list(self._pending_client_messages.keys()):
            for timestamp, current_msg in list(self._pending_client_messages[ident]):
                if self._timer.get_current_time() - timestamp >= self._config.REMOVE_CLIENT_MSG_TIMEOUT:
                    self._remove_message(ident, (timestamp, current_msg))

    def _remove_message(self, ident, msg):
        self._pending_client_messages[ident].remove(msg)
        if not self._pending_client_messages[ident]:
            self._pending_client_messages.pop(ident)

    def __repr__(self):
        return self._name
