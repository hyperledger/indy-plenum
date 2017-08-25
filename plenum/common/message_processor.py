import logging
from typing import Dict

from plenum.common.request import Request
from stp_core.crypto.signer import Signer


class MessageProcessor:
    """
    Helper functions for messages.
    """

    def __init__(self, allowDictOnly=False):
        # if True, message must be converted to Dict before sending.
        self.allowDictOnly = allowDictOnly

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

    def toDict(self, msg: Dict) -> Dict:
        """
        Return a dictionary form of the message

        :param msg: the message to be sent
        :raises: ValueError if msg cannot be converted to an appropriate format
            for transmission
        """

        if isinstance(msg, Request):
            tmsg = msg.as_dict
        elif hasattr(msg, "_asdict"):
            tmsg = dict(msg._asdict())
        elif hasattr(msg, "__dict__"):
            tmsg = dict(msg.__dict__)
        elif self.allowDictOnly:
            raise ValueError("Message cannot be converted to an appropriate "
                             "format for transmission")
        else:
            tmsg = msg
        return tmsg

    def prepForSending(self, msg: Dict, signer: Signer = None) -> Dict:
        msg = self.toDict(msg)
        if signer:
            return signer.sign(msg)
        return msg
