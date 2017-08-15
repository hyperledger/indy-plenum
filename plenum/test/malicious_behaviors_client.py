import types
from logging import getLogger
from typing import Mapping

from plenum.client.client import Client
from plenum.common.request import Request

logger = getLogger()


def makeClientFaulty(client, *behaviors):
    for behavior in behaviors:
        behavior(client=client)


def genDoesntSendRequestToSomeNodes(*nodeName: str,
                                    skipCount: int = 0) -> Client:
    """
    Client does not send request to some nodes

    :param nodeName: request won't be sent to these nodes
    :param skipCount: skips the specified number of
    remotes from the client's list of remotes taken from the front of the list
    :return: a function to create a faulty client
    """

    def inner(client: Client) -> Client:
        if nodeName:
            skipIds = [client.nodestack.getRemote(nn).uid for nn in nodeName]
            ovrdRids = [rid for rid in client.nodestack.remotes.keys()
                        if rid not in skipIds]
        else:
            ovrdRids = list(client.nodestack.remotes.keys())[skipCount:]

        def evilSend(self, msg, *rids, signer=None) -> None:
            logger.debug("EVIL: sending to less nodes {}, ignoring passed "
                         "rids {} and sending to {} instead.".
                         format(msg, rids, ovrdRids))
            return self.nodestack.send(msg, *ovrdRids, signer=signer)

        client.send = types.MethodType(evilSend, client)
        return client

    return inner


# This can be used to test blacklisting a client or throttling of requests.
# DEPR
# def repeatsRequest(client: Client, count: int) -> Client:
#     """
#     Client sends count number of requests for each operation.
#     Different requestIds will be generated for the same operation.
#
#     :param client: the client to make faulty
#     :param count: the number of requests to send for each operation.
#     :return: the faulty client
#     """
#
#     def evilSubmit(self, *operations: Mapping) -> List[Request]:
#         requests = []
#         logger.debug(
#             "EVIL: client creates {} requests for each operation".format(count))
#         for op in operations:
#             for _ in range(count):
#                 request = self.createRequest(op)
#                 self.nodestack.send(request)
#                 requests.append(request)
#         return requests
#
#     client.submit_DEPRECATED = types.MethodType(evilSubmit, client)
#     return client


def sendsUnsignedRequest(client) -> Client:
    """
    Client sends unsigned request to some nodes

    :param client: the client to make faulty
    :return: the faulty client
    """

    def evilSign(self, msg, signer) -> Mapping:
        logger.debug("EVIL: client doesn't sign any of the requests")
        return msg
    client.nodestack.sign = types.MethodType(evilSign, client)
    return client
