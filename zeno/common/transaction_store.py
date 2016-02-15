import asyncio
import logging
import time
from typing import Dict
from typing import Optional

from zeno.common.request_types import Reply, f, Request


class StoreStopping(Exception):
    pass


class StopTimeout(Exception):
    pass


class TransactionStore:
    """
    The TransactionStore is used to keep a record of all the transactions(client
     requests processed) in the system.
    """

    def __init__(self):
        self.running = True
        self.reset()

    # noinspection PyAttributeOutsideInit
    def reset(self):
        """
        Clear the values of all attributes of the transaction store.
        """
        self.getsCounter = 0

        # dictionary of processed requests for each client. Value for each
        # client is a dictionary with request id as key and transaction id as
        # value
        self.processedRequests = {}  # type: Dict[str, Dict[int, str]]

        # dictionary of received requests for each client. Value for each
        # client is an asyncio Queue
        self.requests = {}  # type: Dict[str, asyncio.Queue]

        # dictionary of responses to be sent for each client. Value for each
        # client is an asyncio Queue
        self.responses = {}  # type: Dict[str, asyncio.Queue]

        # dictionary with key as transaction id as key and `ClientReply` as
        # value
        self.transactions = {}  # type: Dict[str, Reply]

    async def stop(self, timeout: int=5) -> None:
        """
        Try to stop the transaction store in the given timeout or raise an
        exception.
        """
        self.running = False
        start = time.perf_counter()
        while True:
            if self.getsCounter == 0:
                return True
            elif time.perf_counter() <= start + timeout:
                await asyncio.sleep(.1)
            else:
                raise StopTimeout("Stop timed out waiting for {} gets to "
                                  "complete.".format(self.getsCounter))

    async def add_txn(self, request: dict) -> None:
        """
        Add the given client request to the transaction store.
        """
        clientId = request[f.CLIENT_ID.nm]
        if clientId not in self.requests:
            self.requests[clientId] = asyncio.Queue()
        if clientId not in self.responses:
            self.responses[clientId] = asyncio.Queue()
        if clientId not in self.processedRequests:
            self.processedRequests[clientId] = {}

        await self.requests[clientId].put(request)

    # noinspection PyProtectedMember
    async def get_all_txn(self, client_id):
        """
        Fetch all transactions for the given client_id.
        """
        self.getsCounter += 1
        try:
            while self.running:
                if client_id in self.responses and \
                        not self.responses[client_id].empty():
                    reply = await self.responses[client_id].get()
                    logging.debug("Reply being served {}".format(reply))
                    return reply._asdict()
                else:
                    await asyncio.sleep(.1)
            raise StoreStopping()
        finally:
            self.getsCounter -= 1

    async def get_txn(self, txnId: str) -> Optional[Reply]:
        """
        Fetch one transaction by transaction id.
        """
        return self.transactions[txnId] if txnId in self.transactions else None

    def addToProcessedRequests(self,
                               clientId: str,
                               reqId: int,
                               txnId: str,
                               reply: Reply) -> None:
        """
        Add a client request to the transaction store's list of processed
        requests.
        """
        self.transactions[txnId] = reply
        if clientId not in self.processedRequests:
            self.processedRequests[clientId] = {}
        self.processedRequests[clientId][reply.reqId] = txnId

    async def reply(self, clientId: str, reply: Reply, txnId: str=None) -> None:
        """
        Add the given clientReply to this transaction store's list of responses.
        Also add to processedRequests if not added previously.
        """
        logging.debug("Reply being sent {}".format(reply))
        if self.isNewReply(clientId, reply, txnId):
            self.addToProcessedRequests(clientId, reply.reqId, txnId, reply)
        if clientId not in self.responses:
            self.responses[clientId] = asyncio.Queue()
        await self.responses[clientId].put(reply)

    def isNewReply(self, clientId, reply, txnId) -> bool:
        """
        If client is not in `processedRequests` or requestId is not there in
        processed requests and txnId is present then its a new reply
        """
        return (clientId not in self.processedRequests or
                reply.reqId not in self.processedRequests[clientId]) and \
               txnId is not None

    def isRequestAlreadyProcessed(self, req: Request) -> bool:
        """
        Is the request identified by the clientId and reqId already processed?
        """
        if req.clientId in self.processedRequests:
            if req.reqId in self.processedRequests[req.clientId]:
                return self.processedRequests[req.clientId][req.reqId]
        return False
