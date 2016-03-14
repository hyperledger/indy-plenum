import asyncio
import logging
import time
from typing import Optional

from plenum.common.request_types import Reply
from plenum.storage.storage import Storage


class StoreStopping(Exception):
    pass


class StopTimeout(Exception):
    pass


class TransactionStore(Storage):
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

        # dictionary of responses to be sent for each client. Value for each
        # client is an asyncio Queue
        self.responses = {}  # type: Dict[str, asyncio.Queue]

        # dictionary with key as transaction id and `Reply` as
        # value
        self.transactions = {}  # type: Dict[str, Reply]

    # Used in test only.
    def start(self, loop):
        pass

    # Used in test only.
    def stop(self, timeout: int = 5) -> None:
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
                time.sleep(.1)
            else:
                raise StopTimeout("Stop timed out waiting for {} gets to "
                                  "complete.".format(self.getsCounter))

    def addToProcessedTxns(self,
                           clientId: str,
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

    async def insertTxn(self, clientId: str, reply: Reply,
                        txnId: str = None) -> None:
        """
        Add the given Reply to this transaction store's list of responses.
        Also add to processedRequests if not added previously.
        """
        logging.debug("Reply being sent {}".format(reply))
        if self._isNewTxn(clientId, reply, txnId):
            self.addToProcessedTxns(clientId, txnId, reply)
        if clientId not in self.responses:
            self.responses[clientId] = asyncio.Queue()
        self.responses[clientId].put(reply)

    async def getTxn(self, clientId, reqId) -> Optional[Reply]:
        if clientId in self.processedRequests:
            if reqId in self.processedRequests[clientId]:
                txnId = self.processedRequests[clientId][reqId]
                return self.transactions[txnId]
        else:
            return None

    def _isNewTxn(self, clientId, reply, txnId) -> bool:
        """
        If client is not in `processedRequests` or requestId is not there in
        processed requests and txnId is present then its a new reply
        """
        return (clientId not in self.processedRequests or
                reply.reqId not in self.processedRequests[clientId]) and \
               txnId is not None

