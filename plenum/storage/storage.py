from plenum.common.request_types import Reply


class Storage:

    def start(self, loop):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    async def insertTxn(self, identifier: str, reply: Reply, txnId: str):
        raise NotImplementedError()

    async def getTxn(self, identifier: str, reqId: int):
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()
