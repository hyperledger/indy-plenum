from plenum.common.types import Reply


class Storage:

    def start(self, loop):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    async def append(self, identifier: str, reply: Reply, txnId: str):
        raise NotImplementedError()

    async def get(self, identifier: str, reqId: int):
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()
