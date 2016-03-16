from plenum.common.request_types import Reply


class Storage:

    def start(self, loop):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    async def append(self, clientId: str, reply: Reply, txnId: str):
        raise NotImplementedError()

    async def get(self, clientId: str, reqId: int):
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()
