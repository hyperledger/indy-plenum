from binascii import unhexlify

from plenum.client.signer import Signer, SimpleSigner
from plenum.persistence.client_req_rep_store import ClientReqRepStore

class IdData:

    def __init__(self,
                 signer: Signer=None,
                 lastReqId: int=0):
        self.signer = signer
        self._lastReqId = lastReqId

    def __getstate__(self):
        return {
            'key': self.signer.seedHex.decode(),
            'lastReqId': self.lastReqId
        }

    def __setstate__(self, obj):
        self.signer = SimpleSigner(seed=unhexlify(obj['key'].encode()))
        self._lastReqId = obj['lastReqId']

    @property
    def lastReqId(self):
        return self._lastReqId

    def refresh(self):
        self._lastReqId += 1

class StoredIdData(IdData):

    def __init__(self,
                 reqRepStore: ClientReqRepStore,
                 signer: Signer=None):
        self.reqRepStore = reqRepStore
        super().__init__(signer, 0)

    @property
    def lastReqId(self) -> int:
        return self.reqRepStore.lastReqId  # type: int

    def refresh(self):
        pass