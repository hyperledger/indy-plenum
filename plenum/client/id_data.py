from binascii import unhexlify

from plenum.client.signer import Signer, SimpleSigner


class IdData:
    def __init__(self,
                 signer: Signer=None,
                 lastReqId: int=0):
        self.signer = signer
        self.lastReqId = lastReqId

    def __getstate__(self):
        return {
            'key': self.signer.seedHex.decode(),
            'lastReqId': self.lastReqId
        }

    def __setstate__(self, obj):
        self.signer = SimpleSigner(seed=unhexlify(obj['key'].encode()))
        self.lastReqId = obj['lastReqId']
