# DEPR
# Deprecated in favour of passing request id store for each specific client
# class IdData:
#
#     def __init__(self,
#                  signer: Signer=None,
#                  lastReqId: int=0):
#         self.signer = signer
#         self._lastReqId = lastReqId
#
#     def __getstate__(self):
#         return {
#             'key': self.signer.seedHex.decode(),
#             'lastReqId': self.lastReqId
#         }
#
#     def __setstate__(self, obj):
#         self.signer = SimpleSigner(seed=unhexlify(obj['key'].encode()))
#         self._lastReqId = obj['lastReqId']
#
#     @property
#     def lastReqId(self):
#         return self._lastReqId
#
#     def refresh(self):
#         self._lastReqId += 1