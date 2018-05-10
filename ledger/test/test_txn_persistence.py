import asyncio
import time
from collections import OrderedDict, namedtuple
from ledger.util import F

Reply = namedtuple("REPLY", ['result'])

fields = OrderedDict([
    ("identifier", (str, str)),
    ("reqId", (str, int)),
    ("txnId", (str, str)),
    ("txnTime", (str, float)),
    ("txnType", (str, str)),
])


def testTxnPersistence(ledger):
    loop = asyncio.get_event_loop()

    def go():
        identifier = "testClientId"
        reply = Reply(result={
            "identifier": identifier,
            "reqId": 1,
            "op": "buy"
        })
        sizeBeforeInsert = ledger.size
        ledger.append(reply.result)
        txn_in_db = ledger.get(identifier=identifier,
                               reqId=reply.result['reqId'])
        assert txn_in_db == reply.result
        assert ledger.size == sizeBeforeInsert + 1
        ledger.reset()
        ledger.stop()

    go()
    loop.close()
