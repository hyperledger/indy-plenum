import random

from plenum.storage.rethinkdb_server import RethinkDB
from plenum.test.eventually import eventually


def getDBPathForMongo(port):
    return "tmp/temp_mongo/{}/".format(port)


def checkReplyIsPersisted(nodes, lpr, reply1):
    """
    This test requires 4 Mongo DB instances running at ports: 27017, 27018,
    27019, 27020
    """

    async def chk(node):
        reply = await node.txnStore.get(reply1.identifier,
                                           reply1.reqId)
        assert reply.viewNo == 0
        assert reply.reqId == 1
        assert reply.result is not None

    for node in nodes:
        lpr.run(eventually(chk, node, retryWait=1, timeout=20))


class TestRethinkDB(RethinkDB):
    def __init__(self, host, port, dirpath: str):
        super().__init__(host, port, dirpath)
        self.portOffset = random.randint(1, 100)

