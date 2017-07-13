import os

from ledger.stores.hash_store import HashStore
from state.kv.kv_store_leveldb import KeyValueStorageLeveldb
from stp_core.common.log import getlogger


logger = getlogger()


class LevelDbHashStore(HashStore):
    def __init__(self, dataDir):
        self.dataDir = dataDir
        self.nodesDbPath = os.path.join(self.dataDir, '_merkleNodes')
        self.leavesDbPath = os.path.join(self.dataDir, '_merkleLeaves')
        self.nodesDb = None
        self.leavesDb = None
        self.open()

    @property
    def is_persistent(self) -> bool:
        return True

    def writeLeaf(self, leafHash):
        self.leavesDb.put(str(self.leafCount + 1), leafHash)
        self.leafCount += 1

    def writeNode(self, node):
        start, height, nodeHash = node
        seqNo = self.getNodePosition(start, height)
        self.nodesDb.put(str(seqNo), nodeHash)

    def readLeaf(self, seqNo):
        return self._readOne(seqNo, self.leavesDb)

    def readNode(self, seqNo):
        return self._readOne(seqNo, self.nodesDb)

    def _readOne(self, pos, db):
        self._validatePos(pos)
        try:
            # Converting any bytearray to bytes
            return bytes(db.get(str(pos)))
        except KeyError:
            logger.error("{} does not have position {}".format(db, pos))

    def readLeafs(self, start, end):
        return self._readMultiple(start, end, self.leavesDb)

    def readNodes(self, start, end):
        return self._readMultiple(start, end, self.nodesDb)

    def _readMultiple(self, start, end, db):
        """
        Returns a list of hashes with serial numbers between start
         and end, both inclusive.
         """
        self._validatePos(start, end)
        # Converting any bytearray to bytes
        return [bytes(db.get(str(pos))) for pos in range(start, end+1)]

    @property
    def leafCount(self) -> int:
        return self.leavesDb.size

    @property
    def nodeCount(self) -> int:
        return self.nodesDb.size

    @leafCount.setter
    def leafCount(self, count: int) -> None:
        self._leafCount = count

    @property
    def closed(self):
        return self.nodesDb is None and self.leavesDb is None

    def open(self):
        self.nodesDb = KeyValueStorageLeveldb(self.nodesDbPath)
        self.leavesDb = KeyValueStorageLeveldb(self.leavesDbPath)

    def close(self):
        self.nodesDb.close()
        self.leavesDb.close()

    def reset(self) -> bool:
        self.nodesDb.close()
        self.nodesDb.drop()
        self.nodesDb.open()

        self.leavesDb.close()
        self.leavesDb.drop()
        self.leavesDb.open()

        self.leafCount = 0

        return True
