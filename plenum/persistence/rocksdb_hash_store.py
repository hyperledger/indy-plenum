from ledger.hash_stores.hash_store import HashStore
from storage.kv_store_rocksdb import KeyValueStorageRocksdb
from stp_core.common.log import getlogger


logger = getlogger()


class RocksDbHashStore(HashStore):
    def __init__(self, dataDir, fileNamePrefix=""):
        self.dataDir = dataDir
        self.nodesDb = None
        self.leavesDb = None
        self.nodes_db_name = fileNamePrefix + '_merkleNodes'
        self.leaves_db_name = fileNamePrefix + '_merkleLeaves'
        self.open()

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
        return [bytes(db.get(str(pos))) for pos in range(start, end + 1)]

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
        self.nodesDb = KeyValueStorageRocksdb(self.dataDir, self.nodes_db_name)
        self.leavesDb = KeyValueStorageRocksdb(
            self.dataDir, self.leaves_db_name)

    def close(self):
        self.nodesDb.close()
        self.leavesDb.close()

    def reset(self) -> bool:
        self.nodesDb.reset()
        self.leavesDb.reset()
        self.leafCount = 0
        return True
