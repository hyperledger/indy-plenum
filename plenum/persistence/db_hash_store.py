import storage.helper

from common.exceptions import PlenumValueError
from ledger.hash_stores.hash_store import HashStore
from plenum.common.config_util import getConfig
from stp_core.common.log import getlogger
from plenum.common.constants import KeyValueStorageType, HS_LEVELDB, HS_ROCKSDB

logger = getlogger()


class DbHashStore(HashStore):
    def __init__(self, dataDir, fileNamePrefix="", db_type=HS_LEVELDB, read_only=False, config=None):
        self.dataDir = dataDir
        if db_type not in (HS_ROCKSDB, HS_LEVELDB):
            raise PlenumValueError(
                'db_type', db_type, "one of {}".format((HS_ROCKSDB, HS_LEVELDB))
            )
        self.db_type = KeyValueStorageType.Leveldb if db_type == HS_LEVELDB \
            else KeyValueStorageType.Rocksdb
        self.config = config or getConfig()
        self.nodesDb = None
        self.leavesDb = None
        self._leafCount = 0
        self._read_only = read_only
        self.nodes_db_name = fileNamePrefix + '_merkleNodes'
        self.leaves_db_name = fileNamePrefix + '_merkleLeaves'
        self.open()

    @property
    def is_persistent(self) -> bool:
        return True

    @property
    def read_only(self) -> bool:
        return self._read_only

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
        return self._leafCount

    @property
    def nodeCount(self) -> int:
        return self.nodesDb.size

    @leafCount.setter
    def leafCount(self, count: int) -> None:
        self._leafCount = count

    @property
    def closed(self):
        return (self.nodesDb is None and self.leavesDb is None) \
            or \
               (self.nodesDb.closed and self.leavesDb.closed)

    def open(self):
        self.nodesDb = storage.helper.initKeyValueStorage(
            self.db_type, self.dataDir, self.nodes_db_name,
            read_only=self._read_only, db_config=self.config.db_merkle_nodes_config)
        self.leavesDb = storage.helper.initKeyValueStorage(
            self.db_type, self.dataDir, self.leaves_db_name,
            read_only=self._read_only, db_config=self.config.db_merkle_leaves_config)
        self._leafCount = self.leavesDb.size

    def close(self):
        self.nodesDb.close()
        self.leavesDb.close()

    def reset(self) -> bool:
        self.nodesDb.reset()
        self.leavesDb.reset()
        self.leafCount = 0
        return True
