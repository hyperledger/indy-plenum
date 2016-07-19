from base64 import b64encode, b64decode

from ledger.stores.hash_store import HashStore
from ledger.util import F
from plenum.common.util import getlogger
from plenum.persistence.orientdb_store import OrientDbStore


logger = getlogger()


class OrientDbHashStore(HashStore):
    """
    Uses OrientDB to store leaf hashes and node hashes
    """

    def __init__(self, store: OrientDbStore):
        self.store = store
        self.leafHashClass = "LeafHashStore"
        self.nodeHashClass = "NodeHashStore"
        self.store.createClasses(self.classesNeeded())
        self._leafCount = self.leafCount

    def writeLeaf(self, leafHash):
        self.store.client.command(
            "insert into {} (seqNo, leafHash) values ({}, '{}')".format(
                self.leafHashClass, self.leafCount + 1, self._tob64(leafHash)))
        self.leafCount += 1

    def writeNode(self, node):
        start, height, nodeHash = node
        seqNo = self.getNodePosition(start, height)
        self.store.client.command(
            "insert into {} (seqNo, nodeHash) values ({}, '{}')".format(
                self.nodeHashClass, seqNo, self._tob64(nodeHash)))

    @staticmethod
    def _tob64(data):
        return b64encode(data).decode()

    @staticmethod
    def _fromb64(data):
        return b64decode(data.encode())

    def readLeaf(self, seqNo):
        return self._readOne(seqNo, self.leafHashClass, F.leafHash.name)

    def readNode(self, seqNo):
        return self._readOne(seqNo, self.nodeHashClass, F.nodeHash.name)

    def _readOne(self, pos, hashClass, attrib):
        self._validatePos(pos)
        resultSet = self.store.client.command(
            "select from {} where seqNo={} limit 1".format(
                hashClass, pos))
        if resultSet:
            return self._fromb64(resultSet[0].oRecordData[attrib])
        else:
            logger.error("{} does not have position {}".format(hashClass, pos))

    def readLeafs(self, start, end):
        return self._readMultiple(start, end, self.leafHashClass,
                                  F.leafHash.name)

    def readNodes(self, start, end):
        return self._readMultiple(start, end, self.nodeHashClass,
                                  F.nodeHash.name)

    def _readMultiple(self, start, end, hashClass, attrib):
        """
        Returns a list of hashes with serial numbers between start
         and end, both inclusive.
         """
        self._validatePos(start, end)
        resultSet = self.store.client.command(
            "select from {} where seqNo between {} and {} order by seqNo asc"
                .format(hashClass, start, end))
        return [self._fromb64(r.oRecordData[attrib]) for r in resultSet]

    @property
    def leafCount(self) -> int:
        result = self.store.client.command("select count(*) from {}".
                                           format(self.leafHashClass))
        return result[0].oRecordData['count']

    @leafCount.setter
    def leafCount(self, count: int) -> None:
        self._leafCount = count

    @staticmethod
    def _validatePos(start, end=None):
        if end:
            assert start < end, "start index must be less than end index"
        if start < 1:
            raise IndexError(
                "seqNo starts from 1, index requested: {}".format(start))

    def createLeafHashClass(self):
        self._createHashClass(self.leafHashClass, {
            F.seqNo.name: "long",
            F.leafHash.name: "string"
        }, F.leafHash.name)

    def createNodeHashClass(self):
        self._createHashClass(self.nodeHashClass, {
            F.seqNo.name: "long",
            F.nodeHash.name: "string"
        }, F.nodeHash.name)

    def _createHashClass(self, className, attributes, index):
        self.store.createClass(className)
        self.store.createClassProperties(className, attributes)
        self.store.createIndexOnClass(className, index, "unique")

    def classesNeeded(self):
        return [(self.leafHashClass, self.createLeafHashClass),
                (self.nodeHashClass, self.createNodeHashClass)]
