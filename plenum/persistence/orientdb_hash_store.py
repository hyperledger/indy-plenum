import pyorient

from ledger.immutable_store.store import F
from ledger.immutable_store.stores.hash_store import HashStore
from plenum.persistence.orientdb_store import OrientDbStore


class OrientDbHashStore(HashStore, OrientDbStore):
    """
    Uses OrientDB to store leaf hashes and node hashes
    """

    def __init__(self, user, password, dbName, host="localhost", port=2424,
                 dbType=pyorient.DB_TYPE_DOCUMENT,
                 storageType=pyorient.STORAGE_TYPE_MEMORY):
        super(OrientDbHashStore, self).__init__(user, password, dbName,
                                                host, port, dbType,
                                                storageType)
        self.leafHashClass = "LeafHashes"
        self.nodeHashClass = "NodeHashes"
        self.createClasses(self.classesNeeded())

    def writeLeaf(self, leaf):
        # append the leaf to the leaf store
        pass

    def writeNode(self, node):
        pass

    def readLeaf(self, pos):
        pass

    def readNode(self, pos):
        pass

    def readLeafs(self, startpos, endpos):
        pass

    def readNodes(self, startpos, endpos):
        pass

    def createLeafHashClass(self):
        self._createHashClass(self.leafHashClass, {
            F.serialNo.name: "integer",
            F.leafHash.name: "string"
        }, F.serialNo.name)

    def createNodeHashClass(self):
        self._createHashClass(self.nodeHashClass, {
            F.rowNo.name: "integer",
            F.nodeHash.name: "string"
        }, F.rowNo.name)

    def _createHashClass(self, className, attributes, index):
        self.createClass(className)
        self.createClassProperties(className, attributes)
        self.createIndexOnClass(className, index)

    def classesNeeded(self):
        return [(self.leafHashClass, self.createLeafHashClass),
                (self.nodeHashClass, self.createNodeHashClass)]


