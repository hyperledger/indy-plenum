from ledger.hash_stores.hash_store import HashStore
from storage.binary_file_store import BinaryFileStore
from storage.kv_store_file import KeyValueStorageFile


class FileHashStore(HashStore):
    # Hashes are stored as raw bytes. By default each leaf hash is of 32 bytes
    # and each node hash is too of 32 bytes. The extra 5 bytes in for each node
    # are used to store the `start` and `height`. `start` takes 4 bytes so it
    # can support upto 1 billion nodes and height takes 1 byte so it can store
    # a tree upto the height of 255
    def __init__(self, dataDir, fileNamePrefix="", leafSize=32, nodeSize=32):
        self.dataDir = dataDir
        self.fileNamePrefix = fileNamePrefix
        nodesFileName = fileNamePrefix + "_merkleNodes"
        leavesFileName = fileNamePrefix + "_merkleLeaves"

        self.nodesFile = BinaryFileStore(self.dataDir, nodesFileName,
                                         isLineNoKey=True,
                                         storeContentHash=False)
        self.leavesFile = BinaryFileStore(self.dataDir, leavesFileName,
                                          isLineNoKey=True,
                                          storeContentHash=False)

        # Do not need line separators since each entry is of fixed size
        self.nodesFile.lineSep = b''
        self.leavesFile.lineSep = b''
        self.nodeSize = nodeSize
        self.leafSize = leafSize

    @property
    def is_persistent(self) -> bool:
        return True

    @staticmethod
    def write(data, store, size):
        if not isinstance(data, bytes):
            data = data.encode()
        dataSize = len(data)
        if dataSize != size:
            raise ValueError(
                "Data size not allowed. Size of the data should be "
                "{} but instead was {}".format(
                    size, dataSize))
        store.put(key=None, value=data)

    @staticmethod
    def read(store: KeyValueStorageFile, entryNo, size):
        store.db_file.seek((entryNo - 1) * size)
        return store.db_file.read(size)

    @staticmethod
    def dataGen(dataFactory, startpos, endpos):
        i = startpos
        while True:
            data = dataFactory(i)
            yield data
            i += 1
            if i <= endpos:
                break

    def writeNode(self, node):
        # TODO: Need to have some exception handling around converting to bytes
        # since they can result in `OverflowError`
        # start, height, nodeHash = node
        # start = start.to_bytes(4, byteorder='little')
        # height = height.to_bytes(1, byteorder='little')
        # data = start + height + nodeHash
        data = node[2]
        self.write(data, self.nodesFile, self.nodeSize)

    def writeLeaf(self, leafHash):
        self.write(leafHash, self.leavesFile, self.leafSize)

    def readNode(self, pos):
        data = self.read(self.nodesFile, pos, self.nodeSize)
        if len(data) < self.nodeSize:
            raise IndexError("No node at given position")
        # start = int.from_bytes(data[:4], byteorder='little')
        # height = int.from_bytes(data[4:5], byteorder='little')
        # nodeHash = data[5:]
        # return start, height, nodeHash
        return data

    def readLeaf(self, pos):
        data = self.read(self.leavesFile, pos, self.leafSize)
        if len(data) < self.leafSize:
            raise IndexError("No leaf at given position")
        return data

    def readLeafs(self, startpos, endpos):
        return self.dataGen(self.readLeaf, startpos, endpos)

    def readNodes(self, startpos, endpos):
        return self.dataGen(self.readNode, startpos, endpos)

    @property
    def leafCount(self) -> int:
        return self.leavesFile.db_file.seek(0, 2) // self.leafSize

    @property
    def nodeCount(self) -> int:
        return self.nodesFile.db_file.seek(0, 2) // self.nodeSize

    @property
    def closed(self):
        return self.nodesFile.closed and self.leavesFile.closed

    def open(self):
        self.nodesFile.open()
        self.leavesFile.open()

    def close(self):
        self.nodesFile.close()
        self.leavesFile.close()

    def reset(self):
        self.nodesFile.reset()
        self.leavesFile.reset()
        return True
