import os
from pathlib import Path


class DirectoryStore:
    """
    A file based implementation of a key value store.
    """

    def __init__(self, baseDir, dbName):
        """
        :param dbDir: The directory where the file storing the data would be
        present
        :param dbName: The name of the file that is used to store the data
        :param isLineNoKey: If false then each line has the key followed by a
        delimiter followed by the value
        :param storeContentHash: Whether to store a hash of the value or not.
        Storing hash can make it really fast to compare the value for equality
        """
        self.baseDir = baseDir
        self.dbName = dbName
        if not os.path.exists(self.baseDir):
            os.makedirs(self.baseDir)
        self.dbPath = os.path.join(self.baseDir, dbName)
        if not os.path.exists(self.dbPath):
            os.makedirs(self.dbPath)

    def keyFilePath(self, key: str):
        return os.path.join(self.dbPath, key)

    def exists(self, key: str):
        return os.path.isfile(self.keyFilePath(key))

    def get(self, key: str):
        if self.exists(key):
            with open(self.keyFilePath(key)) as f:
                return f.read()

    def put(self, key: str, value: str):
        with open(self.keyFilePath(key), "w") as f:
            f.write(value)

    def appendToValue(self, key: str, value: str):
        with open(self.keyFilePath(key), mode="a+") as f:
            f.write(value)
            f.write(os.linesep)

    def iterator(self):
        path = Path(self.dbPath)
        return [(file.name, file.open().read()) for file in path.iterdir()]

    @property
    def keys(self):
        path = Path(self.dbPath)
        return [file.name for file in path.iterdir()]
