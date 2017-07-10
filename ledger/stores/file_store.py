import logging
import os
import shutil
from hashlib import sha256


class FileStore:
    """
    A file based implementation of a key value store.
    """
    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 ensureDurability: bool=True,
                 delimiter="\t",
                 lineSep="\r\n",
                 defaultFile=None):
        """
        :param dbDir: The directory where the file storing the data would be
        present
        :param dbName: The name of the file that is used to store the data
        :param isLineNoKey: If false then each line has the key followed by a
        delimiter followed by the value
        :param storeContentHash: Whether to store a hash of the value or not.
        Storing hash can make it really fast to compare the value for equality
        :param ensureDurability: Should the file be fysnced after every write.
        This can ensure durability in most of the cases, but make
        writes extremely slow. See testMeasureWriteTime. For frequent writes,
        it makes sense to disable flush and fsync on every write
        :param delimiter: delimiter between key and value
        :param lineSep: line separator - defaults to \r\n
        :param defaultFile: file or dir to use for initialization 

        """
        self.delimiter = delimiter
        self.lineSep = lineSep
        self.isLineNoKey = isLineNoKey
        self.storeContentHash = storeContentHash
        self.ensureDurability = ensureDurability
        self._defaultFile = defaultFile

    def _prepareFiles(self, dbDir, dbName, defaultFile):
        if not defaultFile:
            return
        if not os.path.exists(defaultFile):
            errMessage = "File that should be used for " \
                         "initialization does not exist: {}"\
                         .format(defaultFile)
            logging.warning(errMessage)
            raise ValueError(errMessage)
        dataLocation = os.path.join(self.dbDir, dbName)
        copy = shutil.copy if os.path.isfile(defaultFile) else shutil.copytree
        copy(defaultFile, dataLocation)

    def _prepareDBLocation(self, dbDir, dbName):
        self.dbDir = dbDir
        self.dbName = dbName
        if not os.path.exists(self.dbDir):
            os.makedirs(self.dbDir)
        if not os.path.exists(os.path.join(dbDir, dbName)):
            self._prepareFiles(dbDir, dbName, self._defaultFile)

    def _initDB(self, dbDir, dbName):
        self._prepareDBLocation(dbDir, dbName)

    # noinspection PyUnresolvedReferences
    def put(self, value, key=None):
        # If line no is not treated as key then write the key and then the
        # delimiter
        if not self.isLineNoKey:
            if key is None:
                raise ValueError("Key must be provided for storing the value")
            self.dbFile.write(key)
            self.dbFile.write(self.delimiter)

        self.dbFile.write(value)

        if self.storeContentHash:
            self.dbFile.write(self.delimiter)
            if isinstance(value, str):
                value = value.encode()
            hexedHash = sha256(value).hexdigest()
            self.dbFile.write(hexedHash)
        self.dbFile.write(self.lineSep)

        # A little bit smart strategy like flush every 2 seconds
        # or every 10 writes or every 1 KB may be a better idea
        # Make sure data get written to the disk
        # Even flush slows down writes significantly
        self.dbFile.flush()

        if self.ensureDurability:
            # fsync takes too much time on Windows.
            # This is the reason of test_merkle_proof tests slowness on Windows.
            # Even on Linux using fsync slows down the test by at least 2
            # orders of magnitude. See testMeasureWriteTime
            os.fsync(self.dbFile.fileno())

    def get(self, key):
        for k, v in self.iterator():
            if k == key:
                return v

    def _keyIterator(self, lines, prefix=None):
        return self._baseIterator(lines, prefix, True, False)

    def _valueIterator(self, lines, prefix=None):
        return self._baseIterator(lines, prefix, False, True)

    def _keyValueIterator(self, lines, prefix=None):
        return self._baseIterator(lines, prefix, True, True)

    def _parse_line(self, line, prefix=None, returnKey: bool=True,
                    returnValue: bool=True, key=None):
        if self.isLineNoKey:
            k = key
            v = line
        else:
            k, v = line.split(self.delimiter, 1)
        if returnValue:
            if self.storeContentHash:
                value, _ = v.rsplit(self.delimiter, 1)
            else:
                value = v
        if not prefix or k.startswith(prefix):
            if returnKey and returnValue:
                return k, value
            elif returnKey:
                return k
            elif returnValue:
                return value

    # noinspection PyUnresolvedReferences
    def _baseIterator(self, lines, prefix, returnKey: bool, returnValue: bool):
        i = 1
        for line in lines:
            k = str(i)
            yield self._parse_line(line, prefix, returnKey, returnValue, k)
            if self.isLineNoKey:
                i += 1

    def _lines(self):
        raise NotImplementedError()

    # noinspection PyUnresolvedReferences
    def iterator(self, includeKey=True, includeValue=True, prefix=None):
        if not (includeKey or includeValue):
            raise ValueError("At least one of includeKey or includeValue "
                             "should be true")
        # Move to the beginning of file
        self.dbFile.seek(0)

        lines = self._lines()
        if includeKey and includeValue:
            return self._keyValueIterator(lines, prefix=prefix)
        elif includeValue:
            return self._valueIterator(lines, prefix=prefix)
        else:
            return self._keyIterator(lines, prefix=prefix)

    def is_valid_range(self, start=None, end=None):
        assert self.isLineNoKey
        if start and end:
            assert start <= end

    def get_range(self, start=None, end=None):
        self.is_valid_range(start, end)
        for k, value in self.iterator():
            k = int(k)
            if (start is None or k >= start) and (end is None or k <= end):
                yield k, value
            if end is not None and k > end:
                break

    @property
    def lastKey(self):
        # TODO use the efficient way of seeking to the end and moving back till
        # 2nd newline(1 st newline would be encountered immediately until its a
        # blank file) is encountered and after newline read ahead till the
        # delimiter or split the read string till now on delimiter
        k = None
        for k, v in self.iterator():
            pass
        return k

    def appendNewLineIfReq(self):
        try:
            logging.debug("new line check for file: {}".format(self.dbPath))
            with open(self.dbPath, 'a+b') as f:
                size = f.tell()
                if size > 0:
                    f.seek(-len(self.lineSep), 2)  # last character in file
                    if f.read().decode() != self.lineSep:
                        linesep = self.lineSep if isinstance(self.lineSep, bytes) else self.lineSep.encode()
                        f.write(linesep)
                        logging.debug(
                            "new line added for file: {}".format(self.dbPath))
        except FileNotFoundError:
            pass

    @property
    def numKeys(self):
        return sum(1 for l in self.iterator())

    # noinspection PyUnresolvedReferences
    def close(self):
        self.dbFile.close()

    # noinspection PyUnresolvedReferences
    @property
    def closed(self):
        return self.dbFile.closed

    # noinspection PyUnresolvedReferences
    def reset(self):
        self.dbFile.truncate(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
