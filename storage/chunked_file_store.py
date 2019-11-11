import os

from storage.kv_store_file import KeyValueStorageFile
from storage.text_file_store import TextFileStore


class ChunkedFileStore(KeyValueStorageFile):
    """
    Implements a FileStore with chunking behavior.

    Stores chunks of data into separate files. The chunking of data is
    determined by the `chunkSize` parameter. Each chunk of data is written to a
    different file.
    The naming convention of the files is such that the starting number of each
    chunk is the file name, i.e. for a chunkSize of 1000, the first file would
    be 1, the second 1001 etc.

    Every instance of ChunkedFileStore maintains its own directory for
    storing the chunked data files.
    """

    firstChunkIndex = 1

    @staticmethod
    def _fileNameToChunkIndex(fileName):
        try:
            return int(os.path.splitext(fileName)[0])
        except BaseException:
            return None

    @staticmethod
    def _chunkIndexToFileName(index):
        return str(index)

    # TODO: re-factor arguments, since they should be used depending on the chunk type
    def __init__(self,
                 dbDir,
                 dbName,
                 isLineNoKey: bool=False,
                 storeContentHash: bool=True,
                 chunkSize: int=1000,
                 ensureDurability: bool=True,
                 chunk_creator=None,
                 open=True):
        """

        :param chunkSize: number of items in one chunk. Cannot be lower then number of items in defaultFile
        :param chunkStoreConstructor: constructor of store for single chunk
        """

        super().__init__(dbDir,
                         dbName,
                         isLineNoKey,
                         storeContentHash,
                         ensureDurability,
                         open=False)

        self.chunkSize = chunkSize
        self.itemNum = 1  # chunk size counter
        self.dataDir = os.path.join(dbDir, dbName)  # chunk files destination
        self.currentChunk = None  # type: KeyValueStorageFile
        self.currentChunkIndex = None  # type: int

        # TODO: fix chunk_creator support
        def default_chunk_creator(name):
            return TextFileStore(self.dataDir,
                                 name,
                                 isLineNoKey,
                                 storeContentHash,
                                 ensureDurability)

        self._chunkCreator = chunk_creator or default_chunk_creator
        if open:
            self.open()

    @property
    def is_byte(self) -> bool:
        return self.currentChunk.is_byte

    def _init_db_path(self, dbDir, dbName):
        return os.path.join(os.path.expanduser(dbDir), dbName)

    def _init_db_file(self):
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
        if not os.path.isdir(self.db_path):
            raise ValueError("Transactions file {} is not directory"
                             .format(self.db_path))
        self._useLatestChunk()

    def _useLatestChunk(self) -> None:
        """
        Moves chunk cursor to the last chunk
        """
        self._useChunk(self._findLatestChunk())

    def _findLatestChunk(self) -> int:
        """
        Determine which chunk is the latest
        :return: index of a last chunk
        """
        chunks = self._listChunks()
        if len(chunks) > 0:
            return chunks[-1]
        return ChunkedFileStore.firstChunkIndex

    def _startNextChunk(self) -> None:
        """
        Close current and start next chunk
        """
        if self.currentChunk is None:
            self._useLatestChunk()
        else:
            self._useChunk(self.currentChunkIndex + self.chunkSize)

    def _useChunk(self, index) -> None:
        """
        Switch to specific chunk

        :param index:
        """

        if self.currentChunk is not None:
            if self.currentChunkIndex == index and \
                    not self.currentChunk.closed:
                return
            self.currentChunk.close()

        self.currentChunk = self._openChunk(index)
        self.currentChunkIndex = index
        self.itemNum = self.currentChunk.size + 1

    def _openChunk(self, index) -> KeyValueStorageFile:
        """
        Load chunk from file

        :param index: chunk index
        :return: opened chunk
        """

        return self._chunkCreator(
            ChunkedFileStore._chunkIndexToFileName(index))

    def _get_key_location(self, key) -> (int, int):
        """
        Return chunk no and 1-based offset of key
        :param key:
        :return:
        """
        key = int(key)
        if key == 0:
            return 1, 0
        remainder = key % self.chunkSize
        addend = ChunkedFileStore.firstChunkIndex
        chunk_no = key - remainder + addend if remainder \
            else key - self.chunkSize + addend
        offset = remainder or self.chunkSize
        return chunk_no, offset

    def put(self, key, value) -> None:
        if self.itemNum > self.chunkSize:
            self._startNextChunk()
            self.itemNum = 1
        self.itemNum += 1
        self.currentChunk.put(key, value)

    def get(self, key) -> str:
        """
        Determines the file to retrieve the data from and retrieves the data.

        :return: value corresponding to specified key
        """
        # TODO: get is creating files when a key is given which is more than
        # the store size
        chunk_no, offset = self._get_key_location(key)
        with self._openChunk(chunk_no) as chunk:
            return chunk.get(str(offset))

    def reset(self) -> None:
        """
        Clear all data in file storage.
        """
        self.close()
        for f in os.listdir(self.dataDir):
            os.remove(os.path.join(self.dataDir, f))
        self._useLatestChunk()

    def drop(self):
        self.reset()

    def _lines(self):
        """
        Lines in a store (all chunks)

        :return: lines
        """

        chunkIndices = self._listChunks()
        for chunkIndex in chunkIndices:
            with self._openChunk(chunkIndex) as chunk:
                yield from chunk._lines()

    def _parse_line(self, line, prefix=None, returnKey: bool=True,
                    returnValue: bool=True, key=None):
        # TODO: fix this
        return self.currentChunk._parse_line(line, prefix, returnKey, returnValue, key)

    def close(self):
        if self.currentChunk is not None:
            self.currentChunk.close()
        self.currentChunk = None
        self.currentChunkIndex = None
        self.itemNum = None

    def _listChunks(self):
        """
        Lists stored chunks

        :return: sorted list of available chunk indices
        """
        chunks = []
        for fileName in os.listdir(self.dataDir):
            index = ChunkedFileStore._fileNameToChunkIndex(fileName)
            if index is not None:
                chunks.append(index)
        return sorted(chunks)

    def iterator(self, start=None, end=None, include_key=True, include_value=True, prefix=None):
        """
        Store iterator

        :return: Iterator for data in all chunks
        """
        if not (include_key or include_value):
            raise ValueError("At least one of includeKey or includeValue "
                             "should be true")
        if start or end:
            return self._get_range(start, end)

        lines = self._lines()
        if include_key and include_value:
            return self._keyValueIterator(lines, start=start, end=end, prefix=prefix)
        if include_value:
            return self._valueIterator(lines, start=start, end=end, prefix=prefix)
        return self._keyIterator(lines, start=start, end=end, prefix=prefix)

    def _get_range(self, start=None, end=None):
        self._is_valid_range(start, end)

        if not self.size:
            return

        if start and end and start == end:
            res = self.get(start)
            if res:
                yield (start, res)
        else:
            if start is None:
                start = 1
            if end is None:
                end = self.size
            start_chunk_no, start_offset = self._get_key_location(start)
            end_chunk_no, end_offset = self._get_key_location(end)

            if start_chunk_no == end_chunk_no:
                # If entries lie in the same range
                assert end_offset >= start_offset
                with self._openChunk(start_chunk_no) as chunk:
                    yield from zip(range(start, end + 1),
                                   (l for _, l in chunk.iterator(start=start_offset,
                                                                 end=end_offset)))
            else:
                current_chunk_no = start_chunk_no
                while current_chunk_no <= end_chunk_no:
                    with self._openChunk(current_chunk_no) as chunk:
                        if current_chunk_no == start_chunk_no:
                            yield from ((str(current_chunk_no + int(k) - 1), l) for k, l in
                                        chunk.iterator(start=start_offset))
                        elif current_chunk_no == end_chunk_no:
                            yield from ((str(current_chunk_no + int(k) - 1), l)
                                        for k, l in chunk.iterator(end=end_offset))
                        else:
                            yield from ((str(current_chunk_no + int(k) - 1), l)
                                        for k, l in chunk.iterator(start=1, end=self.chunkSize))
                    current_chunk_no += self.chunkSize

    def _append_new_line_if_req(self):
        self._useLatestChunk()
        self.currentChunk._append_new_line_if_req()

    @property
    def size(self) -> int:
        """
        This will iterate only over the last chunk since the name of the last
        chunk indicates how many lines in total exist in all other chunks
        """
        chunks = self._listChunks()
        num_chunks = len(chunks)
        if num_chunks == 0:
            return 0
        count = (num_chunks - 1) * self.chunkSize
        last_chunk = self._openChunk(chunks[-1])
        count += sum(1 for _ in last_chunk._lines())
        last_chunk.close()
        return count

    @property
    def closed(self):
        return self.currentChunk is None
