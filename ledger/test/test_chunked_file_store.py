import os
import random

import math
from time import perf_counter

import itertools
import pytest

from ledger.stores.chunked_file_store import ChunkedFileStore
from ledger.stores.text_file_store import TextFileStore


def countLines(fname) -> int:
    with open(fname) as f:
        return sum(1 for _ in f)


def getValue(key) -> str:
    return str(key) + " Some data"


chunkSize = 3
dataSize = 101
data = [getValue(i) for i in range(1, dataSize+1)]


@pytest.fixture(scope="module")
def chunkedTextFileStore() -> ChunkedFileStore:
    return ChunkedFileStore("/tmp", "chunked_data", True, True, chunkSize,
                            chunkStoreConstructor=TextFileStore)


@pytest.yield_fixture(scope="module")
def populatedChunkedFileStore(chunkedTextFileStore) -> ChunkedFileStore:
    store = chunkedTextFileStore
    store.reset()
    dirPath = "/tmp/chunked_data"
    for d in data:
        store.put(d)
    allFiles = []
    for root, dirs, files in os.walk(dirPath):
        for name in files:
            assert countLines(root + os.path.sep + name) <= chunkSize
            allFiles.append(name)
    assert len(allFiles) == math.ceil(dataSize / chunkSize)
    yield store
    store.close()


def testWriteToNewFileOnceChunkSizeIsReached(populatedChunkedFileStore):
    pass


def testRandomRetrievalFromChunkedFiles(populatedChunkedFileStore):
    keys = [2*chunkSize,
            3*chunkSize+1,
            3*chunkSize+chunkSize,
            random.randrange(1, dataSize + 1)]
    for key in keys:
        value = getValue(key)
        assert populatedChunkedFileStore.get(key) == value


def testSizeChunkedFileStore(populatedChunkedFileStore):
    """
    Check performance of `numKeys`
    """
    s = perf_counter()
    c1 = sum(1 for l in populatedChunkedFileStore.iterator())
    e = perf_counter()
    t1 = e - s
    s = perf_counter()
    c2 = populatedChunkedFileStore.numKeys
    e = perf_counter()
    t2 = e - s
    # It should be faster to use ChunkedStore specific implementation
    # of `numKeys`
    assert t1 > t2
    assert c1 == c2
    assert c2 == dataSize


def testIterateOverChunkedFileStore(populatedChunkedFileStore):
    store = populatedChunkedFileStore
    for k, v in store.iterator():
        assert data[int(k)-1] == v


def test_get_range(populatedChunkedFileStore):
    # Test for range spanning multiple chunks

    # Range begins and ends at chunk boundaries
    num = 0
    for k, v in populatedChunkedFileStore.get_range(chunkSize+1, 2*chunkSize):
        assert data[int(k) - 1] == v
        num += 1
    assert num == chunkSize

    # Range does not begin or end at chunk boundaries
    num = 0
    for k, v in populatedChunkedFileStore.get_range(chunkSize+2, 2*chunkSize+1):
        assert data[int(k) - 1] == v
        num += 1
    assert num == chunkSize

    # Range spans multiple full chunks
    num = 0
    for k, v in populatedChunkedFileStore.get_range(chunkSize + 2,
                                                    5 * chunkSize + 1):
        assert data[int(k) - 1] == v
        num += 1
    assert num == 4*chunkSize

    with pytest.raises(AssertionError):
        list(populatedChunkedFileStore.get_range(5, 1))

    for frm, to in [(i, j) for i, j in itertools.permutations(
            range(1, dataSize+1), 2) if i <= j]:
        for k, v in populatedChunkedFileStore.get_range(frm, to):
            assert data[int(k) - 1] == v


def test_chunk_size_limitation_when_default_file_used(tmpdir):
    """
    This test checks that chunk size can not be lower then a number of items 
    in default file, used for initialization of ChunkedFileStore
    """

    isLineNoKey = True
    storeContentHash = False
    ensureDurability = True
    dbDir = str(tmpdir)
    defaultFile = os.path.join(dbDir, "template")

    lines = [
        "FirstLine\n",
        "OneMoreLine\n",
        "AnotherLine\n",
        "LastDefaultLine\n"
    ]
    with open(defaultFile, "w") as f:
        f.writelines(lines)

    chunkSize = len(lines) - 1

    with pytest.raises(ValueError) as err:
        ChunkedFileStore(dbDir=dbDir,
                         dbName="chunked_data",
                         isLineNoKey=isLineNoKey,
                         storeContentHash=storeContentHash,
                         chunkSize=chunkSize,
                         ensureDurability=ensureDurability,
                         chunkStoreConstructor=TextFileStore,
                         defaultFile=defaultFile)
    assert "Default file is larger than chunk size" in str(err)


def test_valid_inputs_fileNameToChunkIndex():
    """
    Checks valid inputs to _fileNameToChunkIndex
    NOTE: chunksPerDir is 10000 with zero pad of 4
    """
    assert ChunkedFileStore._fileNameToChunkIndex("99/0789.log") == 990789
    assert ChunkedFileStore._fileNameToChunkIndex("99/9789.log") == 999789
    assert ChunkedFileStore._fileNameToChunkIndex("9/9789.log") == 99789
    assert ChunkedFileStore._fileNameToChunkIndex("1/0000.log") == 10000
    assert ChunkedFileStore._fileNameToChunkIndex("0/9999.log") == 9999
    assert ChunkedFileStore._fileNameToChunkIndex("0/0001.log") == 1


def test_invalid_inputs_fileNameToChunkIndex():
    """
    Checks invalid inputs to _fileNameToChunkIndex
    """
    with pytest.raises(ValueError) as excinfo:
        ChunkedFileStore._fileNameToChunkIndex("asdf")
    assert("invalid literal for int" in str(excinfo.value))

    with pytest.raises(ValueError) as excinfo:
        ChunkedFileStore._fileNameToChunkIndex("99/asdf")
    assert("invalid literal for int" in str(excinfo.value))


def test_chunkIndexToFileName():
    """
    Checks valid inputs to _chunkIndexToFileName
    NOTE: chunksPerDir is 10000 with zero pad of 4
    """
    assert ChunkedFileStore._chunkIndexToFileName(990789) == "99/0789.log"
    assert ChunkedFileStore._chunkIndexToFileName(999789) == "99/9789.log"
    assert ChunkedFileStore._chunkIndexToFileName(99789) == "9/9789.log"
    assert ChunkedFileStore._chunkIndexToFileName(10000) == "1/0000.log"
    assert ChunkedFileStore._chunkIndexToFileName(9999) == "0/9999.log"
    assert ChunkedFileStore._chunkIndexToFileName(1) == "0/0001.log"


def test_invalid_inputs_chunkIndexToFileName():
    """
    Checks invalid inputs to _chunkIndexToFileName
    """
    with pytest.raises(ValueError) as excinfo:
        ChunkedFileStore._chunkIndexToFileName("asdf")
    assert("invalid literal for int" in str(excinfo.value))

    assert ChunkedFileStore._chunkIndexToFileName("-1") == None


