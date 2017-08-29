import itertools
import math
import os
import random
from time import perf_counter

import pytest
from storage.chunked_file_store import ChunkedFileStore
from storage.text_file_store import TextFileStore


def countLines(fname) -> int:
    with open(fname) as f:
        return sum(1 for _ in f)


def getValue(key) -> str:
    return str(key) + " Some data"


chunkSize = 3
dataSize = 101
data = [getValue(i) for i in range(1, dataSize + 1)]


@pytest.fixture(scope="function")
def chunkedTextFileStore(tempdir) -> ChunkedFileStore:
    return ChunkedFileStore(tempdir, "chunked_data", True, True, chunkSize)


@pytest.yield_fixture(scope="function")
def populatedChunkedFileStore(tempdir, chunkedTextFileStore) -> ChunkedFileStore:
    store = chunkedTextFileStore
    store.reset()
    dirPath = os.path.join(tempdir, "chunked_data")
    for d in data:
        store.put(None, d)
    assert len(os.listdir(dirPath)) == math.ceil(dataSize / chunkSize)
    assert all(countLines(dirPath + os.path.sep + f) <= chunkSize
               for f in os.listdir(dirPath))
    yield store
    store.close()


def testWriteToNewFileOnceChunkSizeIsReached(populatedChunkedFileStore):
    pass


def testRandomRetrievalFromChunkedFiles(populatedChunkedFileStore):
    keys = [2 * chunkSize,
            3 * chunkSize + 1,
            3 * chunkSize + chunkSize,
            random.randrange(1, dataSize + 1)]
    for key in keys:
        value = getValue(key)
        assert populatedChunkedFileStore.get(key) == value


def testSizeChunkedFileStore(populatedChunkedFileStore):
    """
    Check performance of `size`
    """
    s = perf_counter()
    c1 = sum(1 for l in populatedChunkedFileStore.iterator())
    e = perf_counter()
    t1 = e - s
    s = perf_counter()
    c2 = populatedChunkedFileStore.size
    e = perf_counter()
    t2 = e - s
    # It should be faster to use ChunkedStore specific implementation
    # of `size`
    assert t1 > t2
    assert c1 == c2
    assert c2 == dataSize


def testIterateOverChunkedFileStore(populatedChunkedFileStore):
    store = populatedChunkedFileStore
    for k, v in store.iterator():
        assert data[int(k) - 1] == v


def test_get_range(populatedChunkedFileStore):
    # Test for range spanning multiple chunks

    # Range begins and ends at chunk boundaries
    num = 0
    for k, v in populatedChunkedFileStore.iterator(
            start=chunkSize + 1, end=2 * chunkSize):
        assert data[int(k) - 1] == v
        num += 1
    assert num == chunkSize

    # Range does not begin or end at chunk boundaries
    num = 0
    for k, v in populatedChunkedFileStore.iterator(
            start=chunkSize + 2, end=2 * chunkSize + 1):
        assert data[int(k) - 1] == v
        num += 1
    assert num == chunkSize

    # Range spans multiple full chunks
    num = 0
    for k, v in populatedChunkedFileStore.iterator(start=chunkSize + 2,
                                                   end=5 * chunkSize + 1):
        assert data[int(k) - 1] == v
        num += 1
    assert num == 4 * chunkSize

    with pytest.raises(AssertionError):
        list(populatedChunkedFileStore.iterator(start=5, end=1))

    for frm, to in [(i, j) for i, j in itertools.permutations(
            range(1, dataSize + 1), 2) if i <= j]:
        for k, v in populatedChunkedFileStore.iterator(
                start=frm, end=to):
            assert data[int(k) - 1] == v
