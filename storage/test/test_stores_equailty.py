import os

from storage.chunked_file_store import ChunkedFileStore
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.text_file_store import TextFileStore


def test_chunked_and_text_store_equality(tmpdir):
    """
    This test verifies that TextFileStore and ChunkedFileStore behave equally
    """
    isLineNoKey = True
    storeContentHash = False
    ensureDurability = True
    dbDir = str(tmpdir)

    chunkSize = 4
    chunkedStore = ChunkedFileStore(dbDir=dbDir,
                                    dbName="chunked_data",
                                    isLineNoKey=isLineNoKey,
                                    storeContentHash=storeContentHash,
                                    chunkSize=chunkSize,
                                    ensureDurability=ensureDurability)

    textStore = TextFileStore(dbDir=dbDir,
                              dbName="text_data",
                              isLineNoKey=isLineNoKey,
                              storeContentHash=storeContentHash,
                              ensureDurability=ensureDurability)

    for i in range(1, 5 * chunkSize):
        value = "Some data {}".format(str(i))
        chunkedStore.put(None, value)
        textStore.put(None, value)
        assert textStore.get(str(i))
        assert textStore.get(str(i)) == chunkedStore.get(str(i))

    assert list(chunkedStore.iterator()) == \
        list(textStore.iterator())


def test_leveldb_and_text_store_equality(tmpdir):
    """
    This test verifies that TextFileStore and LeveldbStore behave equally
    """
    isLineNoKey = True
    storeContentHash = False
    ensureDurability = True
    dbDir = str(tmpdir)

    text_store = TextFileStore(dbDir=dbDir,
                               dbName="text_data",
                               isLineNoKey=isLineNoKey,
                               storeContentHash=storeContentHash,
                               ensureDurability=ensureDurability)

    leveldb_store = KeyValueStorageLeveldb(dbDir, "leveldb_data")

    for i in range(1, 10):
        value = "Some data {}".format(str(i))
        text_store.put(None, value)
        leveldb_store.put(str(i), value)
        assert text_store.get(str(i))
        assert leveldb_store.get(str(i)).decode() == text_store.get(str(i))

    assert list(v.decode() for k, v in leveldb_store.iterator()) == \
        list(v for k, v in text_store.iterator())
