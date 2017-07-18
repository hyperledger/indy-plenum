import os
import pytest
from ledger.stores.chunked_file_store import ChunkedFileStore
from ledger.stores.text_file_store import TextFileStore


def test_equality_to_text_file_store(tmpdir):
    """
    This test verifies that TextFileStore and ChunkedFileStore behave equally
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

    chunkSize = len(lines)

    chunkedStore = ChunkedFileStore(dbDir=dbDir,
                                    dbName="chunked_data",
                                    isLineNoKey=isLineNoKey,
                                    storeContentHash=storeContentHash,
                                    chunkSize=chunkSize,
                                    ensureDurability=ensureDurability,
                                    chunkStoreConstructor=TextFileStore,
                                    defaultFile=defaultFile)

    textStore = TextFileStore(dbDir=dbDir,
                              dbName="text_data",
                              isLineNoKey=isLineNoKey,
                              storeContentHash=storeContentHash,
                              ensureDurability=ensureDurability,
                              defaultFile=defaultFile)

    for i in range(1, 5 * chunkSize):
        value = str(i)
        chunkedStore.put(value)
        textStore.put(value)
        assert textStore.get(value) == chunkedStore.get(value)

    assert list(chunkedStore.iterator()) == \
           list(textStore.iterator())


