import time
from binascii import hexlify

from ledger.stores.text_file_store import TextFileStore
from ledger.test.test_file_hash_store import generateHashes


def testMeasureWriteTime(tempdir):
    store = TextFileStore(tempdir, 'benchWithSync', isLineNoKey=True,
                          storeContentHash=False)
    hashes = [hexlify(h).decode() for h in generateHashes(1000)]
    start = time.time()
    for h in hashes:
        store.put(value=h)
    timeTakenWithSync = time.time() - start
    store = TextFileStore(tempdir, 'benchWithoutSync', isLineNoKey=True,
                          storeContentHash=False, ensureDurability=False)
    start = time.time()
    for h in hashes:
        store.put(value=h)
    timeTakenWithoutSync = time.time() - start
    print("Time taken to write {} entries to file with fsync is {} "
          "seconds".format(len(hashes), timeTakenWithSync))
    print("Time taken to write {} entries to file without fsync is {} "
          "seconds".format(len(hashes), timeTakenWithoutSync))
    print("So the difference is {} seconds".
          format(timeTakenWithSync-timeTakenWithoutSync))
    # On most platforms the ratio between write time with fsync and
    # write time without fsync typically must be greater than 100.
    # But on Windows Server 2012 this ratio may be less - down to 30.
    assert timeTakenWithoutSync*10 < timeTakenWithSync, "ratio is {}".\
        format(timeTakenWithSync/timeTakenWithoutSync)
