import os


def removeLockFiles(dbPath):
    if os.path.isdir(dbPath):
        lockFilePath = os.path.join(dbPath, 'LOCK')
        if os.path.isfile(lockFilePath):
            os.remove(lockFilePath)
