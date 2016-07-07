import fnmatch
import os

import asyncio
from ledger.stores.text_file_store import TextFileStore

from plenum.common.motor import Motor
from plenum.common.startable import Status


class P2PCommunicator(Motor):

    def __init__(self):
        super().__init__()
        self.commonBaseDir = os.path.expanduser("~/.clicommonstore")
        self._status = Status.stopped

        if not os.path.exists(self.commonBaseDir):
            os.mkdir(self.commonBaseDir)

    def _getDbName(self, frm, to):
        return frm + "_to_" + to + ".queue"

    # TODO: Need to decide a better name of following function
    def sendMsgToOtherClient(self, frm, to, msg):
        tf = TextFileStore(dbDir=self.commonBaseDir, dbName=self._getDbName(frm, to), isLineNoKey=True)
        tf.put(msg)
        print("*** msg " + msg + " sent from " + frm+ " to " + to)

    def _statusChanged(self, old, new):
        # do nothing for now
        pass

    async def prod(self, limit) -> int:
        self._processAllFiles(self._processAndRemoveMsgs)
        await asyncio.sleep(.1)
        return 1

    def _processEachMsg(self, msg):
        print("*** process msg: " + msg)

    def _processAndRemoveMsgs(self, file, *args):
        filePath = self.commonBaseDir + "/" + file
        with open(filePath) as f:
            for line in f:
                self._processEachMsg(line)
        open(filePath, 'w')

    def getAllMatchingFileNames(self):
        filenames = []
        for client in self.clients:
            filenames = filenames + fnmatch.filter(os.listdir(os.path.abspath(self.commonBaseDir)),
                                                   "*_" + client.identifier + ".queue")
        return filenames

    def _processAllFiles(self, func, *args):
        for file in self.getAllMatchingFileNames():
            func(file, *args)

    def _getPendingMsgCount(self, file):
        filePath = self.commonBaseDir + "/" + file
        num_lines = sum(1 for line in open(filePath))
        return num_lines

    def getPendingQueuedMsgs(self):
        pmc = 0
        for file in self.getAllMatchingFileNames():
            pmc = pmc + self._getPendingMsgCount(file)
        return pmc

    def onStopping(self, *args, **kwargs):
        pass
