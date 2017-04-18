import os

import shutil

from stp_core.common.log import getlogger

logger = getlogger()


class HasFileStorage:

    def __init__(self, name, baseDir, dataDir=None):
        self.name = name
        self.basePath = baseDir
        self.dataDir = dataDir if dataDir else ""
        dataLoc = self.dataLocation
        if not os.path.isdir(dataLoc):
            os.makedirs(dataLoc)

    @property
    def dataLocation(self):
        return self.getDataLocation(self.name, self.basePath, self.dataDir)

    @staticmethod
    def getDataLocation(name, basePath, dataDir=""):
        return os.path.join(basePath, dataDir, name)

    def hasFile(self, fileName):
        return os.path.isfile(os.path.join(self.dataLocation, fileName))

    def wipe(self):
        """
        IMPORTANT: calling this method will destroy local data
        :return:
        """
        self.wipeDataLocation(self.dataLocation)

    @staticmethod
    def wipeDataLocation(dataLocation):
        try:
            shutil.rmtree(dataLocation)
        except Exception as ex:
            logger.debug("Error while removing temporary directory {}".format(
                ex))
