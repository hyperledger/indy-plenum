import os

import shutil

from plenum.common.log import getlogger

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
        return os.path.join(self.basePath, self.dataDir, self.name)

    def hasFile(self, fileName):
        return os.path.isfile(os.path.join(self.dataLocation, fileName))

    def wipe(self):
        """
        IMPORTANT: calling this method will destroy local data
        :return:
        """
        loc = self.dataLocation
        try:
            shutil.rmtree(loc)
        except Exception as ex:
            logger.debug("Error while removing temporary directory {}".format(
                ex))
