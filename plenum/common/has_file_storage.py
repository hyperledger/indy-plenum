import os

import shutil

from stp_core.common.log import getlogger

logger = getlogger()


class HasFileStorage:

    def __init__(self, dataLocation):
        self._dataLocation = dataLocation
        if not os.path.isdir(dataLocation):
            os.makedirs(dataLocation)

    @property
    def dataLocation(self):
        return self._dataLocation

    def hasFile(self, fileName):
        return os.path.isfile(os.path.join(self._dataLocation, fileName))

    def wipe(self):
        """
        IMPORTANT: calling this method will destroy local data
        :return:
        """
        self.wipeDataLocation(self._dataLocation)

    @staticmethod
    def wipeDataLocation(dataLocation):
        try:
            shutil.rmtree(dataLocation)
        except Exception as ex:
            logger.debug("Error while removing temporary directory {}".format(
                ex))
