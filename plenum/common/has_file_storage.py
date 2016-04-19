import os


# TODO Maybe the name should be just FileStorage
class HasFileStorage:

    def __init__(self, name, baseDir, dataDir=None):
        self.name = name
        self.basePath = baseDir
        self.dataDir = dataDir if dataDir else ""
        if not os.path.isdir(self.getDataLocation()):
            os.makedirs(self.getDataLocation())

    def getDataLocation(self):
        return os.path.join(self.basePath, self.dataDir, self.name)

    def hasFile(self, fileName):
        return os.path.isfile(os.path.join(self.getDataLocation(), fileName))
